use async_trait::async_trait;
use bitar::{ChunkIndex, ChunkSizeAndOffset, CloneOutput, HashSum};
use blake2::{Blake2b, Digest};
use log::*;
use prost::Message;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::{create_dir_all, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::Url;

use crate::storedict;
use crate::STORE_MAGIC;

#[derive(Debug, Clone)]
pub enum InputArchive {
    Local(std::path::PathBuf),
    Remote {
        url: Url,
        retries: u32,
        retry_delay: Duration,
        receive_timeout: Option<Duration>,
    },
}

impl InputArchive {
    pub fn source(&self) -> String {
        match self {
            Self::Local(p) => format!("{}", p.display()),
            Self::Remote { url, .. } => url.to_string(),
        }
    }
}

pub fn build_store_header(dictionary: &storedict::StoreDictionary) -> Vec<u8> {
    let mut header: Vec<u8> = vec![];
    let mut hasher = Blake2b::new();
    let mut dictionary_buf: Vec<u8> = Vec::new();

    dictionary
        .encode(&mut dictionary_buf)
        .expect("encode dictionary");

    // File magic indicating bita archive version 1
    header.extend(STORE_MAGIC);
    header.extend(&(dictionary_buf.len() as u64).to_le_bytes());
    header.extend(dictionary_buf);

    // Create and store hash of full header
    hasher.input(&header);
    header.extend(&hasher.result());
    header
}

pub fn chunk_path_from_hash(hash: &HashSum) -> PathBuf {
    let subdir_bytes = 2;
    let mut subdir_name = String::with_capacity(subdir_bytes * 2);
    hash.slice()[..subdir_bytes]
        .iter()
        .for_each(|b| subdir_name.push_str(&format!("{:02x}", b)));
    Path::new("chunks")
        .join(subdir_name)
        .join(&format!("{}", hash))
        .with_extension("chunk")
}

#[derive(Clone, Debug)]
struct ChunkStore {
    root_path: PathBuf,
}
impl ChunkStore {
    fn new(root_path: &Path) -> Self {
        Self {
            root_path: root_path.to_path_buf(),
        }
    }
    async fn filter_present_chunks(
        &self,
        verify: bool,
        chunks: &ChunkIndex,
    ) -> Result<ChunkIndex, bitar::Error> {
        let mut new_index: HashMap<HashSum, ChunkSizeAndOffset> = HashMap::new();
        for (hash, v) in chunks.iter() {
            // Test if chunk file exists
            let chunk_path = self.root_path.join(chunk_path_from_hash(hash));
            match OpenOptions::new().read(true).open(chunk_path).await {
                Ok(mut chunk_file) => {
                    if verify {
                        let mut chunk_buf = Vec::with_capacity(v.size);
                        if match chunk_file.read_to_end(&mut chunk_buf).await {
                            Ok(_) => HashSum::b2_digest(&chunk_buf, hash.len()) != *hash,
                            Err(_err) => false,
                        } {
                            // Chunk present but seems corrupt
                            warn!("Chunk {} corrupt, will be re-fetched", hash);
                            new_index.insert(hash.clone(), v.clone());
                        }
                    }
                }
                Err(_err) => {
                    // Chunk is not present
                    new_index.insert(hash.clone(), v.clone());
                }
            }
        }
        Ok(new_index.into())
    }

    fn chunker_config_to_params(
        conf: &bitar::ChunkerConfig,
        chunk_hash_length: u32,
    ) -> storedict::ChunkerParameters {
        match conf {
            bitar::ChunkerConfig::BuzHash(hash_config) => storedict::ChunkerParameters {
                chunk_hash_length,
                chunk_filter_bits: hash_config.filter_bits.bits(),
                chunking_algorithm: storedict::chunker_parameters::ChunkingAlgorithm::Buzhash
                    as i32,
                min_chunk_size: hash_config.min_chunk_size as u32,
                max_chunk_size: hash_config.max_chunk_size as u32,
                rolling_hash_window_size: hash_config.window_size as u32,
            },
            bitar::ChunkerConfig::RollSum(hash_config) => storedict::ChunkerParameters {
                chunk_hash_length,
                chunk_filter_bits: hash_config.filter_bits.bits(),
                chunking_algorithm: storedict::chunker_parameters::ChunkingAlgorithm::Rollsum
                    as i32,
                min_chunk_size: hash_config.min_chunk_size as u32,
                max_chunk_size: hash_config.max_chunk_size as u32,
                rolling_hash_window_size: hash_config.window_size as u32,
            },
            bitar::ChunkerConfig::FixedSize(fixed_size) => storedict::ChunkerParameters {
                chunk_hash_length,
                chunk_filter_bits: 0,
                chunking_algorithm: storedict::chunker_parameters::ChunkingAlgorithm::FixedSize
                    as i32,
                min_chunk_size: 0,
                max_chunk_size: *fixed_size as u32,
                rolling_hash_window_size: 0,
            },
        }
    }

    fn dictionary(&self, archive: &bitar::Archive) -> storedict::StoreDictionary {
        storedict::StoreDictionary {
            application_version: crate::PKG_VERSION.to_string(),
            chunker_params: Some(Self::chunker_config_to_params(
                archive.chunker_config(),
                archive.chunk_hash_length() as u32,
            )),
            source_checksum: archive.source_checksum().to_vec(),
            source_total_size: archive.total_source_size(),
            source_order: archive.rebuild_order().to_vec(),
            chunk_descriptors: archive
                .chunk_descriptors()
                .iter()
                .map(|desc| storedict::ChunkDescriptor {
                    checksum: desc.checksum.to_vec(),
                    source_size: desc.source_size,
                })
                .collect(),
        }
    }
}

#[async_trait]
impl CloneOutput for ChunkStore {
    async fn write_chunk(
        &mut self,
        hash: &HashSum,
        _offsets: &[u64],
        buf: &[u8],
    ) -> Result<(), bitar::Error> {
        let chunk_path = self.root_path.join(chunk_path_from_hash(hash));
        create_dir_all(chunk_path.parent().expect("chunk subdir")).await?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&chunk_path)
            .await?;
        debug!("write chunk {} to {}", hash, chunk_path.display());
        file.write_all(&buf[..]).await.expect("write chunk file");
        Ok(())
    }
}

async fn clone_with_reader<R>(
    store_root: &Path,
    mut reader: R,
    mut output_dict: File,
    verify_present: bool,
) where
    R: bitar::Reader,
{
    let archive = bitar::Archive::try_init(&mut reader)
        .await
        .expect("init archive");
    let chunks_to_get = archive.source_index().clone();

    let mut store = ChunkStore::new(store_root);
    let clone_opts = bitar::CloneOptions::default();

    // Don't fetch chunks already in store
    let mut chunks_left = store
        .filter_present_chunks(verify_present, &chunks_to_get)
        .await
        .expect("filter chunks");

    info!(
        "{} chunks present in store, {} chunks to fetch",
        chunks_to_get.len() - chunks_left.len(),
        chunks_left.len()
    );

    // Fetch the rest of the chunks from archive
    bitar::clone_from_archive(
        &clone_opts,
        &mut reader,
        &archive,
        &mut chunks_left,
        &mut store,
    )
    .await
    .expect("clone from archive");

    // Write the store dictionary file
    let header_buf = build_store_header(&store.dictionary(&archive));
    output_dict
        .write_all(&header_buf[..])
        .await
        .expect("write output file");
}

pub async fn clone(
    input: InputArchive,
    output: &Path,
    store_root: &Path,
    force_create: bool,
    verify_present: bool,
) {
    let input_source = input.source();

    let output_dict = tokio::fs::OpenOptions::new()
        .write(true)
        .create(force_create)
        .create_new(!force_create)
        .open(&output)
        .await
        .expect("open output file");

    //let mut reader = input.new_reader().await;
    info!(
        "cloning archive {} to {} (chunks at {}/chunks)",
        input_source,
        output.display(),
        store_root.display()
    );
    match input {
        InputArchive::Local(path) => {
            clone_with_reader(
                store_root,
                File::open(path)
                    .await
                    .expect("failed to open local archive"),
                output_dict,
                verify_present,
            )
            .await
        }
        InputArchive::Remote {
            url,
            retries,
            retry_delay,
            receive_timeout,
        } => {
            let mut request = reqwest::Client::new().get(url);
            if let Some(timeout) = receive_timeout {
                request = request.timeout(timeout);
            }
            clone_with_reader(
                store_root,
                bitar::ReaderRemote::from_request(request)
                    .retries(retries)
                    .retry_delay(retry_delay),
                output_dict,
                verify_present,
            )
            .await
        }
    }
    info!(
        "Successfully cloned {} to {}",
        input_source,
        output.display()
    );
}
