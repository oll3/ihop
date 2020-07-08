use async_trait::async_trait;
use bitar::HashSum;
use blake2::{Blake2b, Digest};
use log::*;
use nbd_async::BlockDevice;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use std::{io, io::SeekFrom};
use tokio::{fs::File, io::AsyncReadExt};

use crate::{
    chunk_map::{ChunkMap, ChunkOffsetSize},
    clone::chunk_path_from_hash,
    mount_file,
};

struct IhopBackedDevice {
    root_path: PathBuf,
    block_size: u32,
    block_count: u64,
    chunk_location_map: ChunkMap<PathBuf>,
}

#[async_trait(?Send)]
impl BlockDevice for IhopBackedDevice {
    async fn read(&mut self, mut offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let mut buf_offset = 0;
        let mut locations = self
            .chunk_location_map
            .iter_overlapping(ChunkOffsetSize::new(offset, buf.len()))
            .collect::<Vec<(&ChunkOffsetSize, &PathBuf)>>();
        locations.sort_by(|(loca, _), (locb, _)| loca.offset.partial_cmp(&locb.offset).unwrap());
        for (location, path) in locations {
            let mut chunk_file = File::open(self.root_path.join(path))
                .await
                .expect("open chunk file");

            let offset_in_file = offset - location.offset;
            let read_from_file = std::cmp::min(
                buf.len() - buf_offset,
                location.size - offset_in_file as usize,
            );
            debug!(
                "requested offset: {} (size {}), chunk start: {} (size: {}), seek to {}",
                offset,
                buf.len() - buf_offset,
                location.offset,
                location.size,
                offset_in_file,
            );
            chunk_file
                .seek(SeekFrom::Start(offset_in_file))
                .await
                .expect("seek in chunk file");
            chunk_file
                .read_exact(&mut buf[buf_offset..buf_offset + read_from_file])
                .await
                .expect("read chunk from file");
            buf_offset += read_from_file;
            offset += read_from_file as u64;
        }
        Ok(())
    }
    async fn write(&mut self, _offset: u64, _buf: &[u8]) -> io::Result<()> {
        unimplemented!()
    }
}

fn make_device(
    root_path: &Path,
    dictionary: &crate::storedict::StoreDictionary,
    block_size: u32,
) -> IhopBackedDevice {
    let mut offset: u64 = 0;
    let mut chunk_location_map: ChunkMap<PathBuf> = ChunkMap::new();
    for index in &dictionary.source_order {
        let cd = &dictionary.chunk_descriptors[*index as usize];
        let hash = HashSum::from_vec(cd.checksum.clone());
        let chunk_path = chunk_path_from_hash(&hash);
        chunk_location_map.insert(
            ChunkOffsetSize::new(offset, cd.source_size as usize),
            chunk_path,
        );
        offset += cd.source_size as u64;
    }

    let block_count = dictionary.source_total_size / block_size as u64;
    info!(
        "load device of {} chunks, total {} bytes ({} blocks), source checksum: {}",
        dictionary.source_order.len(),
        dictionary.source_total_size,
        block_count,
        HashSum::from_slice(&dictionary.source_checksum[..]),
    );

    IhopBackedDevice {
        root_path: root_path.to_path_buf(),
        block_size,
        block_count,
        chunk_location_map,
    }
}

async fn mount_ihop(mut backend_file: File, root_path: &Path, nbd_dev: &Path, block_size: u32) {
    let mut dict_size_buf = vec![0; std::mem::size_of::<u64>()];
    backend_file
        .read_exact(&mut dict_size_buf)
        .await
        .expect("read dictionary size");
    let dict_size = u64::from_le_bytes((&dict_size_buf[..]).try_into().unwrap());
    let mut dict_buf = vec![0; dict_size as usize];
    backend_file
        .read_exact(&mut dict_buf)
        .await
        .expect("read dictionary");
    {
        let mut expected_checksum = vec![0; 64];
        backend_file
            .read_exact(&mut expected_checksum)
            .await
            .expect("read checksum");

        let mut hasher = Blake2b::new();
        hasher.update(&crate::STORE_MAGIC[..]);
        hasher.update(&dict_size_buf[..]);
        hasher.update(&dict_buf[..]);
        let checksum = hasher.finalize().to_vec();

        if checksum != expected_checksum {
            panic!(
                "header checksum mismatch (expected {:?}, was {:?})",
                expected_checksum, checksum
            );
        }
    }

    let dictionary: crate::storedict::StoreDictionary =
        prost::Message::decode(&dict_buf[..]).expect("decode dictionary");

    let device = make_device(root_path, &dictionary, block_size);
    nbd_async::serve_local_nbd(nbd_dev, device.block_size, device.block_count, device)
        .await
        .expect("mount");
}

pub async fn mount(backend: &Path, nbd_dev: &Path, block_size: u32) {
    let mut backend_file = File::open(backend).await.expect("open");
    let mut magic = vec![0; 6];
    backend_file.read_exact(&mut magic).await.expect("read");
    if &magic[..] == crate::STORE_MAGIC {
        info!("mount ihop {} on {}", backend.display(), nbd_dev.display());
        let root_path = backend.parent().expect("store root");
        mount_ihop(backend_file, root_path, nbd_dev, block_size).await;
    } else {
        info!(
            "mount regular file {} on {} with block size {}",
            backend.display(),
            nbd_dev.display(),
            block_size
        );
        mount_file::mount(backend_file, nbd_dev, block_size).await;
    }
}
