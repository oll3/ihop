mod clone;
mod mount;
mod mount_file;
mod nbd;
mod size_str;

use clap::{App, Arg, SubCommand};
use std::path::Path;
use std::time::Duration;

pub const PKG_NAME: &str = env!("CARGO_PKG_NAME");
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const STORE_MAGIC: &[u8; 6] = b"IHOP1\0";

pub mod storedict {
    include!(concat!(env!("OUT_DIR"), "/store_dictionary.rs"));
}

fn parse_input_config(matches: &clap::ArgMatches<'_>) -> clone::InputArchive {
    let input = matches.value_of("INPUT").unwrap().to_string();
    match input.parse::<url::Url>() {
        Ok(url) => {
            // Use as URL
            clone::InputArchive::Remote {
                url,
                retries: matches
                    .value_of("http-retry-count")
                    .unwrap_or("0")
                    .parse()
                    .expect("failed to parse http-retry-count"),
                retry_delay: Duration::from_secs(
                    matches
                        .value_of("http-retry-delay")
                        .map(|v| v.parse().expect("failed to parse http-retry-delay"))
                        .unwrap_or(0),
                ),
                receive_timeout: matches
                    .value_of("http-timeout")
                    .map(|v| Duration::from_secs(v.parse().expect("failed to parse http-timeout"))),
            }
        }
        Err(_) => {
            // Use as path
            clone::InputArchive::Local(input.into())
        }
    }
}

fn parse_size(size_str: &str) -> usize {
    let size_val: String = size_str.chars().filter(|a| a.is_numeric()).collect();
    let size_val: usize = size_val.parse().expect("parse");
    let size_unit: String = size_str.chars().filter(|a| !a.is_numeric()).collect();
    if size_unit.is_empty() {
        return size_val;
    }
    match size_unit.as_str() {
        "GiB" => 1024 * 1024 * 1024 * size_val,
        "MiB" => 1024 * 1024 * size_val,
        "KiB" => 1024 * size_val,
        "B" => size_val,
        _ => panic!("Invalid size unit"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new(PKG_NAME)
        .version(PKG_VERSION)
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .global(true)
                .help("Set log level verbosity"),
        )
        .subcommand(
            SubCommand::with_name("mount")
                .about("Mount a chunk store or file as a nbd device.\nProcess needs to run as long as the mount is active.")
                .arg(
                    Arg::with_name("BACKEND")
                        .value_name("BACKEND")
                        .help("Device backend can either be store or a single file.")
                        .required(true),
                )
                .arg(
                    Arg::with_name("NBD")
                        .value_name("NBD")
                        .help("NBD device path (eg /dev/nbd0).")
                        .required(true),
                )
                .arg(
                    Arg::with_name("block-size")
                        .long("block-size")
                        .value_name("SIZE")
                        .help("Set the chunk data compression level (0-9) [default: 6]"),
                ),
        )
        .subcommand(
            SubCommand::with_name("clone")
                .about("Clone a bita archive to a store.")
                .arg(
                    Arg::with_name("INPUT")
                        .value_name("INPUT")
                        .help("Input file (can be a local archive or a URL)")
                        .required(true),
                )
                .arg(
                    Arg::with_name("OUTPUT")
                        .value_name("OUTPUT")
                        .help("Where to store chunks and dictionary")
                        .required(true),
                )
                .arg(
                    Arg::with_name("force-create")
                        .short("f")
                        .long("force-create")
                        .help("Overwrite dictionary file if it exist"),
                )
                .arg(
                    Arg::with_name("naive")
                        .long("naive")
                        .help("Do not verify the checksum of chunks already present"),
                ),
        )
        .get_matches();

    // Init logger
    pretty_env_logger::formatted_timed_builder()
        .filter(
            None,
            match matches.occurrences_of("verbose") {
                0 => log::LevelFilter::Info,
                1 => log::LevelFilter::Debug,
                _ => log::LevelFilter::Trace,
            },
        )
        .init();

    // Handle mount subcommand
    if let Some(matches) = matches.subcommand_matches("mount") {
        let backend = Path::new(matches.value_of("BACKEND").unwrap());
        let nbd_dev = Path::new(matches.value_of("NBD").unwrap());
        let block_size = parse_size(matches.value_of("avg-chunk-size").unwrap_or("512B")) as u32;
        mount::mount(backend, nbd_dev, block_size).await
    }
    // Handle clone subcommand
    if let Some(matches) = matches.subcommand_matches("clone") {
        let output = Path::new(matches.value_of("OUTPUT").unwrap());
        let store_root = output.parent().unwrap_or_else(|| Path::new("./"));
        let input_archive = parse_input_config(&matches);
        clone::clone(
            input_archive,
            output,
            store_root,
            matches.is_present("force-create"),
            !matches.is_present("naive"),
        )
        .await
    }
    Ok(())
}
