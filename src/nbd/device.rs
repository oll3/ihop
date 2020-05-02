use async_trait::async_trait;
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_core::stream::Stream;
use futures_util::stream::StreamExt;
use log::*;
use std::io::Error;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::thread::JoinHandle;
use tokio::fs::OpenOptions;
use tokio::{io::AsyncRead, io::AsyncWriteExt, net::UnixStream};

use crate::nbd;

const MAX_BATCH_REQUESTS: usize = 4;

#[async_trait]
pub trait BlockDevice {
    async fn read(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), Error>;
    fn block_size(&self) -> u32;
    fn block_count(&self) -> u64;
}

struct RequestStream {
    sock: Option<UnixStream>,
    do_it_thread: Option<JoinHandle<Result<(), Error>>>,
    read_buf: Vec<u8>,
    requests: Vec<nbd::Request>,
    file: tokio::fs::File,
}

pub async fn new_device<P: AsRef<Path>, B>(path: P, mut block_device: B) -> Result<(), Error>
where
    B: Unpin + BlockDevice,
{
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.as_ref())
        .await?;

    let (sock, kernel_sock) = UnixStream::pair()?;

    nbd::set_block_size(&file, block_device.block_size())?;
    nbd::set_size_blocks(&file, block_device.block_count())?;
    nbd::set_timeout(&file, 10)?;
    nbd::clear_sock(&file)?;

    let inner_file = file.try_clone().await?;
    let do_it_thread = Some(std::thread::spawn(move || -> Result<(), Error> {
        nbd::set_sock(&inner_file, kernel_sock.as_raw_fd())?;
        let _ = nbd::set_flags(&inner_file, 0);

        // The do_it ioctl will block until device is disconnected, hence
        // the separate thread.
        nbd::do_it(&inner_file)?;

        let _ = nbd::clear_sock(&inner_file);
        let _ = nbd::clear_queue(&inner_file);
        debug!("closed receive thread");
        Ok(())
    }));

    let mut stream = RequestStream {
        sock: Some(sock),
        do_it_thread,
        read_buf: vec![0; nbd::SIZE_OF_REQUEST * MAX_BATCH_REQUESTS],
        requests: Vec::new(),
        file,
    };

    let mut reply_buf = vec![];
    while let Some(num_requests) = stream.next().await {
        if let Err(err) = num_requests {
            return Err(err);
        }
        let sock = match stream.sock {
            Some(ref mut sock) => sock,
            None => break,
        };
        for request in &stream.requests {
            debug!("received request {:?}", request);
            let mut reply = nbd::Reply::from_request(&request);
            match request.command {
                nbd::Command::Read => {
                    let start_offs = reply_buf.len();
                    reply_buf.resize(start_offs + nbd::SIZE_OF_REPLY + request.len, 0);
                    if let Err(err) = block_device
                        .read(
                            request.from,
                            &mut reply_buf[start_offs + nbd::SIZE_OF_REPLY..],
                        )
                        .await
                    {
                        reply.error = err.raw_os_error().unwrap_or(nix::errno::Errno::EIO as i32);
                    }
                    reply.write_to_slice(&mut reply_buf[start_offs..])?;
                }
                nbd::Command::Flush => {
                    reply.append_to_vec(&mut reply_buf)?;
                }
                nbd::Command::Write => unimplemented!(),
                nbd::Command::Disc => unimplemented!(),
                nbd::Command::Trim => unimplemented!(),
                nbd::Command::WriteZeroes => unimplemented!(),
            }
        }
        sock.write_all(&reply_buf).await?;
        reply_buf.clear();
    }
    Ok(())
}

impl Drop for RequestStream {
    fn drop(&mut self) {
        let _ = nbd::disconnect(&self.file);
        self.sock = None;
        if let Some(do_it_thread) = self.do_it_thread.take() {
            if let Err(err) = do_it_thread.join() {
                error!("thread ended with error: {:?}", err);
            }
        }
    }
}

impl RequestStream {
    fn read_next(&mut self, cx: &mut Context) -> Poll<Option<Result<usize, Error>>> {
        let sock = match self.sock {
            Some(ref mut sock) => sock,
            None => return Poll::Ready(None),
        };
        let read_buf = &mut self.read_buf;
        let rc = Pin::new(sock).poll_read(cx, read_buf);
        let n = match rc {
            Poll::Ready(Ok(0)) => return Poll::Ready(None),
            Poll::Ready(Ok(n)) => n,
            Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
            Poll::Pending => {
                return Poll::Pending;
            }
        };
        self.requests.clear();
        for offs in (0..n).step_by(nbd::SIZE_OF_REQUEST) {
            let request =
                nbd::Request::try_from_bytes(&self.read_buf[offs..offs + nbd::SIZE_OF_REQUEST]);
            self.requests.push(match request {
                Ok(req) => req,
                Err(err) => return Poll::Ready(Some(Err(err))),
            });
        }
        Poll::Ready(Some(Ok(self.requests.len())))
    }
}

impl Stream for RequestStream {
    type Item = Result<usize, Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.read_next(cx)
    }
}
