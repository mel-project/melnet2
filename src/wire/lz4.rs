use std::{io::Write, task::Poll};

use lz4_flex::frame::FrameEncoder;
use pin_project::pin_project;
use reusable_box_future::ReusableBoxFuture;
use smol::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

/// An async streaming LZ4 compressor. Implements the [AsyncWrite] interface.
#[pin_project]
pub struct AsyncLz4Encoder<W: AsyncWrite> {
    inner: Option<W>,
    compressor: Option<FrameEncoder<Vec<u8>>>,

    running_write: ReusableBoxFuture<(std::io::Result<()>, W, FrameEncoder<Vec<u8>>)>,
}

impl<W: AsyncWrite + Send + 'static + Unpin> AsyncLz4Encoder<W> {
    /// Creates a new AsyncLz4Encoder.
    pub fn new(inner: W) -> Self {
        Self {
            inner: None,
            compressor: None,
            running_write: ReusableBoxFuture::new({
                async move { (Ok(()), inner, FrameEncoder::new(Vec::new())) }
            }),
        }
    }
}

impl<W: AsyncWrite + Send + 'static + Unpin> AsyncWrite for AsyncLz4Encoder<W> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.project();

        // poll the in-progress future. if it's pending, we pending too.
        let (r, w, f) = smol::ready!(this.running_write.poll(cx));
        *this.inner = Some(w);
        *this.compressor = Some(f);
        if let Err(err) = r {
            return Poll::Ready(Err(err));
        }

        // if we got here, in_progress is already done. we queue up another one
        let n = this.compressor.as_mut().unwrap().write(buf).unwrap();
        let new_fut = {
            let mut compressor = this.compressor.take().unwrap();
            let mut inner = this.inner.take().unwrap();
            async move {
                (
                    lz4_write(&mut inner, &mut compressor).await,
                    inner,
                    compressor,
                )
            }
        };
        this.running_write.set(new_fut);
        Poll::Ready(Ok(n))
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        // TODO not implemented
        Poll::Ready(Ok(()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        // we just poll the in-progress future.
        let this = self.project();
        let (r, w, f) = smol::ready!(this.running_write.poll(cx));
        *this.inner = Some(w);
        *this.compressor = Some(f);
        Poll::Ready(r)
    }
}

async fn lz4_write(
    write_to: &mut (impl AsyncWrite + Unpin),
    compressor: &mut FrameEncoder<Vec<u8>>,
) -> std::io::Result<()> {
    compressor.flush()?;
    let vec = compressor.get_mut();
    write_to.write_all(vec).await?;
    write_to.flush().await?;
    vec.clear();
    Ok(())
}

// /// An async streaming LZ4 decompressor. Implements the [AsyncRead] interface.
// #[pin_project]
// pub struct AsyncLz4Decoder<R: AsyncRead> {
//     #[pin]
//     inner: R,

//     decompressor: FrameDecoder<Cursor<Vec<u8>>>,
// }

// impl<R: AsyncRead> AsyncRead for AsyncLz4Decoder<R> {
//     fn poll_read(
//         self: std::pin::Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//         buf: &mut [u8],
//     ) -> Poll<std::io::Result<usize>> {
//         let this = self.project();
//         let decomp_posn = this.decompressor.get_ref().position();
//         if let Ok(n) = this.decompressor.read(buf) {
//             return Poll::Ready(Ok(n));
//         } else {
//             // reset the state
//             this.decompressor.get_mut().set_position(decomp_posn)
//         }
//         // try to fill the decompressor
//         let mut temp_buf = [0u8; 16384];
//         let n = smol::ready!(this.inner.poll_read(cx, &mut temp_buf));
//         let n = match n {
//             Err(err) => return Poll::Ready(Err(err)),
//             Ok(n) => n,
//         };
//         this.decompressor.get_mut().write(&buf[..n]).unwrap();
//     }
// }
