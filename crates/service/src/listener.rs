use axum::serve::Listener;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_util::sync::CancellationToken;

/// Axum owns the connection tasks. Revoking their I/O also interrupts a slow
/// receiver that prevents Hyper from polling a response body during forced drain.
pub(crate) struct ManagedListener {
    pub listener: TcpListener,
    pub force: CancellationToken,
    pub failed: CancellationToken,
}
impl Listener for ManagedListener {
    type Io = ManagedIo;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (ManagedIo, SocketAddr) {
        loop {
            match self.listener.accept().await {
                Ok((stream, address)) => {
                    return (
                        ManagedIo {
                            stream,
                            cancelled: Box::pin(self.force.clone().cancelled_owned()),
                            stopped: false,
                        },
                        address,
                    );
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                    ) => {}
                Err(_) => {
                    self.failed.cancel();
                    return std::future::pending().await;
                }
            }
        }
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

pub(crate) struct ManagedIo {
    stream: TcpStream,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    stopped: bool,
}
impl ManagedIo {
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        self.stopped = self.stopped || self.cancelled.as_mut().poll(cx).is_ready();
        if self.stopped {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "service stopped",
            ))
        } else {
            Ok(())
        }
    }
}
impl AsyncRead for ManagedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ManagedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
