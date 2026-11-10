//! Reading and writing frames on a byte stream.

use super::frame::{decode_header, decode_payload, encode, Frame, FrameError, HEADER_LEN};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// How long a frame may stall once it has started. A peer that sends part of a
/// frame and then goes quiet (a hung ssh, a stopped process) would otherwise
/// hold the reader, and with it the whole connection, forever. Waiting between
/// frames has no limit here: an idle connection is the heartbeat's business.
pub const FRAME_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The next frame, or `None` when the peer closed the stream between frames.
/// A stream that ends inside a frame is [`FrameError::Truncated`]. After any
/// error the stream is out of step and must be dropped.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> Result<Option<Frame>, FrameError> {
    Ok(read_frame_sized(reader, max_len, FRAME_IDLE_TIMEOUT)
        .await?
        .map(|(frame, _)| frame))
}

/// Like [`read_frame`], with the payload length (what the frame cost on the
/// wire, for budgeting) and the stall limit `idle`: once the first byte of a
/// frame has arrived, no single read may wait longer than `idle`; a stalled
/// frame is an `io::ErrorKind::TimedOut` error.
pub async fn read_frame_sized<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_len: usize,
    idle: Duration,
) -> Result<Option<(Frame, usize)>, FrameError> {
    let mut header = [0u8; HEADER_LEN];
    let mut filled = 0;
    while filled < HEADER_LEN {
        let n = if filled == 0 {
            reader.read(&mut header).await?
        } else {
            read_within(reader, &mut header[filled..], idle).await?
        };
        if n == 0 {
            return if filled == 0 {
                Ok(None)
            } else {
                Err(FrameError::Truncated)
            };
        }
        filled += n;
    }
    let len = decode_header(header, max_len)?;
    let mut payload = vec![0u8; len];
    let mut got = 0;
    while got < len {
        let n = read_within(reader, &mut payload[got..], idle).await?;
        if n == 0 {
            return Err(FrameError::Truncated);
        }
        got += n;
    }
    Ok(Some((decode_payload(&payload)?, len)))
}

async fn read_within<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    idle: Duration,
) -> Result<usize, FrameError> {
    match tokio::time::timeout(idle, reader.read(buf)).await {
        Ok(r) => Ok(r?),
        Err(_) => Err(FrameError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "the peer stopped in the middle of a frame",
        ))),
    }
}

/// Write one frame and flush it.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &Frame,
    max_len: usize,
) -> Result<(), FrameError> {
    let bytes = encode(frame, max_len)?;
    writer.write_all(&bytes).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::MAX_FRAME_LEN;
    use tokio::io::duplex;

    #[tokio::test]
    async fn frames_round_trip_and_eof_between_frames_is_clean() {
        let (mut a, mut b) = duplex(1024);
        write_frame(&mut a, &Frame::ping(1), MAX_FRAME_LEN)
            .await
            .unwrap();
        write_frame(&mut a, &Frame::pong(2), MAX_FRAME_LEN)
            .await
            .unwrap();
        drop(a);
        assert_eq!(
            read_frame(&mut b, MAX_FRAME_LEN).await.unwrap(),
            Some(Frame::ping(1))
        );
        assert_eq!(
            read_frame(&mut b, MAX_FRAME_LEN).await.unwrap(),
            Some(Frame::pong(2))
        );
        assert_eq!(read_frame(&mut b, MAX_FRAME_LEN).await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_frame_that_stalls_midway_times_out_but_an_idle_stream_does_not() {
        let bytes = encode(&Frame::ping(1), MAX_FRAME_LEN).unwrap();
        let idle = Duration::from_millis(50);
        // Half a header, then silence; and a body cut short, then silence.
        for cut in [2, bytes.len() - 1] {
            let (mut a, mut b) = duplex(1024);
            a.write_all(&bytes[..cut]).await.unwrap();
            let err = read_frame_sized(&mut b, MAX_FRAME_LEN, idle)
                .await
                .unwrap_err();
            match err {
                FrameError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
                other => panic!("{other:?}"),
            }
            drop(a);
        }
        // Between frames the reader waits as long as the peer keeps the stream
        // open, and then reads the frame together with its payload length.
        let (mut a, mut b) = duplex(1024);
        let reader =
            tokio::spawn(async move { read_frame_sized(&mut b, MAX_FRAME_LEN, idle).await });
        tokio::time::sleep(idle * 4).await;
        a.write_all(&bytes).await.unwrap();
        let (frame, len) = reader.await.unwrap().unwrap().unwrap();
        assert_eq!(frame, Frame::ping(1));
        assert_eq!(len, bytes.len() - HEADER_LEN);
    }

    #[tokio::test]
    async fn eof_inside_a_header_or_a_body_is_truncation() {
        let bytes = encode(&Frame::ping(1), MAX_FRAME_LEN).unwrap();
        for cut in [2, bytes.len() - 1] {
            let mut r: &[u8] = &bytes[..cut];
            assert!(matches!(
                read_frame(&mut r, MAX_FRAME_LEN).await,
                Err(FrameError::Truncated)
            ));
        }
    }

    #[tokio::test]
    async fn oversize_garbage_and_malformed_frames_are_errors() {
        let mut r: &[u8] = b"Welcome to the machine\n";
        assert!(matches!(
            read_frame(&mut r, MAX_FRAME_LEN).await,
            Err(FrameError::TooLarge { .. })
        ));
        let mut bytes = 5u32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"hello");
        let mut r: &[u8] = &bytes;
        assert!(matches!(
            read_frame(&mut r, MAX_FRAME_LEN).await,
            Err(FrameError::Malformed(_))
        ));
        let mut r: &[u8] = &[0, 0, 0, 0];
        assert!(matches!(
            read_frame(&mut r, MAX_FRAME_LEN).await,
            Err(FrameError::Empty)
        ));
        // The limit applies to the reader's own configured maximum.
        let big = encode(&Frame::ping(1), MAX_FRAME_LEN).unwrap();
        let mut r: &[u8] = &big;
        assert!(matches!(
            read_frame(&mut r, 3).await,
            Err(FrameError::TooLarge { .. })
        ));
        let mut sink = Vec::new();
        assert!(matches!(
            write_frame(&mut sink, &Frame::ping(1), 3).await,
            Err(FrameError::TooLarge { .. })
        ));
        assert!(sink.is_empty());
    }
}
