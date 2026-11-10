//! Reading and writing frames on a byte stream.

use super::frame::{decode_header, decode_payload, encode, Frame, FrameError, HEADER_LEN};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The next frame, or `None` when the peer closed the stream between frames.
/// A stream that ends inside a frame is [`FrameError::Truncated`]. After any
/// error the stream is out of step and must be dropped.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_len: usize,
) -> Result<Option<Frame>, FrameError> {
    let mut header = [0u8; HEADER_LEN];
    let mut filled = 0;
    while filled < HEADER_LEN {
        let n = reader.read(&mut header[filled..]).await?;
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
    match reader.read_exact(&mut payload).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err(FrameError::Truncated)
        }
        Err(e) => return Err(e.into()),
    }
    Ok(Some(decode_payload(&payload)?))
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
