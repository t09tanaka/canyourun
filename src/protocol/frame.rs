//! The frame: a 4 byte big-endian length followed by that many bytes of JSON.
//!
//! The JSON is an envelope with a stream id, a stream kind and a body. This
//! module is pure: it turns frames into bytes and back and enforces the size
//! limit. Reading from and writing to a transport is [`super::io`].

use crate::wire::{Message, Operation};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Largest JSON body one frame may carry (2 MiB). A chunk of
/// [`super::chunk::MAX_CHUNK_BYTES`] bytes grows by a third in base64 and still
/// fits with room for the envelope.
pub const MAX_FRAME_LEN: usize = 2_097_152;

/// Bytes of the length prefix.
pub const HEADER_LEN: usize = 4;

/// Which queue a frame travels in. Control frames (requests, heartbeats, small
/// responses, errors) never wait behind the other two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Control,
    Log,
    Blob,
}

/// What a frame carries. `Ping` and `Pong` are connection heartbeats; the wire
/// DTOs have no heartbeat of their own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Body {
    Message(Message),
    Ping,
    Pong,
}

/// One frame: the stream it belongs to (a request and its reply share it), the
/// queue it travels in, and the body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    pub stream: u32,
    pub kind: StreamKind,
    pub body: Body,
}

impl Frame {
    /// A message frame; the kind follows from the message.
    pub fn message(stream: u32, message: Message) -> Frame {
        Frame {
            stream,
            kind: kind_of(&message),
            body: Body::Message(message),
        }
    }

    pub fn ping(stream: u32) -> Frame {
        Frame {
            stream,
            kind: StreamKind::Control,
            body: Body::Ping,
        }
    }

    pub fn pong(stream: u32) -> Frame {
        Frame {
            stream,
            kind: StreamKind::Control,
            body: Body::Pong,
        }
    }
}

/// The queue an operation's frames use, in both directions. The match is
/// exhaustive on purpose: a new operation must be classified here.
pub fn kind_of_operation(op: Operation) -> StreamKind {
    match op {
        Operation::ReadLog => StreamKind::Log,
        Operation::BlobPut | Operation::BlobGet | Operation::SecretBundlePut => StreamKind::Blob,
        Operation::Hello
        | Operation::Inspect
        | Operation::Forecast
        | Operation::SubmitAttempt
        | Operation::GetAttempt
        | Operation::ListAttempts
        | Operation::RenewLease
        | Operation::CancelAttempt
        | Operation::RetireAttempt
        | Operation::WatchEvents
        | Operation::GetResult
        | Operation::AckCollected
        | Operation::BlobVerify
        | Operation::EnvReport
        | Operation::EnvSync => StreamKind::Control,
    }
}

/// The queue a message uses. Responses count too: the big frames are the
/// replies to `read_log` and `blob_get`.
pub fn kind_of(message: &Message) -> StreamKind {
    match message {
        Message::Request(r) => kind_of_operation(r.operation()),
        Message::Response { response, .. } => kind_of_operation(response.operation()),
        Message::Error(_) => StreamKind::Control,
    }
}

#[derive(Debug, Error)]
pub enum FrameError {
    #[error("frame of {len} bytes exceeds the {max} byte limit")]
    TooLarge { len: usize, max: usize },
    #[error("empty frame")]
    Empty,
    #[error("connection ended inside a frame")]
    Truncated,
    #[error("malformed frame: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("transport error: {0}")]
    Io(#[from] std::io::Error),
}

fn check_len(len: usize, max: usize) -> Result<usize, FrameError> {
    if len == 0 {
        Err(FrameError::Empty)
    } else if len > max {
        Err(FrameError::TooLarge { len, max })
    } else {
        Ok(len)
    }
}

/// The bytes to put on the wire for a frame: length prefix and JSON.
pub fn encode(frame: &Frame, max: usize) -> Result<Vec<u8>, FrameError> {
    let json = serde_json::to_vec(frame)?;
    let len = check_len(json.len(), max)?;
    let mut out = Vec::with_capacity(HEADER_LEN + len);
    out.extend_from_slice(&(len as u32).to_be_bytes());
    out.extend_from_slice(&json);
    Ok(out)
}

/// The body length a header announces. The check runs before anything is
/// allocated, so a garbage prefix cannot make the reader allocate gigabytes.
pub fn decode_header(header: [u8; HEADER_LEN], max: usize) -> Result<usize, FrameError> {
    check_len(u32::from_be_bytes(header) as usize, max)
}

pub fn decode_payload(payload: &[u8]) -> Result<Frame, FrameError> {
    Ok(serde_json::from_slice(payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ApiError, ErrorCode};
    use crate::ids::*;
    use crate::wire::*;

    fn error_message() -> Message {
        Message::Error(ErrorResponse {
            schema_version: SchemaVersion::CURRENT,
            request_id: None,
            error: ApiError::new(ErrorCode::Internal, "x"),
        })
    }

    /// A spread of frames: every body, extreme stream ids, text that needs
    /// JSON escaping and non-ASCII.
    fn sample_frames() -> Vec<Frame> {
        let mut out = vec![Frame::ping(0), Frame::pong(u32::MAX)];
        for (i, text) in [
            "",
            "x",
            "quote \" and \\ and \n",
            "日本語 \u{1F600}",
            "\u{0}\u{1f}",
        ]
        .iter()
        .enumerate()
        {
            let m = Message::Error(ErrorResponse {
                schema_version: SchemaVersion::CURRENT,
                request_id: None,
                error: ApiError::new(ErrorCode::Internal, *text),
            });
            out.push(Frame::message([0, 1, u32::MAX][i % 3], m));
        }
        out
    }

    #[test]
    fn what_is_encoded_decodes_to_the_same_frame_with_a_truthful_prefix() {
        for frame in sample_frames() {
            let bytes = encode(&frame, MAX_FRAME_LEN).unwrap();
            let header: [u8; 4] = bytes[..4].try_into().unwrap();
            let len = decode_header(header, MAX_FRAME_LEN).unwrap();
            assert_eq!(len, bytes.len() - HEADER_LEN, "prefix is the payload size");
            assert_eq!(decode_payload(&bytes[4..]).unwrap(), frame);
            // Any strict prefix of the payload is not a frame: a cut stream is
            // never mistaken for a shorter valid one.
            for cut in 0..len {
                assert!(
                    decode_payload(&bytes[4..4 + cut]).is_err(),
                    "{frame:?} cut {cut}"
                );
            }
        }
    }

    #[test]
    fn the_size_limit_binds_encode_and_decode_at_the_same_byte() {
        for frame in sample_frames() {
            let len = serde_json::to_vec(&frame).unwrap().len();
            assert!(encode(&frame, len).is_ok());
            assert!(encode(&frame, len - 1).is_err());
            let header = (len as u32).to_be_bytes();
            assert_eq!(decode_header(header, len).unwrap(), len);
            assert!(decode_header(header, len - 1).is_err());
        }
    }

    #[test]
    fn constructors_set_stream_and_kind() {
        let f = Frame::ping(9);
        assert_eq!(
            (f.stream, f.kind, f.body),
            (9, StreamKind::Control, Body::Ping)
        );
        let f = Frame::pong(3);
        assert_eq!(
            (f.stream, f.kind, f.body),
            (3, StreamKind::Control, Body::Pong)
        );
        let f = Frame::message(4, error_message());
        assert_eq!(f.stream, 4);
        assert_eq!(f.kind, StreamKind::Control);
    }

    #[test]
    fn length_prefix_is_big_endian_and_exact() {
        let frame = Frame::ping(1);
        let bytes = encode(&frame, MAX_FRAME_LEN).unwrap();
        let json = serde_json::to_vec(&frame).unwrap();
        assert_eq!(&bytes[..4], &(json.len() as u32).to_be_bytes());
        assert_eq!(&bytes[4..], &json[..]);
        assert_eq!(decode_header([0, 0, 1, 0], MAX_FRAME_LEN).unwrap(), 256);
    }

    #[test]
    fn an_oversize_error_names_both_sizes() {
        let frame = Frame::ping(1);
        let len = serde_json::to_vec(&frame).unwrap().len();
        let e = encode(&frame, len - 1).unwrap_err();
        assert!(matches!(e, FrameError::TooLarge { .. }));
        let text = e.to_string();
        assert!(text.contains(&len.to_string()), "{text}");
        assert!(text.contains(&(len - 1).to_string()), "{text}");
    }

    #[test]
    fn rejects_empty_and_garbage_prefixes() {
        // The message is what the operator reads when a stream is out of step.
        let e = decode_header([0, 0, 0, 0], MAX_FRAME_LEN).unwrap_err();
        assert!(matches!(e, FrameError::Empty));
        assert!(e.to_string().contains("empty"));
        // "Welc" from a login banner on stdout is a huge length, not a frame.
        assert!(matches!(
            decode_header(*b"Welc", MAX_FRAME_LEN),
            Err(FrameError::TooLarge { .. })
        ));
        assert!(matches!(
            decode_header([0xff; 4], MAX_FRAME_LEN),
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[test]
    fn rejects_malformed_payloads() {
        assert!(matches!(
            decode_payload(b"not json"),
            Err(FrameError::Malformed(_))
        ));
        assert!(matches!(
            decode_payload(br#"{"stream":1,"kind":"control"}"#),
            Err(FrameError::Malformed(_))
        ));
        assert!(matches!(
            decode_payload(br#"{"stream":1,"kind":"nope","body":{"t":"ping"}}"#),
            Err(FrameError::Malformed(_))
        ));
    }

    #[test]
    fn kinds_follow_the_operation_in_both_directions() {
        use Operation::*;
        for op in Operation::ALL {
            let want = match op {
                ReadLog => StreamKind::Log,
                BlobPut | BlobGet | SecretBundlePut => StreamKind::Blob,
                _ => StreamKind::Control,
            };
            assert_eq!(kind_of_operation(op), want, "{op:?}");
        }
        let rid = RequestId::generate();
        let req = Request::ReadLog(ReadLogRequest {
            schema_version: SchemaVersion::CURRENT,
            request_id: rid.clone(),
            attempt_id: AttemptId::generate(),
            node_id: NodeId::parse("n").unwrap(),
            stream: LogStream::Stdout,
            offset: 0,
            max_bytes: 1,
        });
        assert_eq!(kind_of(&Message::Request(req)), StreamKind::Log);
        let resp = Response::BlobGet(BlobGetResponse {
            schema_version: SchemaVersion::CURRENT,
            digest: crate::ids::Digest::of_bytes(b""),
            offset: 0,
            total_size: 0,
            data: Bytes(vec![]),
            eof: true,
        });
        let m = Message::Response {
            request_id: rid,
            response: resp,
        };
        assert_eq!(kind_of(&m), StreamKind::Blob);
        assert_eq!(kind_of(&error_message()), StreamKind::Control);
    }
}
