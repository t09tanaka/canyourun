#[path = "proto_common/mod.rs"]
mod common;

use common::*;
use remote_task_runner::error::ErrorCode;
use remote_task_runner::ids::*;
use remote_task_runner::protocol::chunk::{ChunkReceiver, LogCursor};
use remote_task_runner::protocol::frame::{encode, Body, Frame, MAX_FRAME_LEN};
use remote_task_runner::protocol::io::{read_frame, write_frame};
use remote_task_runner::protocol::mux::{run_writer, Outbox, Shared};
use remote_task_runner::protocol::sched::Outgoing;
use remote_task_runner::protocol::transfer::{get_blob, put_blob, read_log, TransferError};
use remote_task_runner::protocol::transport::duplex_pair;
use remote_task_runner::protocol::{accept, connect, CallError, ConnectError, Unconfirmed};
use remote_task_runner::wire::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

// --- hello ----------------------------------------------------------------

#[tokio::test]
async fn a_matching_peer_connects_and_requests_round_trip() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    assert_eq!(conn.negotiated.version, 1);
    assert_eq!(conn.worker.worker_id.as_str(), "w1");
    assert_eq!(conn.worker.cluster_id, cluster);
    assert_eq!(srv.negotiated, conn.negotiated);
    serve(srv.incoming, Arc::new(|r| Some(Ok(ack_response(&r)))));
    let req = ack_request();
    let resp = conn.client.call(req.clone(), T).await.unwrap();
    assert_eq!(resp, ack_response(&req));
    assert!(conn.client.ping(T).await.is_ok());
}

#[tokio::test]
async fn incompatible_versions_are_refused_on_both_sides() {
    let cluster = ClusterId::generate();
    let (a, b) = duplex_pair(4096);
    let mut cfg = client_cfg(&cluster);
    cfg.protocol_versions = vec![9];
    let (c, s) = tokio::join!(connect(a, cfg.clone()), accept(b, server_cfg(&cluster)));
    match c {
        Err(ConnectError::Refused(e)) => assert_eq!(e.code, ErrorCode::ProtocolIncompatible),
        _ => panic!("coordinator should be refused"),
    }
    match s {
        Err(ConnectError::Incompatible(e)) => assert_eq!(e.code, ErrorCode::ProtocolIncompatible),
        _ => panic!("worker should refuse"),
    }
}

#[tokio::test]
async fn a_different_cluster_is_refused() {
    let (a, b) = duplex_pair(4096);
    let (c, s) = tokio::join!(
        connect(a, client_cfg(&ClusterId::generate())),
        accept(b, server_cfg(&ClusterId::generate()))
    );
    match c {
        Err(ConnectError::Refused(e)) => {
            assert_eq!(e.code, ErrorCode::ProtocolIncompatible);
            assert!(e.message.contains("cluster_id"));
        }
        _ => panic!("expected refusal"),
    }
    assert!(s.is_err());
}

#[tokio::test]
async fn a_required_feature_the_worker_lacks_is_refused() {
    let cluster = ClusterId::generate();
    let (a, b) = duplex_pair(4096);
    let mut cfg = client_cfg(&cluster);
    cfg.required_features = vec!["telepathy".into()];
    cfg.features.push("telepathy".into());
    let (c, _s) = tokio::join!(connect(a, cfg.clone()), accept(b, server_cfg(&cluster)));
    // The worker does not know the feature, so it is not negotiated; the
    // coordinator refuses the answer.
    match c {
        Err(ConnectError::Incompatible(e)) => assert!(e.message.contains("telepathy")),
        _ => panic!("expected incompatible"),
    }
}

#[tokio::test]
async fn another_schema_version_in_hello_is_refused_with_protocol_incompatible() {
    let cluster = ClusterId::generate();
    let (mut raw, b) = duplex_pair(4096);
    let server = tokio::spawn(async move { accept(b, server_cfg(&cluster.clone())).await });
    let cluster2 = ClusterId::generate();
    let hello = HelloRequest {
        schema_version: SchemaVersion(99),
        request_id: RequestId::generate(),
        cluster_id: cluster2,
        coordinator_id: CoordinatorId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        protocol_versions: vec![1],
        features: vec![],
    };
    let f = Frame::message(0, Message::Request(Request::Hello(hello)));
    write_frame(&mut raw.writer, &f, MAX_FRAME_LEN)
        .await
        .unwrap();
    let reply = read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .unwrap();
    match reply.body {
        Body::Message(Message::Error(e)) => {
            assert_eq!(e.error.code, ErrorCode::ProtocolIncompatible)
        }
        other => panic!("{other:?}"),
    }
    assert!(server.await.unwrap().is_err());
    // The worker closed the connection.
    assert!(read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn a_worker_must_get_hello_first() {
    let cluster = ClusterId::generate();
    let (mut raw, b) = duplex_pair(4096);
    let server = tokio::spawn(async move { accept(b, server_cfg(&cluster)).await });
    write_frame(&mut raw.writer, &Frame::ping(1), MAX_FRAME_LEN)
        .await
        .unwrap();
    let reply = read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(reply.body, Body::Message(Message::Error(_))));
    assert!(matches!(
        server.await.unwrap(),
        Err(ConnectError::Unexpected(_))
    ));
}

#[tokio::test]
async fn a_lying_worker_answer_is_refused_by_the_coordinator() {
    let cluster = ClusterId::generate();
    let (a, mut raw) = duplex_pair(4096);
    let c2 = cluster.clone();
    let coordinator = tokio::spawn(async move { connect(a, client_cfg(&c2)).await });
    let req = read_frame(&mut raw.reader, MAX_FRAME_LEN)
        .await
        .unwrap()
        .unwrap();
    let Body::Message(Message::Request(Request::Hello(h))) = req.body else {
        panic!("expected hello")
    };
    let mut resp = identity("w1").response(
        &cluster,
        &remote_task_runner::protocol::hello::Negotiated {
            version: 77,
            features: vec![],
        },
    );
    resp.protocol_version = 77;
    let reply = Frame::message(
        req.stream,
        Message::Response {
            request_id: h.request_id,
            response: Response::Hello(resp),
        },
    );
    write_frame(&mut raw.writer, &reply, MAX_FRAME_LEN)
        .await
        .unwrap();
    assert!(matches!(
        coordinator.await.unwrap(),
        Err(ConnectError::Incompatible(_))
    ));
}

#[tokio::test]
async fn hello_times_out_and_a_silent_close_is_reported() {
    let cluster = ClusterId::generate();
    let (a, _keep) = duplex_pair(4096);
    let mut cfg = client_cfg(&cluster);
    cfg.hello_timeout = Duration::from_millis(50);
    match connect(a, cfg.clone()).await {
        Err(ConnectError::TimedOut) => {}
        _ => panic!("expected timeout"),
    }
    let (a, b) = duplex_pair(4096);
    drop(b);
    assert!(matches!(
        connect(a, client_cfg(&cluster)).await,
        Err(ConnectError::Closed) | Err(ConnectError::Frame(_))
    ));
    let e = ConnectError::TimedOut.api_error();
    assert_eq!(e.code, ErrorCode::CommunicationUnknown);
}

// --- unconfirmed communication --------------------------------------------

#[tokio::test]
async fn a_disconnect_leaves_outstanding_requests_unconfirmed() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let caller = {
        let client = conn.client.clone();
        tokio::spawn(async move { client.call(ack_request(), T).await })
    };
    // The worker received the request and then died without answering.
    let inbound = incoming.next().await.unwrap();
    drop(inbound);
    drop(incoming);
    let err = caller.await.unwrap().unwrap_err();
    assert!(matches!(
        err,
        CallError::Unconfirmed(Unconfirmed::Disconnected)
    ));
    assert!(err.is_unconfirmed());
    assert_eq!(err.to_api_error().code, ErrorCode::CommunicationUnknown);
    // The next call on the dead connection was certainly not sent.
    let err = conn.client.call(ack_request(), T).await.unwrap_err();
    assert!(matches!(err, CallError::NotSent), "{err:?}");
    assert!(!err.is_unconfirmed());
    assert!(conn.client.is_closed());
}

#[tokio::test]
async fn a_timeout_is_unconfirmed_and_a_late_answer_is_ignored() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut incoming = srv.incoming;
    let err = {
        let call = conn.client.call(ack_request(), Duration::from_millis(50));
        let (r, _) = tokio::join!(call, async {
            // Hold the request past the caller's timeout.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        r.unwrap_err()
    };
    assert!(matches!(err, CallError::Unconfirmed(Unconfirmed::TimedOut)));
    let late = incoming.next().await.unwrap();
    let resp = ack_response(&late.request);
    late.responder.respond(resp).await.unwrap();
    // The connection is unharmed: the late reply is dropped, new calls work.
    serve(incoming, Arc::new(|r| Some(Ok(ack_response(&r)))));
    assert!(conn.client.call(ack_request(), T).await.is_ok());
    assert!(conn.client.ping(T).await.is_ok());
}

#[tokio::test]
async fn a_worker_error_comes_back_as_a_remote_error_not_unconfirmed() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    serve(
        srv.incoming,
        Arc::new(|_| {
            Some(Err(remote_task_runner::error::ApiError::new(
                ErrorCode::StaleEpoch,
                "old",
            )))
        }),
    );
    match conn.client.call(ack_request(), T).await.unwrap_err() {
        CallError::Remote(e) => assert_eq!(e.code, ErrorCode::StaleEpoch),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_request_over_the_frame_limit_is_refused_before_sending() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, Arc::new(|r| Some(Ok(ack_response(&r)))));
    let big = Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        digest: Digest::of_bytes(b""),
        total_size: 0,
        offset: 0,
        data: Bytes(vec![0u8; MAX_FRAME_LEN]),
    });
    assert!(matches!(
        conn.client.call(big, T).await.unwrap_err(),
        CallError::Frame(_)
    ));
    // The connection is still fine.
    assert!(conn.client.call(ack_request(), T).await.is_ok());
}

// --- control before bulk ---------------------------------------------------

fn blob_item(i: u32) -> Outgoing {
    let req = Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        digest: Digest::of_bytes(b""),
        total_size: 0,
        offset: 0,
        data: Bytes(vec![i as u8; 100_000]),
    });
    let frame = Frame::message(i + 1, Message::Request(req));
    Outgoing {
        kind: frame.kind,
        bytes: encode(&frame, MAX_FRAME_LEN).unwrap(),
    }
}

#[tokio::test]
async fn a_heartbeat_overtakes_a_queue_of_large_blob_frames() {
    // A tiny link makes the writer block inside the first blob frame; thirty
    // more are queued behind it. The heartbeat must come out right after the
    // frame in flight, not after the queue.
    let (a, mut b) = duplex_pair(2048);
    let outbox = Outbox::new();
    tokio::spawn(run_writer(
        a.writer,
        Arc::clone(&outbox),
        Shared::new(MAX_FRAME_LEN),
    ));
    let producer = {
        let outbox = Arc::clone(&outbox);
        tokio::spawn(async move {
            for i in 0..30 {
                outbox.send(blob_item(i)).await.unwrap();
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    outbox
        .send_control(&Frame::ping(500), MAX_FRAME_LEN)
        .unwrap();
    let mut ping_at = None;
    for n in 0..40 {
        let f = read_frame(&mut b.reader, MAX_FRAME_LEN)
            .await
            .unwrap()
            .unwrap();
        if matches!(f.body, Body::Ping) {
            ping_at = Some(n);
            break;
        }
    }
    let ping_at = ping_at.expect("the ping was written");
    assert!(ping_at <= 1, "ping came after {ping_at} blob frames");
    producer.abort();
}

#[tokio::test]
async fn pings_stay_fast_while_a_blob_transfers_over_a_real_connection() {
    let cluster = ClusterId::generate();
    let (a, b) = duplex_pair(16 * 1024);
    let (c, s) = tokio::join!(
        connect(a, client_cfg(&cluster)),
        accept(b, server_cfg(&cluster))
    );
    let (conn, srv) = (c.unwrap(), s.unwrap());
    let state: Arc<Mutex<Option<ChunkReceiver>>> = Arc::new(Mutex::new(None));
    serve(srv.incoming, blob_worker(Arc::clone(&state)));
    let data = pseudo_random(3 * 1024 * 1024);
    let client = conn.client.clone();
    let source = SourceId::generate();
    let put = {
        let data = data.clone();
        tokio::spawn(async move {
            put_blob(
                &client,
                CoordinatorEpoch(1),
                &source,
                &data,
                0,
                64 * 1024,
                T,
            )
            .await
        })
    };
    let mut worst = Duration::ZERO;
    while !put.is_finished() {
        worst = worst.max(conn.client.ping(T).await.unwrap());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(put.await.unwrap().unwrap().complete);
    // Generous: one 64 KiB frame over an in-memory link, not the 3 MiB blob.
    assert!(worst < Duration::from_secs(2), "worst ping {worst:?}");
}

// --- blob transfer ---------------------------------------------------------

/// A worker that stores one blob, keeping its state across connections.
fn blob_worker(state: Arc<Mutex<Option<ChunkReceiver>>>) -> Handler {
    Arc::new(move |req| match req {
        Request::BlobPut(r) => {
            let mut guard = state.lock().unwrap();
            let rcv =
                guard.get_or_insert_with(|| ChunkReceiver::new(r.digest.clone(), r.total_size));
            Some(match rcv.accept(r.offset, &r.data.0) {
                Ok(a) => Ok(Response::BlobPut(BlobPutResponse {
                    schema_version: SchemaVersion::CURRENT,
                    digest: r.digest,
                    received: a.received,
                    complete: a.complete,
                })),
                Err(remote_task_runner::protocol::chunk::ChunkError::Gap { received }) => {
                    Ok(Response::BlobPut(BlobPutResponse {
                        schema_version: SchemaVersion::CURRENT,
                        digest: r.digest,
                        received,
                        complete: false,
                    }))
                }
                Err(e) => Err(remote_task_runner::error::ApiError::new(
                    ErrorCode::Conflict,
                    e.to_string(),
                )),
            })
        }
        _ => None,
    })
}

#[tokio::test]
async fn an_interrupted_blob_upload_resumes_from_the_workers_offset() {
    let cluster = ClusterId::generate();
    let data = pseudo_random(1000);
    let source = SourceId::generate();
    let state: Arc<Mutex<Option<ChunkReceiver>>> = Arc::new(Mutex::new(None));

    // First connection: the worker takes three chunks and then dies.
    let (conn, srv) = pair(&cluster).await;
    let seen = Arc::new(Mutex::new(0u32));
    let handler: Handler = {
        let inner = blob_worker(Arc::clone(&state));
        let seen = Arc::clone(&seen);
        Arc::new(move |req| {
            let mut n = seen.lock().unwrap();
            *n += 1;
            if *n > 3 {
                None
            } else {
                inner(req)
            }
        })
    };
    let worker = serve(srv.incoming, handler);
    let err = put_blob(
        &conn.client,
        CoordinatorEpoch(1),
        &source,
        &data,
        0,
        100,
        Duration::from_millis(100),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, TransferError::Call(CallError::Unconfirmed(_))),
        "{err:?}"
    );
    worker.abort();
    assert_eq!(state.lock().unwrap().as_ref().unwrap().received(), 300);

    // Second connection: the sender starts from 0 again; the worker's offset
    // steers it, and only the missing 700 bytes count as new.
    let (conn, srv) = pair(&cluster).await;
    serve(srv.incoming, blob_worker(Arc::clone(&state)));
    let done = put_blob(&conn.client, CoordinatorEpoch(1), &source, &data, 0, 100, T)
        .await
        .unwrap();
    assert!(done.complete);
    assert_eq!(done.received, 1000);
}

#[tokio::test]
async fn a_blob_with_the_wrong_digest_is_refused_and_not_completed() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let state = Arc::new(Mutex::new(None));
    serve(srv.incoming, blob_worker(state));
    let data = pseudo_random(64);
    let lie = Request::BlobPut(BlobPutRequest {
        schema_version: SchemaVersion::CURRENT,
        request_id: RequestId::generate(),
        coordinator_epoch: CoordinatorEpoch(1),
        source_id: SourceId::generate(),
        digest: Digest::of_bytes(b"not these bytes"),
        total_size: 64,
        offset: 0,
        data: Bytes(data),
    });
    match conn.client.call(lie, T).await.unwrap_err() {
        CallError::Remote(e) => {
            assert_eq!(e.code, ErrorCode::Conflict);
            assert!(e.message.contains("digest mismatch"));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_blob_download_resumes_from_a_verified_prefix_and_checks_the_digest() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let data = Arc::new(pseudo_random(5000));
    let digest = Digest::of_bytes(&data);
    let store = Arc::clone(&data);
    serve(
        srv.incoming,
        Arc::new(move |req| match req {
            Request::BlobGet(r) => {
                let start = (r.offset as usize).min(store.len());
                let end = (start + r.max_bytes as usize).min(store.len());
                Some(Ok(Response::BlobGet(BlobGetResponse {
                    schema_version: SchemaVersion::CURRENT,
                    digest: r.digest,
                    offset: start as u64,
                    total_size: store.len() as u64,
                    data: Bytes(store[start..end].to_vec()),
                    eof: end == store.len(),
                })))
            }
            _ => None,
        }),
    );
    let whole = get_blob(&conn.client, &digest, vec![], 700, T)
        .await
        .unwrap();
    assert_eq!(whole, *data);
    let resumed = get_blob(&conn.client, &digest, data[..1234].to_vec(), 700, T)
        .await
        .unwrap();
    assert_eq!(resumed, *data);
    // A prefix that is not the start of this blob fails the digest check.
    let mut bad = data[..1234].to_vec();
    bad[0] ^= 1;
    let err = get_blob(&conn.client, &digest, bad, 700, T)
        .await
        .unwrap_err();
    assert!(matches!(err, TransferError::Chunk(_)), "{err:?}");
}

// --- logs ------------------------------------------------------------------

#[tokio::test]
async fn non_utf8_log_bytes_round_trip_and_a_reread_is_harmless() {
    let cluster = ClusterId::generate();
    let (conn, srv) = pair(&cluster).await;
    let mut spool = vec![0xff, 0xfe, 0x00, 0x80, b'\n', 0xc3, 0x28, 0xf0, 0x9f, 0x92];
    spool.extend(pseudo_random(2000));
    let store = Arc::new(spool.clone());
    serve(
        srv.incoming,
        Arc::new(move |req| match req {
            Request::ReadLog(r) => {
                let start = (r.offset as usize).min(store.len());
                let end = (start + r.max_bytes as usize).min(store.len());
                Some(Ok(Response::ReadLog(ReadLogResponse {
                    schema_version: SchemaVersion::CURRENT,
                    offset: start as u64,
                    data: Bytes(store[start..end].to_vec()),
                    next_offset: end as u64,
                    eof: end == store.len(),
                    log_truncated: false,
                    dropped_bytes: 0,
                })))
            }
            _ => None,
        }),
    );
    let attempt = AttemptId::generate();
    let node = NodeId::parse("build").unwrap();
    let mut cursor = LogCursor::new(0);
    let mut got = Vec::new();
    loop {
        let r = read_log(
            &conn.client,
            &attempt,
            &node,
            LogStream::Stdout,
            &mut cursor,
            300,
            T,
        )
        .await
        .unwrap();
        got.extend_from_slice(&r.fresh);
        if r.eof {
            break;
        }
    }
    assert_eq!(got, spool);
    assert!(std::str::from_utf8(&got).is_err());
    // After a reconnect the reader may ask again from an older offset; the
    // cursor drops what it already has.
    let mut again = LogCursor::new(0);
    let r = read_log(
        &conn.client,
        &attempt,
        &node,
        LogStream::Stdout,
        &mut again,
        300,
        T,
    )
    .await
    .unwrap();
    assert_eq!(r.fresh, spool[..300]);
    let r2 = read_log(
        &conn.client,
        &attempt,
        &node,
        LogStream::Stdout,
        &mut again,
        300,
        T,
    )
    .await
    .unwrap();
    assert_eq!(r2.fresh, spool[300..600]);
}
