//! Which queued frame is written next. Control frames always go first, so a
//! large `blob_get` reply or a burst of log chunks delays a heartbeat by at most
//! the frame already being written. Log and blob frames take turns.

use super::frame::StreamKind;
use std::collections::VecDeque;

/// A frame already turned into bytes, with the queue it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    pub kind: StreamKind,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct Scheduler {
    control: VecDeque<Outgoing>,
    log: VecDeque<Outgoing>,
    blob: VecDeque<Outgoing>,
    /// The log queue goes before the blob queue on the next bulk pick.
    log_next: bool,
    /// Sum of `bytes.len()` over the control queue.
    control_bytes: usize,
}

impl Scheduler {
    pub fn new() -> Self {
        Self::default()
    }

    fn queue(&mut self, kind: StreamKind) -> &mut VecDeque<Outgoing> {
        match kind {
            StreamKind::Control => &mut self.control,
            StreamKind::Log => &mut self.log,
            StreamKind::Blob => &mut self.blob,
        }
    }

    pub fn push(&mut self, item: Outgoing) {
        if item.kind == StreamKind::Control {
            self.control_bytes += item.bytes.len();
        }
        self.queue(item.kind).push_back(item);
    }

    /// Control frames waiting, and their total size. The caller bounds these: a
    /// peer that never reads must not grow them without limit.
    pub fn control_len(&self) -> usize {
        self.control.len()
    }

    pub fn control_bytes(&self) -> usize {
        self.control_bytes
    }

    pub fn pop(&mut self) -> Option<Outgoing> {
        if let Some(item) = self.control.pop_front() {
            self.control_bytes -= item.bytes.len();
            return Some(item);
        }
        let order = if self.log_next {
            [StreamKind::Log, StreamKind::Blob]
        } else {
            [StreamKind::Blob, StreamKind::Log]
        };
        for kind in order {
            if let Some(item) = self.queue(kind).pop_front() {
                self.log_next = kind == StreamKind::Blob;
                return Some(item);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.control.len() + self.log.len() + self.blob.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(kind: StreamKind, tag: u8) -> Outgoing {
        Outgoing {
            kind,
            bytes: vec![tag],
        }
    }

    fn drain(s: &mut Scheduler) -> Vec<(StreamKind, u8)> {
        std::iter::from_fn(|| s.pop())
            .map(|o| (o.kind, o.bytes[0]))
            .collect()
    }

    #[test]
    fn control_overtakes_queued_bulk() {
        let mut s = Scheduler::new();
        for i in 0..50 {
            s.push(item(StreamKind::Blob, i));
        }
        s.push(item(StreamKind::Log, 99));
        s.push(item(StreamKind::Control, 200));
        s.push(item(StreamKind::Control, 201));
        assert_eq!(s.len(), 53);
        assert_eq!(s.pop().unwrap().bytes, vec![200]);
        assert_eq!(s.pop().unwrap().bytes, vec![201]);
        // A control frame that arrives between bulk frames goes next again.
        s.push(item(StreamKind::Control, 202));
        assert_eq!(s.pop().unwrap().bytes, vec![202]);
    }

    #[test]
    fn control_size_is_tracked_through_push_and_pop() {
        let mut s = Scheduler::new();
        assert_eq!((s.control_len(), s.control_bytes()), (0, 0));
        s.push(Outgoing {
            kind: StreamKind::Control,
            bytes: vec![0; 5],
        });
        s.push(Outgoing {
            kind: StreamKind::Control,
            bytes: vec![0; 7],
        });
        // Bulk frames are not counted: they have their own backpressure.
        s.push(Outgoing {
            kind: StreamKind::Blob,
            bytes: vec![0; 100],
        });
        assert_eq!((s.control_len(), s.control_bytes()), (2, 12));
        s.pop();
        assert_eq!((s.control_len(), s.control_bytes()), (1, 7));
        s.pop();
        s.pop();
        assert_eq!((s.control_len(), s.control_bytes()), (0, 0));
    }

    #[test]
    fn each_queue_is_fifo() {
        let mut s = Scheduler::new();
        for i in 0..3 {
            s.push(item(StreamKind::Control, i));
        }
        for i in 10..13 {
            s.push(item(StreamKind::Blob, i));
        }
        let order: Vec<u8> = drain(&mut s).into_iter().map(|(_, t)| t).collect();
        assert_eq!(order, vec![0, 1, 2, 10, 11, 12]);
    }

    #[test]
    fn log_and_blob_take_turns_and_neither_starves() {
        let mut s = Scheduler::new();
        for i in 0..3 {
            s.push(item(StreamKind::Log, i));
            s.push(item(StreamKind::Blob, 10 + i));
        }
        let kinds: Vec<StreamKind> = drain(&mut s).into_iter().map(|(k, _)| k).collect();
        assert_eq!(
            kinds,
            vec![
                StreamKind::Blob,
                StreamKind::Log,
                StreamKind::Blob,
                StreamKind::Log,
                StreamKind::Blob,
                StreamKind::Log
            ]
        );
    }

    #[test]
    fn a_single_bulk_queue_drains_and_the_scheduler_empties() {
        let mut s = Scheduler::new();
        assert!(s.is_empty());
        assert!(s.pop().is_none());
        s.push(item(StreamKind::Log, 1));
        s.push(item(StreamKind::Log, 2));
        assert!(!s.is_empty());
        assert_eq!(
            drain(&mut s),
            vec![(StreamKind::Log, 1), (StreamKind::Log, 2)]
        );
        assert!(s.is_empty());
        s.push(item(StreamKind::Blob, 3));
        s.push(item(StreamKind::Blob, 4));
        assert_eq!(
            drain(&mut s),
            vec![(StreamKind::Blob, 3), (StreamKind::Blob, 4)]
        );
    }
}
