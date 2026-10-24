use super::{Error, Op, diff_revisions};
use crate::JsonValue;
use parking_lot::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Open,
    Prepared,
    Consumed,
    Aborted,
}
struct Lease {
    owner: Arc<AtomicU64>,
    revision: u64,
    status: Mutex<Status>,
}
impl Lease {
    fn stale(&self) -> bool {
        self.owner.load(Ordering::Acquire) != self.revision
    }
    fn abort(&self) {
        if self.stale() {
            return;
        }
        let mut status = self.status.lock();
        if matches!(*status, Status::Open | Status::Prepared) {
            *status = Status::Aborted;
        }
    }
}
fn error(message: &str) -> Error {
    Error::Type(message.into())
}

pub struct Tracker {
    value: Arc<JsonValue>,
    owner: Arc<AtomicU64>,
}
pub struct Change {
    base: Arc<JsonValue>,
    state: Option<JsonValue>,
    lease: Arc<Lease>,
    settled: bool,
}
pub struct Prepared {
    base: Arc<JsonValue>,
    value: Arc<JsonValue>,
    ops: Vec<Op>,
    lease: Arc<Lease>,
}

pub fn track(value: JsonValue) -> Tracker {
    Tracker::new(Arc::new(value))
}
impl Tracker {
    pub fn new(value: Arc<JsonValue>) -> Self {
        Self {
            value,
            owner: Arc::new(AtomicU64::new(0)),
        }
    }
    pub fn value(&self) -> &Arc<JsonValue> {
        &self.value
    }
    pub fn revision(&self) -> u64 {
        self.owner.load(Ordering::Acquire)
    }
    fn lease(&self, status: Status) -> Arc<Lease> {
        Arc::new(Lease {
            owner: self.owner.clone(),
            revision: self.revision(),
            status: Mutex::new(status),
        })
    }
    pub fn begin_change(&self) -> Change {
        Change {
            base: self.value.clone(),
            state: Some(self.value.as_ref().clone()),
            lease: self.lease(Status::Open),
            settled: false,
        }
    }
    pub fn prepare_replace(&self, value: Arc<JsonValue>) -> Prepared {
        let same = value.as_ref() == self.value.as_ref();
        Prepared {
            base: self.value.clone(),
            ops: if same {
                vec![]
            } else {
                vec![Op::Replace(value.as_ref().clone())]
            },
            value: if same { self.value.clone() } else { value },
            lease: self.lease(Status::Prepared),
        }
    }
    /// The candidate is materialized before persistence. Adoption only validates
    /// the lease and swaps the authoritative root; it performs no replay or I/O.
    pub fn adopt(&mut self, prepared: &Prepared) -> Result<(), Error> {
        if !Arc::ptr_eq(&self.owner, &prepared.lease.owner) {
            return Err(error("Prepared change belongs to a different tracker"));
        }
        let mut status = prepared.lease.status.lock();
        match *status {
            Status::Consumed => return Err(error("Prepared change has already been used")),
            Status::Aborted => return Err(error("Prepared change has been aborted")),
            Status::Open => return Err(error("Prepared change is not ready")),
            Status::Prepared => {}
        }
        if prepared.lease.stale() || !Arc::ptr_eq(&self.value, &prepared.base) {
            return Err(error("Prepared change is stale"));
        }
        self.value = prepared.value.clone();
        *status = Status::Consumed;
        self.owner.fetch_add(1, Ordering::Release);
        Ok(())
    }
}

impl Change {
    pub fn state(&mut self) -> Result<&mut JsonValue, Error> {
        if self.settled || self.lease.stale() || *self.lease.status.lock() != Status::Open {
            return Err(error("Cannot use a settled overlay"));
        }
        self.state
            .as_mut()
            .ok_or_else(|| error("Cannot use a settled overlay"))
    }
    pub fn prepare(&mut self) -> Result<Prepared, Error> {
        if self.settled {
            return Err(error("Change has already been settled"));
        }
        if self.lease.stale() || *self.lease.status.lock() != Status::Open {
            return Err(error("Cannot use a settled overlay"));
        }
        let value = self.state.take().expect("open change owns draft");
        let ops = diff_revisions(&self.base, &value);
        let value = if ops.is_empty() {
            self.base.clone()
        } else {
            Arc::new(value)
        };
        *self.lease.status.lock() = Status::Prepared;
        self.settled = true;
        Ok(Prepared {
            base: self.base.clone(),
            value,
            ops,
            lease: self.lease.clone(),
        })
    }
    pub fn abort(&mut self) {
        self.lease.abort();
        self.settled = true;
        self.state = None;
    }
}
impl Prepared {
    pub fn base(&self) -> &Arc<JsonValue> {
        &self.base
    }
    pub fn value(&self) -> &Arc<JsonValue> {
        &self.value
    }
    pub fn ops(&self) -> &[Op] {
        &self.ops
    }
    pub fn base_revision(&self) -> u64 {
        self.lease.revision
    }
    pub fn abort(&self) {
        self.lease.abort();
    }
}
