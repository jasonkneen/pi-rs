use super::{Error, Op, apply_immutable, diff_revisions};
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
enum Edits {
    Diff,
    Recorded {
        ops: Vec<Op>,
        untracked_base: Option<JsonValue>,
    },
}
pub struct Change {
    base: Option<Arc<JsonValue>>,
    state: Option<JsonValue>,
    edits: Edits,
    lease: Arc<Lease>,
    settled: bool,
}
pub struct Prepared {
    base: Arc<JsonValue>,
    value: Arc<JsonValue>,
    ops: Arc<Vec<Op>>,
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
            base: Some(self.value.clone()),
            state: Some(self.value.as_ref().clone()),
            edits: Edits::Diff,
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
            }
            .into(),
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
    fn check_open(&self) -> Result<(), Error> {
        if self.settled || self.lease.stale() || *self.lease.status.lock() != Status::Open {
            return Err(error("Cannot use a settled overlay"));
        }
        Ok(())
    }
    pub fn current(&self) -> Result<&JsonValue, Error> {
        self.check_open()?;
        self.state
            .as_ref()
            .ok_or_else(|| error("Cannot use a settled overlay"))
    }
    pub fn state(&mut self) -> Result<&mut JsonValue, Error> {
        self.check_open()?;
        if let Edits::Recorded { untracked_base, .. } = &mut self.edits
            && untracked_base.is_none()
        {
            *untracked_base = self.state.clone();
        }
        self.state
            .as_mut()
            .ok_or_else(|| error("Cannot use a settled overlay"))
    }
    fn pending_diff(&self) -> Vec<Op> {
        let current = self.state.as_ref().expect("open change owns draft");
        match &self.edits {
            Edits::Diff => {
                diff_revisions(self.base.as_ref().expect("open change owns base"), current)
            }
            Edits::Recorded {
                untracked_base: Some(base),
                ..
            } => diff_revisions(base, current),
            Edits::Recorded {
                untracked_base: None,
                ..
            } => Vec::new(),
        }
    }
    /// Apply and retain an explicit operation stream, including structural edits
    /// whose final value equals the base. Failure changes neither draft nor log.
    pub fn apply_ops(&mut self, ops: &[Op]) -> Result<(), Error> {
        self.apply_ops_checked(ops, |_| Ok(()))
    }
    /// Validate the candidate before adopting it into this open change. Object
    /// document boundaries use this to reject a scalar root without replaying twice.
    pub fn apply_ops_checked(
        &mut self,
        ops: &[Op],
        validate: impl FnOnce(&JsonValue) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let current = self.current()?;
        if ops.is_empty() {
            return validate(current);
        }
        let candidate = apply_immutable(Some(current), ops)?;
        validate(&candidate)?;
        let pending = self.pending_diff();
        let mut recorded = match std::mem::replace(&mut self.edits, Edits::Diff) {
            Edits::Diff => Vec::new(),
            Edits::Recorded { ops, .. } => ops,
        };
        recorded.extend(pending);
        recorded.extend_from_slice(ops);
        self.state = Some(candidate);
        self.edits = Edits::Recorded {
            ops: recorded,
            untracked_base: None,
        };
        Ok(())
    }
    pub fn prepare(&mut self) -> Result<Prepared, Error> {
        if self.settled {
            return Err(error("Change has already been settled"));
        }
        self.check_open()?;
        let pending = self.pending_diff();
        let replay = !matches!(
            &self.edits,
            Edits::Recorded {
                untracked_base: None,
                ..
            }
        );
        let mut ops = match std::mem::replace(&mut self.edits, Edits::Diff) {
            Edits::Diff => Vec::new(),
            Edits::Recorded { ops, .. } => ops,
        };
        ops.extend(pending);
        // Replacements reset persisted path dictionaries and classify a base.
        // Earlier operations are overwritten, so keep the last replacement at
        // index zero just as a flushed reference batch does.
        if let Some(index) = ops.iter().rposition(|op| matches!(op, Op::Replace(_))) {
            ops.drain(..index);
        }
        let base = self.base.as_ref().expect("open change owns base").clone();
        let replayed = if replay && !ops.is_empty() {
            Some(Arc::new(apply_immutable(Some(&base), &ops)?))
        } else {
            None
        };
        self.base = None;
        let value = self.state.take().expect("open change owns draft");
        let value = if ops.is_empty() {
            base.clone()
        } else {
            replayed.unwrap_or_else(|| Arc::new(value))
        };
        *self.lease.status.lock() = Status::Prepared;
        self.settled = true;
        Ok(Prepared {
            base,
            value,
            ops: ops.into(),
            lease: self.lease.clone(),
        })
    }
    pub fn abort(&mut self) {
        self.lease.abort();
        self.settled = true;
        self.state = None;
        self.base = None;
        self.edits = Edits::Diff;
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
    /// The operation batch shared by persistence, policy callbacks and publication.
    pub fn shared_ops(&self) -> Arc<Vec<Op>> {
        self.ops.clone()
    }
    pub fn base_revision(&self) -> u64 {
        self.lease.revision
    }
    pub fn abort(&self) {
        self.lease.abort();
    }
}
