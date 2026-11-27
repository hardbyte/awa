use std::sync::{LockResult, Mutex, MutexGuard};

#[track_caller]
fn unpoisoned<Guard>(result: LockResult<Guard>) -> Guard {
    #[expect(
        clippy::expect_used,
        reason = "a poisoned lock means a holder panicked; its state is not trusted"
    )]
    result.expect("lock poisoned")
}

pub(crate) trait MutexExt<Value> {
    fn lock_unpoisoned(&self) -> MutexGuard<'_, Value>;
}

impl<Value> MutexExt<Value> for Mutex<Value> {
    #[track_caller]
    fn lock_unpoisoned(&self) -> MutexGuard<'_, Value> {
        unpoisoned(self.lock())
    }
}
