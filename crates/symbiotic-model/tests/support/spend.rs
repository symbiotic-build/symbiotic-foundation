// Reuse the production SQLite implementation without linking SQLite into model contracts.
// The embedded implementation also exports credential-only transaction APIs.
#[allow(dead_code)]
#[path = "../../../symbiotic-ai-runtime/src/spend.rs"]
mod sqlite;
use std::sync::Arc;
use symbiotic_model::{
    ModelError, SpendLedger, SpendReceipt, SpendReceiptRef, SpendReservation, SpendState,
};
use symbiotic_trace::UsageTrace;
struct Fixture {
    ledger: sqlite::SqliteSpendLedger,
    after_reserve: Option<Box<dyn Fn() + Send + Sync>>,
}
pub fn ledger() -> Arc<dyn SpendLedger> {
    ledger_with_after_reserve(None)
}
pub fn ledger_with_after_reserve(
    after_reserve: Option<Box<dyn Fn() + Send + Sync>>,
) -> Arc<dyn SpendLedger> {
    Arc::new(Fixture {
        ledger: sqlite::SqliteSpendLedger::in_memory(),
        after_reserve,
    })
}
impl SpendLedger for Fixture {
    fn release_before_dispatch(&self, r: &SpendReceiptRef) -> Result<(), ModelError> {
        self.ledger.release_before_dispatch(r)
    }
    fn reserve(&self, r: &SpendReservation) -> Result<bool, ModelError> {
        let accepted = self.ledger.reserve(r)?;
        if accepted && let Some(hook) = &self.after_reserve {
            hook();
        }
        Ok(accepted)
    }
    fn reserve_explicit(&self, r: &SpendReservation, limit: u32) -> Result<bool, ModelError> {
        let accepted = self.ledger.reserve_explicit(r, limit)?;
        if accepted && let Some(hook) = &self.after_reserve {
            hook();
        }
        Ok(accepted)
    }
    fn discard_recovery(&self, a: &str, i: &str) -> Result<(), ModelError> {
        self.ledger.discard_recovery(a, i)
    }
    fn purge_recovery(
        &self,
        matches: &dyn Fn(&serde_json::Value) -> Result<bool, ModelError>,
    ) -> Result<usize, ModelError> {
        self.ledger.purge_recovery(matches)
    }
    fn acquire_handoff(
        &self,
        handoff: &symbiotic_model::AcceptedSpendHandoff,
        account: &str,
        input_identity: &str,
        owner: &str,
    ) -> Result<(), ModelError> {
        self.ledger
            .acquire_handoff(handoff, account, input_identity, owner)
    }
    fn receipt(&self, r: &SpendReceiptRef) -> Result<Option<SpendReceipt>, ModelError> {
        self.ledger.receipt(r)
    }
    fn invocation(&self, a: &str, i: &str) -> Result<Option<SpendReceipt>, ModelError> {
        self.ledger.invocation(a, i)
    }
    fn finish(
        &self,
        r: &SpendReceiptRef,
        s: SpendState,
        u: Option<UsageTrace>,
        o: Option<serde_json::Value>,
        invocation: Option<&str>,
    ) -> Result<(), ModelError> {
        self.ledger.finish(r, s, u, o, invocation)
    }
}
