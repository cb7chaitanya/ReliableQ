//! Idempotency key derivation for the bundled charge side effect (spec
//! sec. 9.2). Deterministic and job-scoped — not attempt-scoped — so
//! every re-execution of the same job (after a lease reclaim, or an
//! explicit dead-job retry reusing the same job ID) sends the exact
//! same key, letting the charge service recognize and replay it
//! instead of creating a second charge.

use uuid::Uuid;

pub fn charge_idempotency_key(job_id: Uuid) -> String {
    format!("reliableq:charge:{job_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_is_deterministic_for_the_same_job_id() {
        let id = Uuid::new_v4();
        assert_eq!(charge_idempotency_key(id), charge_idempotency_key(id));
    }

    #[test]
    fn key_differs_across_job_ids() {
        assert_ne!(
            charge_idempotency_key(Uuid::new_v4()),
            charge_idempotency_key(Uuid::new_v4())
        );
    }

    #[test]
    fn key_has_the_documented_shape() {
        let id = Uuid::new_v4();
        assert_eq!(charge_idempotency_key(id), format!("reliableq:charge:{id}"));
    }
}
