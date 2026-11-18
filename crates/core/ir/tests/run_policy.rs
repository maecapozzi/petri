use std::time::Duration;

use ir::RunPolicy;
use serde_json::json;

#[test]
fn old_serialized_policies_do_not_acquire_new_timeout_budgets() {
    let policy: RunPolicy = serde_json::from_value(json!({})).expect("old empty policy");
    assert_eq!(policy.stall_timeout, None);
    assert_eq!(policy.max_active_duration, None);

    let old_stall_only: RunPolicy =
        serde_json::from_value(json!({ "stall_timeout": { "secs": 30, "nanos": 0 } }))
            .expect("old stall-only policy");
    assert_eq!(old_stall_only.stall_timeout, Some(Duration::from_secs(30)));
    assert_eq!(old_stall_only.max_active_duration, None);
}

#[test]
fn disabled_and_explicit_budgets_remain_distinct_on_the_wire() {
    let disabled = serde_json::to_value(RunPolicy::default()).expect("disabled policy");
    assert_eq!(disabled, json!({}));

    let explicit = RunPolicy {
        stall_timeout: Some(Duration::from_secs(10 * 60)),
        max_active_duration: Some(Duration::from_secs(90 * 60)),
        ..RunPolicy::default()
    };
    let encoded = serde_json::to_value(explicit).expect("explicit policy");
    assert!(encoded.get("stall_timeout").is_some(), "{encoded}");
    assert!(encoded.get("max_active_duration").is_some(), "{encoded}");
}
