use polym_rust_demo::{Reply, apply_exchange_response};
use serde_json::json;

fn reply() -> Reply {
    serde_json::from_value(json!({"id":"live-test","state":"prepared","reason":"","order_hash":"0xabc","policy_ms":null,"post_ms":null,"finalize_ms":null,"source_to_dispatch_ms":null,"queue_ms":0,"sign_ms":0,"journal_ms":0,"dispatch_ms":null,"total_ms":0})).unwrap()
}
#[test]
fn live_acknowledgements_preserve_uncertainty_and_do_not_invent_fills() {
    let mut r = reply();
    apply_exchange_response(
        &mut r,
        200,
        &json!({"success":true,"orderID":"0xabc","status":"delayed"}),
    );
    assert_eq!(r.state, "accepted");
    assert_eq!(r.exchange_status.as_deref(), Some("delayed"));
    apply_exchange_response(
        &mut r,
        400,
        &json!({"success":false,"errorMsg":"no orders found to match","secret":"must not persist"}),
    );
    assert_eq!(r.state, "rejected");
    assert!(r.clob_response.as_ref().unwrap().get("secret").is_none());
    apply_exchange_response(
        &mut r,
        400,
        &json!({"error":"invalid maker amount precision"}),
    );
    assert_eq!(
        r.clob_response.as_ref().unwrap()["errorMsg"],
        "invalid maker amount precision"
    );
    assert!(r.reason.contains("invalid maker amount precision"));
    apply_exchange_response(
        &mut r,
        200,
        &json!({"success":true,"orderID":"0xwrong","status":"matched"}),
    );
    assert_eq!(r.state, "unknown");
    apply_exchange_response(
        &mut r,
        400,
        &json!({"success":true,"orderID":"0xabc","status":"matched"}),
    );
    assert_eq!(r.state, "unknown");
    apply_exchange_response(&mut r, 503, &json!({}));
    assert_eq!(r.state, "unknown");
}

#[test]
fn no_match_classification_requires_matching_hash_and_uncontradicted_evidence() {
    let no_match = json!({"orderID":"0xabc","errorMsg":"no orders found to match with FAK order. FAK orders are partially filled or killed if no match is found."});
    for status in [200, 400, 422] {
        let mut r = reply();
        apply_exchange_response(&mut r, status, &no_match);
        assert_eq!(r.state, "rejected");
    }
    for (status, update) in [
        (503, json!({})),
        (400, json!({"orderID":"0xwrong"})),
        (400, json!({"success":true})),
        (200, json!({"success":true,"status":"matched"})),
        (400, json!({"takingAmount":"5"})),
        (400, json!({"makingAmount":"4.95"})),
        (400, json!({"tradeIDs":["trade"]})),
        (400, json!({"transactionsHashes":["0xtransaction"]})),
        (400, json!({"errorMsg":"upstream internal error"})),
    ] {
        let mut r = reply();
        let mut body = no_match.clone();
        body.as_object_mut()
            .unwrap()
            .extend(update.as_object().unwrap().clone());
        apply_exchange_response(&mut r, status, &body);
        assert_eq!(r.state, "unknown", "status={status} body={body}");
    }
}
