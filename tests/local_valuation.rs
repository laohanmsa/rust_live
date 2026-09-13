use polym_rust_demo::shadow_valuation::{M5Input, expected_payout};
use serde::Deserialize;

#[derive(Deserialize)]
struct Vector {
    input: M5Input,
    expected: String,
}

#[test]
fn local_m5_matches_django_on_observed_orders_and_time_boundaries() {
    let vectors: Vec<Vector> = serde_json::from_str(include_str!("m5_vectors.json")).unwrap();
    for vector in vectors {
        let actual = expected_payout(&vector.input).unwrap();
        let expected: rust_decimal::Decimal = vector.expected.parse().unwrap();
        assert!(
            (actual - expected).abs() < "0.000000000001".parse().unwrap(),
            "actual={actual} expected={expected}"
        );
    }
}
