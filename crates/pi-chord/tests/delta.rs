use pi_chord::delta::*;
use pi_chord::json::{Value, to_value};
macro_rules! json { ($($tokens:tt)+) => { Value::from(serde_json::json!($($tokens)+)) }; }

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/delta.json")).unwrap()
}
fn ops(value: &Value) -> Vec<Op> {
    serde_json::from_str(&value.to_string()).unwrap()
}

#[test]
fn pinned_reference_stream_encodes_decodes_and_replays() {
    let fixture = fixture();
    let mut encoder = Encoder::new();
    let mut decoder = Decoder::new();
    let mut state = Value::Null;
    for (index, row) in fixture["streams"].as_array().unwrap().iter().enumerate() {
        let batch = ops(&row["ops"]);
        let wire = encoder.encode(&batch).unwrap();
        assert_eq!(
            to_value(&wire).unwrap(),
            row["wire"],
            "encode batch {index}"
        );
        let decoded = decoder.decode(row["wire"].as_array().unwrap()).unwrap();
        assert_eq!(decoded, batch, "decode batch {index}");
        state = apply(state, &decoded).unwrap();
        assert_eq!(state, row["value"], "apply batch {index}");
    }
}

#[test]
fn pinned_reference_application_and_input_ownership() {
    for row in fixture()["application"].as_array().unwrap() {
        let initial = row["initial"].clone();
        let operations = ops(&row["ops"]);
        assert_eq!(
            apply_immutable(Some(&initial), &operations).unwrap(),
            row["value"]
        );
        assert_eq!(initial, row["initial"]);
        assert_eq!(to_value(&operations).unwrap(), row["ops"]);
    }
}

#[test]
fn pinned_reference_validation_errors() {
    let fixture = fixture();
    for row in fixture["errors"].as_array().unwrap() {
        assert_eq!(
            Op::from_value(&row["op"]).unwrap_err().to_string(),
            row["message"].to_utf8().unwrap(),
            "{}",
            row["op"]
        );
    }
    for row in fixture["wireErrors"].as_array().unwrap() {
        assert_eq!(
            Decoder::new()
                .decode(row["wire"].as_array().unwrap())
                .unwrap_err()
                .to_string(),
            row["message"].to_utf8().unwrap(),
            "{}",
            row["wire"]
        );
    }
    for row in fixture["applicationErrors"].as_array().unwrap() {
        assert_eq!(
            apply(row["initial"].clone(), &ops(&row["ops"]))
                .unwrap_err()
                .to_string(),
            row["message"].to_utf8().unwrap(),
            "{}",
            row["ops"]
        );
    }
}

#[test]
fn pinned_reference_overlap_uses_utf16_units_and_candidate_budget() {
    for row in fixture()["overlaps"].as_array().unwrap() {
        let a = row["args"].as_array().unwrap();
        assert_eq!(
            overlap(
                &a[0].to_utf8().unwrap(),
                &a[1].to_utf8().unwrap(),
                a[2].as_u64().unwrap() as usize,
                a[3].as_u64().unwrap() as usize,
                a[4].as_u64().unwrap() as usize
            ),
            row["value"].as_u64().unwrap() as usize
        );
    }
}

#[test]
fn base_resets_dictionaries_and_each_batch_needs_an_explicit_first_path() {
    let mut enc = Encoder::new();
    let mut dec = Decoder::new();
    for i in 0..3 {
        let batch = ops(&json!([["s", ["a"], i]]));
        let wire = enc.encode(&batch).unwrap();
        assert_eq!(dec.decode(&wire).unwrap(), batch);
    }
    dec.decode(&enc.encode(&[Op::Replace(json!({"a":0}))]).unwrap())
        .unwrap();
    assert!(dec.decode(&[json!(["s", 0, 3])]).is_err());
    assert!(dec.decode(&[json!(["s", 3])]).is_err());
    assert_eq!(
        enc.encode(&ops(&json!([["s", ["a"], 4]]))).unwrap(),
        vec![json!(["s", ["a"], 4])]
    );
}

#[test]
fn immutable_replacement_payload_and_batched_replay_are_independent() {
    let first = ops(&json!([["r",{"nested":{"value":1}}]]));
    let second = ops(&json!([["s", ["nested", "value"], 2]]));
    let value = apply_immutable_batches(None, [first.as_slice(), second.as_slice()]).unwrap();
    assert_eq!(value, json!({"nested":{"value":2}}));
    assert_eq!(first[0], Op::Replace(json!({"nested":{"value":1}})));
}

#[test]
fn arrays_remain_dense_and_large_splices_do_not_overflow_the_stack() {
    let items = vec![Value::Null; 300_000];
    let result = apply(json!([]), &[Op::Splice(vec![], 0, 0, items)]).unwrap();
    assert_eq!(result.as_array().unwrap().len(), 300_000);
}

#[test]
fn decoded_vocabulary_rejects_wire_only_forms() {
    for value in [
        json!(["s", 1]),
        json!(["d"]),
        json!(["a", "x"]),
        json!(["t", 2]),
        json!(["p", 0, 0, []]),
        json!(["#", 0, ["v"]]),
        json!(["s", 0, 1]),
    ] {
        assert!(Op::from_value(&value).is_err());
        assert!(assert_valid_wire_op(&value).is_ok());
    }
}
