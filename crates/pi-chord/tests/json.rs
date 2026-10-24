use pi_chord::delta::{Decoder, Encoder, Op, apply};
use pi_chord::json::{parse, stringify, to_value};
use pi_chord::{JsonString, JsonValue};

#[test]
fn lone_surrogates_survive_parsing_serialization_and_nested_typed_records() {
    let source = r#"{"\ud800":"\udfff","pair":"\ud834\udd1e","mixed":["a\ud800b","\udfff\ud800"]}"#;
    let value = parse(source).unwrap();
    assert_eq!(parse(&stringify(&value)).unwrap(), value);
    assert_eq!(
        serde_json::from_str::<JsonValue>(&serde_json::to_string(&value).unwrap()).unwrap(),
        value
    );
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Record {
        data: JsonValue,
    }
    let record = Record {
        data: value.clone(),
    };
    let raw = serde_json::to_string(&record).unwrap();
    assert_eq!(serde_json::from_str::<Record>(&raw).unwrap().data, value);
    assert_eq!(
        value["pair"].as_string().unwrap().units(),
        &[0xd834, 0xdd1e]
    );
}

#[test]
fn truncation_can_split_and_append_can_rejoin_a_surrogate_pair() {
    let initial = parse(r#"{"text":"\ud834\udd1e"}"#).unwrap();
    let truncate = Op::Truncate(vec!["text".into()], 1);
    let split = apply(initial, &[truncate]).unwrap();
    assert_eq!(stringify(&split), r#"{"text":"\udd1e"}"#);
    let prefix = parse(r#"{"text":"\ud834"}"#).unwrap();
    let append = Op::Append(vec!["text".into()], JsonString::from_utf16(vec![0xdd1e]));
    let joined = apply(prefix, &[append]).unwrap();
    assert_eq!(
        joined["text"].as_string().unwrap().units(),
        &[0xd834, 0xdd1e]
    );
    assert_eq!(parse(&stringify(&joined)).unwrap(), joined);
}

#[test]
fn surrogate_paths_and_payloads_round_trip_through_both_vocabularies() {
    let value = parse(r#"["s",["\ud800"],"\udfff"]"#).unwrap();
    let op = Op::from_value(&value).unwrap();
    assert_eq!(op.to_value(), value);
    let batch = vec![op];
    let mut enc = Encoder::new();
    let mut dec = Decoder::new();
    for _ in 0..3 {
        let wire = enc.encode(&batch).unwrap();
        let detached: Vec<JsonValue> =
            serde_json::from_str(&serde_json::to_string(&wire).unwrap()).unwrap();
        assert_eq!(dec.decode(&detached).unwrap(), batch);
    }
    let target = apply(parse("{}").unwrap(), &batch).unwrap();
    assert_eq!(stringify(&target), r#"{"\ud800":"\udfff"}"#);
    assert_eq!(to_value(&batch).unwrap()[0], value);
}

#[test]
fn stringify_matches_ecmascript_keys_numbers_and_escaping() {
    let value = parse(r#"{"z":-0.0,"2":2,"01":1,"0":0,"4294967295":5,"1":1,"a":"\u0000\b\f\t\r\n\\\"/","n":1e+21}"#).unwrap();
    assert_eq!(
        stringify(&value),
        "{\"0\":0,\"1\":1,\"2\":2,\"z\":0,\"01\":1,\"4294967295\":5,\"a\":\"\\u0000\\b\\f\\t\\r\\n\\\\\\\"/\",\"n\":1e+21}"
    );
}

#[test]
fn parser_rejects_malformed_json_instead_of_silently_changing_it() {
    for source in [
        "",
        "nul",
        "null null",
        "[1,]",
        "{\"a\":1,}",
        "{a:1}",
        "01",
        "1.",
        "1e",
        "--1",
        "NaN",
        "Infinity",
        "\u{b}null",
        "\"\n\"",
        "\"\\uZZZZ\"",
        "\"\\u123\"",
        "\"\\x20\"",
    ] {
        assert!(parse(source).is_err(), "accepted {source:?}");
    }
}

#[test]
fn numbers_have_javascript_equality_across_json_spellings() {
    assert_eq!(parse("1").unwrap(), parse("1.0").unwrap());
    assert_eq!(parse("-0").unwrap(), parse("0.0").unwrap());
    assert_eq!(
        parse("9007199254740993").unwrap(),
        parse("9007199254740992").unwrap()
    );
}
