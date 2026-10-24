use pi_durable::*;

fn round_trip<T: serde::Serialize + serde::de::DeserializeOwned>(source: &str) {
    let expected: JsonValue = serde_json::from_str(source).unwrap();
    let decoded: T = serde_json::from_str(source).unwrap();
    let encoded = pi_chord::json::to_value(&decoded).unwrap();
    assert_eq!(encoded, expected);
}
#[test]
fn arbitrary_utf16_and_explicit_null_survive_nested_record_discriminants() {
    round_trip::<StorageWrite>(
        r#"{"type":"task","value":{"id":2,"conversationId":1,"kind":"\ud800","version":1,"input":{"\ud801":"\udfff"},"state":{"status":"waiting","checkpoint":{"phase":"next","text":"\ud800"},"on":[3],"policy":"allSettled"},"memos":{"\ud800":null},"background":false,"abortRequested":false}}"#,
    );
    round_trip::<StorageWrite>(
        r#"{"type":"task","value":{"id":2,"conversationId":1,"kind":"task","version":1,"input":null,"state":{"status":"terminal","outcome":{"status":"failed","error":{"message":"\ud800","detail":null},"result":null}},"background":false,"abortRequested":false}}"#,
    );
    round_trip::<StorageWrite>(
        r#"{"type":"entry","value":{"id":3,"conversationId":1,"kind":"\ud800","data":null,"model":[{"role":"user","content":"\udfff","timestamp":0}],"edits":[{"target":2,"action":"replace","messages":[{"role":"user","content":"\ud800","timestamp":0}]}]}}"#,
    );
    round_trip::<StorageWrite>(
        r#"{"type":"submission","value":{"id":4,"conversationId":1,"requestId":"\ud800","type":"input","status":"unanswered","reason":"\udfff","detail":null}}"#,
    );
    round_trip::<StorageWrite>(
        r#"{"type":"document.create","record":{"id":5,"kind":"\ud800","key":"\udfff","scope":{"kind":"session"}},"content":{"kind":"base","version":1,"value":{"\ud800":"\udfff"}}}"#,
    );
    round_trip::<StorageWrite>(
        r#"{"type":"document.change","id":5,"content":{"kind":"delta","version":1,"ops":[["s",["\ud800"],"\udfff"]]}}"#,
    );
}

#[test]
fn completed_tasks_and_passive_submissions_reject_invalid_lifecycles() {
    assert!(
        serde_json::from_str::<SubmissionRecord>(
            r#"{"id":2,"conversationId":1,"type":"write","status":"placed","entry":3}"#
        )
        .is_err()
    );
    assert!(serde_json::from_str::<TaskRecord>(r#"{"id":2,"conversationId":1,"kind":"task","version":1,"input":null,"state":{"status":"terminal","outcome":{"status":"completed","result":null}},"memos":{},"background":false,"abortRequested":false}"#).is_err());
    assert!(serde_json::from_str::<StorageWrite>(r#"{"type":"document.create","record":{"id":2,"kind":"a","scope":{"kind":"session"}},"content":{"kind":"delta","version":1,"ops":[]}}"#).is_err());
    assert!(serde_json::from_str::<DocumentCreate>(r#"{"id":2,"kind":"a","scope":{"kind":"conversation","conversationId":1},"history":"latest","fork":"asOf"}"#).is_err());
}

#[test]
fn ids_accept_json_number_spellings_and_reject_non_safe_numbers() {
    assert_eq!(
        serde_json::from_str::<EntryId>("1.0").unwrap(),
        EntryId::from_raw(1)
    );
    for invalid in [
        "1.5",
        "9007199254740992",
        "-9007199254740992",
        "\"1\"",
        "null",
    ] {
        assert!(
            serde_json::from_str::<EntryId>(invalid).is_err(),
            "{invalid}"
        );
    }
}

#[test]
fn entry_tokens_narrow_by_kind_without_changing_data() {
    assert!(define_entry::<JsonValue>("").is_err());
    let entry: EntryRecord =
        serde_json::from_str(r#"{"id":2,"conversationId":1,"kind":"pi.user","data":null}"#)
            .unwrap();
    assert!(USER_ENTRY.is(Some(&entry)));
    assert!(!ASSISTANT_ENTRY.is(Some(&entry)));
    assert!(!USER_ENTRY.is(None));
    assert_eq!(entry.data, Some(JsonValue::Null));
}

#[test]
fn nested_storage_errors_keep_their_reference_names_and_first_cause() {
    let error = Error::from(pi_chord::delta::Error::UnsafePath("__proto__".into()));
    assert_eq!(error.name(), "UnsafePathError");
    let failed = Error::failed(error.clone());
    assert_eq!(failed.name(), "SessionFailed");
    assert!(matches!(failed.kind(),ErrorKind::SessionFailed {cause} if cause.same_error(&error)));
    assert_eq!(Error::read_after_write("entry").name(), "ReadAfterWrite");
    assert_eq!(Error::request("bad cursor").name(), "StorageRequestError");
}
