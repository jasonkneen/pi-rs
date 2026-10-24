use pi_chord::{JsonString, JsonValue, json::to_value, output::*};
fn fixture() -> JsonValue {
    serde_json::from_str(include_str!("fixtures/output.json")).unwrap()
}
fn limits(value: &JsonValue) -> OutputLimits {
    serde_json::from_str(&value.to_string()).unwrap()
}

#[test]
fn pinned_reference_bounds_sanitization_and_byte_counts() {
    for (index, row) in fixture()["bounded"].as_array().unwrap().iter().enumerate() {
        let text = row["text"].as_string().unwrap();
        assert_eq!(
            to_value(&bound_output(text, limits(&row["limits"]))).unwrap(),
            row["result"],
            "bounds {index}"
        );
        assert_eq!(
            JsonValue::String(sanitize_output(text)),
            row["sanitized"],
            "sanitize {index}"
        );
        assert_eq!(
            utf8_byte_length(text),
            row["byteLength"].as_u64().unwrap() as usize,
            "byte length {index}"
        );
    }
}

#[test]
fn pinned_reference_chunked_output_and_exact_dropped_totals() {
    for (index, row) in fixture()["streams"].as_array().unwrap().iter().enumerate() {
        let mut buffer = OutputBuffer::new(limits(&row["limits"]));
        for (step_index, step) in row["steps"].as_array().unwrap().iter().enumerate() {
            let skipped = if step["skipped"].is_null() {
                None
            } else {
                Some(serde_json::from_str(&step["skipped"].to_string()).unwrap())
            };
            let accepted = if step["kind"].string_equals("text") {
                buffer.push_text(step["data"].as_string().unwrap().clone(), skipped)
            } else {
                let bytes: Vec<_> = step["data"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8)
                    .collect();
                buffer.push_bytes(&bytes, skipped)
            }
            .unwrap();
            assert_eq!(
                accepted,
                step["accepted"].as_bool().unwrap(),
                "accepted stream {index} step {step_index}"
            );
            assert_eq!(
                to_value(&buffer.snapshot()).unwrap(),
                step["result"],
                "snapshot stream {index} step {step_index}"
            );
            assert_eq!(
                buffer.stored_bytes(),
                step["storedBytes"].as_u64().unwrap() as usize,
                "stored stream {index} step {step_index}"
            );
        }
        buffer.end();
        assert_eq!(
            to_value(&buffer.snapshot()).unwrap(),
            row["result"],
            "final snapshot stream {index}"
        );
        assert_eq!(
            buffer.stored_bytes(),
            row["storedBytes"].as_u64().unwrap() as usize,
            "final stored stream {index}"
        );
    }
}

#[test]
fn skipped_output_requires_tail_retention() {
    let mut buffer = OutputBuffer::new(OutputLimits {
        max_bytes: 100,
        max_lines: 10,
        retain: Retain::Head,
    });
    assert_eq!(
        buffer
            .push_text(
                "text",
                Some(OutputSkip {
                    bytes: 1000,
                    newlines: 100,
                    ends_with_newline: true
                })
            )
            .unwrap_err(),
        "Skipped output requires tail retention"
    );
}

#[test]
fn tail_storage_stays_bounded_across_a_long_stream() {
    let mut buffer = OutputBuffer::new(OutputLimits {
        max_bytes: 1024,
        max_lines: 10,
        retain: Retain::Tail,
    });
    let chunk = JsonString::from("retained line of output\n");
    for _ in 0..10000 {
        buffer.push_text(chunk.clone(), None).unwrap();
        assert!(buffer.stored_bytes() <= 1024 + utf8_byte_length(&chunk) * 2);
    }
    let result = buffer.snapshot();
    assert_eq!(result.dropped_lines, 9990);
}
