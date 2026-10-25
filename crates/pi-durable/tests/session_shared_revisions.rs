#[allow(dead_code)]
#[path = "support/session_storage.rs"]
mod support;
use parking_lot::Mutex;
use pi_chord::{delta::Op, json::parse};
use pi_durable::*;
use std::sync::Arc;
use support::ControlledStorage;

#[tokio::test]
async fn storage_publication_and_snapshots_retain_native_revisions_and_operation_payloads() {
    let storage = Arc::new(ControlledStorage::default());
    let session = Session::new(storage.clone(), SessionOptions::default());
    let definition = Arc::new(
        AnyDocDefinition::new(
            "shared",
            1,
            DefinitionScope::Session,
            false,
            |_| {
                Ok(JsonObject::try_from_value(
                    parse(r#"{"items":[1,2],"other":{"label":"old"}}"#).unwrap(),
                )
                .unwrap())
            },
            |_, _, _| Ok(false),
        )
        .unwrap(),
    );
    let address = DocumentAddress {
        kind: "shared".into(),
        scope: DocumentScope::Session,
        key: None,
    };
    let (def, addr) = (definition.clone(), address.clone());
    let context = Context::default();
    session
        .commit(
            move |tx| async move {
                tx.doc_at(&def, addr, None).await?;
                Ok(())
            },
            &context,
        )
        .await
        .unwrap();
    let before = session
        .snapshot_raw(definition.clone(), address.clone(), &context)
        .await
        .unwrap()
        .unwrap();
    {
        let writes = storage.writes.lock();
        let StorageWrite::DocumentCreate { content, .. } = &writes.last().unwrap()[0] else {
            panic!("expected a base")
        };
        assert!(std::ptr::eq(
            content.value.value().as_object().unwrap(),
            before.as_object().unwrap()
        ));
    }
    let publications = Arc::new(Mutex::new(Vec::new()));
    let received = publications.clone();
    let _subscription = session
        .subscribe_commits(move |publication, _| {
            received.lock().push(publication.clone());
            Ok(())
        })
        .unwrap();
    let (def, addr) = (definition.clone(), address.clone());
    session
        .commit(
            move |tx| async move {
                tx.doc_at(&def, addr, None).await?.apply_ops(&[Op::Set(
                    vec!["other".into()],
                    parse(r#"{"label":"new"}"#).unwrap(),
                )])?;
                Ok(())
            },
            &context,
        )
        .await
        .unwrap();
    let after = session
        .snapshot_raw(definition, address, &context)
        .await
        .unwrap()
        .unwrap();
    let publication = publications.lock().last().unwrap().clone();
    let CommitChange::Document(DocumentCommitChange::Document { value, ops, .. }) =
        &publication.changes[0]
    else {
        panic!("expected a document")
    };
    {
        let writes = storage.writes.lock();
        let StorageWrite::DocumentChange {
            content: DocumentContent::Delta { ops: persisted, .. },
            ..
        } = &writes.last().unwrap()[0]
        else {
            panic!("expected a delta")
        };
        assert!(Arc::ptr_eq(value, &after));
        assert!(Arc::ptr_eq(persisted, ops));
        let Op::Set(_, payload) = &ops[0] else {
            panic!("expected a Set")
        };
        assert!(std::ptr::eq(
            payload.as_object().unwrap(),
            after["other"].as_object().unwrap()
        ));
        assert!(std::ptr::eq(
            before["items"].as_array().unwrap(),
            after["items"].as_array().unwrap()
        ));
        assert!(before["other"]["label"].string_equals("old"));
    }
    session.close(&context).await.unwrap();
}
