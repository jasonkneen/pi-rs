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

#[tokio::test]
async fn checkpoint_retains_the_prepared_value_and_batch_for_both_storage_decisions() {
    for base in [false, true] {
        let storage = Arc::new(ControlledStorage::default());
        let session = Session::new(storage.clone(), SessionOptions::default());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let received = calls.clone();
        let definition = Arc::new(
            AnyDocDefinition::new_with_shared_checkpoint(
                "policy.shared",
                1,
                DefinitionScope::Session,
                false,
                |_| Ok(JsonObject::try_from_value(parse(r#"{"count":0}"#).unwrap()).unwrap()),
                move |value, ops, info| {
                    received
                        .lock()
                        .push((value.clone(), ops.clone(), info.deltas_since_base));
                    Ok(base)
                },
            )
            .unwrap(),
        );
        let address = DocumentAddress {
            kind: "policy.shared".into(),
            scope: DocumentScope::Session,
            key: None,
        };
        let context = Context::default();
        let (def, addr) = (definition.clone(), address.clone());
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
        assert!(
            calls.lock().is_empty(),
            "creation does not consult the policy"
        );
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
                    tx.doc_at(&def, addr, None)
                        .await?
                        .apply_ops(&[Op::Set(vec!["count".into()], 1.into())])?;
                    Ok(())
                },
                &context,
            )
            .await
            .unwrap();
        let snapshot = session
            .snapshot_raw(definition, address, &context)
            .await
            .unwrap()
            .unwrap();
        {
            let calls = calls.lock();
            assert_eq!(calls.len(), 1);
            let (value, ops, count) = &calls[0];
            assert_eq!(*count, 0);
            assert!(std::ptr::eq(
                value.value().as_object().unwrap(),
                snapshot.as_object().unwrap()
            ));
            let publications = publications.lock();
            let CommitChange::Document(DocumentCommitChange::Document {
                value: published,
                ops: published_ops,
                ..
            }) = &publications[0].changes[0]
            else {
                panic!("expected a document publication")
            };
            assert!(Arc::ptr_eq(&snapshot, published));
            assert!(Arc::ptr_eq(ops, published_ops));
            let writes = storage.writes.lock();
            let StorageWrite::DocumentChange { content, .. } = &writes.last().unwrap()[0] else {
                panic!("expected a document change")
            };
            match (base, content) {
                (false, DocumentContent::Delta { ops: stored, .. }) => {
                    assert!(Arc::ptr_eq(ops, stored));
                }
                (true, DocumentContent::Base { value: stored, .. }) => {
                    assert!(std::ptr::eq(
                        value.value().as_object().unwrap(),
                        stored.value().as_object().unwrap()
                    ));
                }
                _ => panic!("checkpoint decision was not respected"),
            }
        }
        session.close(&context).await.unwrap();
    }
}
