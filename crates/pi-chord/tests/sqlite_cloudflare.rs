use parking_lot::Mutex;
use pi_chord::sqlite::{cloudflare::*, *};
use rusqlite::{
    Connection,
    types::{Value, ValueRef},
};
use std::{collections::VecDeque, sync::Arc};

struct Host {
    connection: Arc<Mutex<Connection>>,
    operations: Arc<Mutex<Vec<&'static str>>>,
}
impl Host {
    fn new() -> Self {
        Self {
            connection: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            operations: Arc::default(),
        }
    }
}
struct Cursor {
    rows: VecDeque<DurableObjectSqlRow>,
    operations: Arc<Mutex<Vec<&'static str>>>,
}
impl DurableObjectSqlCursor for Cursor {
    fn next(&mut self) -> SqliteResult<Option<DurableObjectSqlRow>> {
        self.operations.lock().push("next");
        Ok(self.rows.pop_front())
    }
    fn into_array(self: Box<Self>) -> SqliteResult<Vec<DurableObjectSqlRow>> {
        self.operations.lock().push("toArray");
        Ok(self.rows.into())
    }
}
impl DurableObjectSqliteHost for Host {
    fn exec(
        &self,
        sql: &str,
        bindings: &[DurableObjectSqlValue],
    ) -> SqliteResult<Box<dyn DurableObjectSqlCursor>> {
        let connection = self.connection.lock();
        let mut result = VecDeque::new();
        if bindings.is_empty() && sql.trim().trim_end_matches(';').contains(';') {
            connection.execute_batch(sql)?;
        } else {
            let mut statement = connection.prepare(sql)?;
            let bindings: Vec<_> = bindings
                .iter()
                .map(|value| match value {
                    DurableObjectSqlValue::Null => Value::Null,
                    DurableObjectSqlValue::Number(n) => Value::Real(*n),
                    DurableObjectSqlValue::Text(s) => Value::Text(s.clone()),
                    DurableObjectSqlValue::Blob(b) => Value::Blob(b.clone()),
                })
                .collect();
            if statement.column_count() == 0 {
                statement.execute(rusqlite::params_from_iter(bindings))?;
            } else {
                let names: Vec<_> = statement.column_names().map(str::to_owned).collect();
                let mut cursor = statement.query(rusqlite::params_from_iter(bindings))?;
                while let Some(row) = cursor.next()? {
                    let mut value = DurableObjectSqlRow::new();
                    for (i, name) in names.iter().enumerate() {
                        value.insert(
                            name.clone(),
                            match row.get_ref(i)? {
                                ValueRef::Null => DurableObjectSqlValue::Null,
                                ValueRef::Integer(n) => DurableObjectSqlValue::Number(n as f64),
                                ValueRef::Real(n) => DurableObjectSqlValue::Number(n),
                                ValueRef::Text(s) => DurableObjectSqlValue::Text(
                                    String::from_utf8_lossy(s).into_owned(),
                                ),
                                ValueRef::Blob(b) => DurableObjectSqlValue::Blob(b.to_vec()),
                            },
                        );
                    }
                    result.push_back(value);
                }
            }
        }
        Ok(Box::new(Cursor {
            rows: result,
            operations: self.operations.clone(),
        }))
    }
    fn transaction_raw(
        &self,
        callback: DurableObjectTransactionCallback,
    ) -> SqliteFuture<SqliteTransactionResult> {
        let connection = self.connection.clone();
        Box::pin(async move {
            connection.lock().execute_batch("BEGIN")?;
            match callback().await {
                Ok(value) => {
                    connection.lock().execute_batch("COMMIT")?;
                    Ok(value)
                }
                Err(error) => match connection.lock().execute_batch("ROLLBACK") {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(SqliteError::rollback(error, rollback.into())),
                },
            }
        })
    }
}
fn database() -> (
    Arc<Host>,
    Arc<DurableObjectSqliteStorage>,
    DurableObjectSqliteDatabase,
) {
    let host = Arc::new(Host::new());
    let storage = Arc::new(DurableObjectSqliteStorage::new(host.clone()));
    let db = DurableObjectSqliteDatabase::new(storage.clone());
    (host, storage, db)
}

#[tokio::test]
async fn blob_and_bigint_bindings_round_trip_and_get_uses_only_next() {
    let (host, _, db) = database();
    db.exec("CREATE TABLE blobs (id INTEGER PRIMARY KEY,data BLOB,n INTEGER)")
        .await
        .unwrap();
    db.run(
        "INSERT INTO blobs VALUES (?,?,?)",
        &[1.into(), SqliteValue::Blob(vec![1, 2, 3]), 7.into()],
    )
    .await
    .unwrap();
    db.run(
        "INSERT INTO blobs VALUES (?,?,?)",
        &[2.into(), SqliteValue::Blob(vec![4, 5]), 8.into()],
    )
    .await
    .unwrap();
    let row = db
        .get("SELECT data,n FROM blobs ORDER BY id", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row["data"], SqliteValue::Blob(vec![1, 2, 3]));
    assert_eq!(row["n"], SqliteValue::Real(7.0));
    assert_eq!(&*host.operations.lock(), &["next"]);
    assert_eq!(db.all("SELECT n FROM blobs", &[]).await.unwrap().len(), 2);
    assert_eq!(&*host.operations.lock(), &["next", "toArray"]);
    assert!(
        db.get("SELECT n FROM blobs WHERE id=9", &[])
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn unsafe_integer_binding_is_rejected_without_running_sql() {
    let (_, _, db) = database();
    db.exec("CREATE TABLE numbers (n INTEGER)").await.unwrap();
    for n in [9_007_199_254_740_993, -9_007_199_254_740_993] {
        assert_eq!(
            db.run("INSERT INTO numbers VALUES (?)", &[n.into()])
                .await
                .unwrap_err()
                .to_string(),
            format!("Durable Object SQL cannot bind {n} without losing precision")
        );
    }
    assert!(
        db.all("SELECT n FROM numbers", &[])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn idle_statement_runs_before_its_future_is_returned_and_close_preserves_host() {
    let (host, _, db) = database();
    db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let written = db.run("INSERT INTO items VALUES (?)", &[1.into()]);
    assert_eq!(
        host.connection
            .lock()
            .query_row("SELECT id FROM items", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        1
    );
    written.await.unwrap();
    db.close().await.unwrap();
    db.close().await.unwrap();
    assert!(db.get("SELECT id FROM items", &[]).await.unwrap().is_some());
}

#[tokio::test]
async fn rejected_transaction_rolls_back_and_retires_all_handle_operations() {
    let (_, _, db) = database();
    db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let (send, receive) = tokio::sync::oneshot::channel();
    let error = SqliteError::new("abort");
    let result = db
        .transaction::<(), _, _>({
            let error = error.clone();
            move |tx| async move {
                send.send(tx.clone()).ok().unwrap();
                tx.run("INSERT INTO items VALUES (?)", &[1.into()]).await?;
                Err(error)
            }
        })
        .await
        .unwrap_err();
    assert!(result.same_error(&error));
    assert!(
        db.all("SELECT id FROM items", &[])
            .await
            .unwrap()
            .is_empty()
    );
    let handle = receive.await.unwrap();
    assert!(
        handle
            .exec("SELECT 1")
            .await
            .unwrap_err()
            .to_string()
            .contains("no longer active")
    );
    assert!(handle.run("SELECT ?", &[1.into()]).await.is_err());
    assert!(handle.get("SELECT 1", &[]).await.is_err());
    assert!(handle.all("SELECT 1", &[]).await.is_err());
}

#[tokio::test]
async fn adapters_share_the_storage_queue_and_close_waits_for_transactions() {
    let (_, storage, db) = database();
    let other = DurableObjectSqliteDatabase::new(storage);
    db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let transaction = db.transaction::<(), _, _>(move |tx| async move {
        tx.run("INSERT INTO items VALUES (?)", &[1.into()]).await?;
        started.send(()).unwrap();
        gate.await.unwrap();
        Err(SqliteError::new("abort"))
    });
    entered.await.unwrap();
    let outside = other.run("INSERT INTO items VALUES (?)", &[2.into()]);
    let read = other.all("SELECT id FROM items", &[]);
    let closed = tokio::spawn(db.close());
    tokio::task::yield_now().await;
    assert!(!closed.is_finished());
    release.send(()).unwrap();
    assert!(transaction.await.is_err());
    outside.await.unwrap();
    let rows = read.await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], SqliteValue::Real(2.0));
    closed.await.unwrap().unwrap();
}

#[tokio::test]
async fn direct_storage_calls_participate_in_the_active_host_transaction() {
    let (host, storage, db) = database();
    db.exec("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let transaction = db.transaction::<(), _, _>(move |tx| async move {
        tx.run("INSERT INTO items VALUES (?)", &[1.into()]).await?;
        started.send(()).unwrap();
        gate.await.unwrap();
        Err(SqliteError::new("abort"))
    });
    entered.await.unwrap();
    storage
        .exec(
            "INSERT INTO items VALUES (?)",
            &[DurableObjectSqlValue::Number(2.0)],
        )
        .unwrap();
    release.send(()).unwrap();
    assert!(transaction.await.is_err());
    assert_eq!(
        host.connection
            .lock()
            .query_row("SELECT count(*) FROM items", [], |row| row.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn portable_migrations_run_through_host_adapter() {
    let (_, _, db) = database();
    let migrations = [SqliteMigration {
        version: 1,
        statements: vec![
            "CREATE TABLE items (id INTEGER)".into(),
            "INSERT INTO items VALUES (7)".into(),
        ],
    }];
    apply_sqlite_migrations(&db, &migrations, "app_schema")
        .await
        .unwrap();
    apply_sqlite_migrations(&db, &migrations, "app_schema")
        .await
        .unwrap();
    assert_eq!(
        db.get("SELECT id FROM items", &[]).await.unwrap().unwrap()["id"],
        SqliteValue::Real(7.0)
    );
}

#[tokio::test]
async fn a_long_backlog_after_a_transaction_does_not_recurse_on_the_stack() {
    let (_, _, db) = database();
    db.exec("CREATE TABLE items (id INTEGER)").await.unwrap();
    let (release, gate) = tokio::sync::oneshot::channel();
    let transaction = db.transaction(move |_| async move {
        gate.await.unwrap();
        Ok(())
    });
    let writes: Vec<_> = (0..10000)
        .map(|id| db.run("INSERT INTO items VALUES (?)", &[id.into()]))
        .collect();
    release.send(()).unwrap();
    transaction.await.unwrap();
    for write in writes {
        write.await.unwrap();
    }
    assert_eq!(
        db.get("SELECT count(*) AS n FROM items", &[])
            .await
            .unwrap()
            .unwrap()["n"],
        SqliteValue::Real(10000.0)
    );
}
