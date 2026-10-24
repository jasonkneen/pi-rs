use super::queue::{SerialQueue, panic_error};
use super::*;
use futures::FutureExt;
use parking_lot::Mutex;
use rusqlite::{
    Connection,
    types::{Value as BindValue, ValueRef},
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
};

type Work = Box<dyn FnOnce(&mut Connection) + Send>;
enum Command {
    Work(Work),
    Close(tokio::sync::oneshot::Sender<SqliteResult<()>>),
}
#[derive(Clone)]
struct Worker {
    sender: mpsc::Sender<Command>,
}
impl Worker {
    fn call<T, F>(&self, operation: F) -> SqliteFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> SqliteResult<T> + Send + 'static,
    {
        let (send, receive) = tokio::sync::oneshot::channel();
        let command = Command::Work(Box::new(move |connection| {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(connection)))
                    .unwrap_or_else(|panic| Err(panic_error(panic)));
            let _ = send.send(result);
        }));
        let sent = self.sender.send(command);
        Box::pin(async move {
            sent.map_err(|_| SqliteError::new("database is not open"))?;
            receive
                .await
                .map_err(|_| SqliteError::new("SQLite worker ended"))?
        })
    }
    fn close(&self) -> SqliteFuture<()> {
        let (send, receive) = tokio::sync::oneshot::channel();
        let sent = self.sender.send(Command::Close(send));
        Box::pin(async move {
            sent.map_err(|_| SqliteError::new("database is not open"))?;
            receive
                .await
                .map_err(|_| SqliteError::new("SQLite worker ended"))?
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct NativeSqliteDatabaseOptions {
    pub wal_auto_checkpoint_pages: u32,
    pub busy_timeout_ms: u64,
}
impl Default for NativeSqliteDatabaseOptions {
    fn default() -> Self {
        Self {
            wal_auto_checkpoint_pages: 1000,
            busy_timeout_ms: 5000,
        }
    }
}
#[derive(Clone)]
pub struct NativeSqliteDatabase {
    worker: Worker,
    queue: Arc<SerialQueue>,
    closed: Arc<AtomicBool>,
}

pub async fn open_native_sqlite_database(
    path: impl AsRef<Path>,
    options: NativeSqliteDatabaseOptions,
) -> SqliteResult<NativeSqliteDatabase> {
    let path = path.as_ref().to_owned();
    let (sender, receiver) = mpsc::channel();
    let (opened, ready) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("pi-sqlite".into())
        .spawn(move || {
            let connection = (|| -> SqliteResult<Connection> {
                if path != Path::new(":memory:")
                    && let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
                {
                    std::fs::create_dir_all(parent).map_err(|e| SqliteError::new(e.to_string()))?;
                }
                let connection = Connection::open(path)?;
                connection
                    .busy_timeout(std::time::Duration::from_millis(options.busy_timeout_ms))?;
                connection.pragma_update(None, "journal_mode", "WAL")?;
                connection.pragma_update(None, "synchronous", "NORMAL")?;
                connection.pragma_update(
                    None,
                    "wal_autocheckpoint",
                    options.wal_auto_checkpoint_pages,
                )?;
                Ok(connection)
            })();
            let mut connection = match connection {
                Ok(connection) => {
                    let _ = opened.send(Ok(()));
                    connection
                }
                Err(error) => {
                    let _ = opened.send(Err(error));
                    return;
                }
            };
            while let Ok(command) = receiver.recv() {
                match command {
                    Command::Work(work) => work(&mut connection),
                    Command::Close(done) => {
                        connection.flush_prepared_statement_cache();
                        let checkpoint = connection
                            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                            .map_err(SqliteError::from);
                        let close = connection
                            .close()
                            .map_err(|(_, error)| SqliteError::from(error));
                        let _ = done.send(checkpoint.and(close));
                        return;
                    }
                }
            }
        })
        .map_err(|e| SqliteError::new(e.to_string()))?;
    ready
        .await
        .map_err(|_| SqliteError::new("SQLite worker ended while opening"))??;
    Ok(NativeSqliteDatabase {
        worker: Worker { sender },
        queue: Arc::default(),
        closed: Arc::new(AtomicBool::new(false)),
    })
}

fn bind(params: Vec<SqliteValue>) -> Vec<BindValue> {
    params
        .into_iter()
        .map(|value| match value {
            SqliteValue::Null => BindValue::Null,
            SqliteValue::Real(v) => BindValue::Real(v),
            SqliteValue::Integer(v) => BindValue::Integer(v),
            SqliteValue::Text(v) => BindValue::Text(v),
            SqliteValue::Blob(v) => BindValue::Blob(v),
        })
        .collect()
}
fn rows(
    connection: &mut Connection,
    sql: &str,
    params: Vec<SqliteValue>,
    first_only: bool,
) -> SqliteResult<Vec<SqliteRow>> {
    let mut statement = connection.prepare_cached(sql)?;
    let names: Vec<_> = statement.column_names().map(str::to_owned).collect();
    let params = bind(params);
    let mut cursor = statement.query(rusqlite::params_from_iter(params))?;
    let mut result = Vec::new();
    while let Some(row) = cursor.next()? {
        let mut value = SqliteRow::new();
        for (index, name) in names.iter().enumerate() {
            value.insert(
                name.clone(),
                match row.get_ref(index)? {
                    ValueRef::Null => SqliteValue::Null,
                    ValueRef::Integer(v) => SqliteValue::Integer(v),
                    ValueRef::Real(v) => SqliteValue::Real(v),
                    ValueRef::Text(v) => SqliteValue::Text(String::from_utf8_lossy(v).into_owned()),
                    ValueRef::Blob(v) => SqliteValue::Blob(v.to_vec()),
                },
            );
        }
        result.push(value);
        if first_only {
            break;
        }
    }
    Ok(result)
}
#[derive(Clone)]
struct Executor {
    worker: Worker,
    active: Option<Arc<Mutex<bool>>>,
}
impl Executor {
    fn call<T, F>(&self, operation: F) -> SqliteFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> SqliteResult<T> + Send + 'static,
    {
        // The lease check and worker submission form one operation. Commands
        // accepted before retirement must run before COMMIT, even if their
        // waiters are dropped. A later call must never join another transaction.
        let active = self.active.as_ref().map(|active| active.lock());
        if active.as_ref().is_some_and(|active| !**active) {
            return Box::pin(async {
                Err(SqliteError::new(
                    "SQLite transaction handle is no longer active",
                ))
            });
        }
        self.worker.call(operation)
    }
}
impl SqliteExecutor for Executor {
    fn exec(&self, sql: &str) -> SqliteFuture<()> {
        let sql = sql.to_owned();
        self.call(move |c| c.execute_batch(&sql).map_err(Into::into))
    }
    fn run(&self, sql: &str, params: &[SqliteValue]) -> SqliteFuture<()> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        self.call(move |c| {
            c.prepare_cached(&sql)?
                .execute(rusqlite::params_from_iter(bind(params)))?;
            Ok(())
        })
    }
    fn get(&self, sql: &str, params: &[SqliteValue]) -> SqliteFuture<Option<SqliteRow>> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        self.call(move |c| Ok(rows(c, &sql, params, true)?.into_iter().next()))
    }
    fn all(&self, sql: &str, params: &[SqliteValue]) -> SqliteFuture<Vec<SqliteRow>> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        self.call(move |c| rows(c, &sql, params, false))
    }
}
impl NativeSqliteDatabase {
    fn executor(&self) -> Executor {
        Executor {
            worker: self.worker.clone(),
            active: None,
        }
    }
}
impl SqliteExecutor for NativeSqliteDatabase {
    fn exec(&self, sql: &str) -> SqliteFuture<()> {
        let sql = sql.to_owned();
        let executor = self.executor();
        self.queue.submit(move || executor.exec(&sql))
    }
    fn run(&self, sql: &str, params: &[SqliteValue]) -> SqliteFuture<()> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        let executor = self.executor();
        self.queue.submit(move || executor.run(&sql, &params))
    }
    fn get(&self, sql: &str, params: &[SqliteValue]) -> SqliteFuture<Option<SqliteRow>> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        let executor = self.executor();
        self.queue.submit(move || executor.get(&sql, &params))
    }
    fn all(&self, sql: &str, params: &[SqliteValue]) -> SqliteFuture<Vec<SqliteRow>> {
        let sql = sql.to_owned();
        let params = params.to_vec();
        let executor = self.executor();
        self.queue.submit(move || executor.all(&sql, &params))
    }
}
struct Cleanup {
    worker: Worker,
    needs_rollback: Arc<AtomicBool>,
    active: Arc<Mutex<bool>>,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        *self.active.lock() = false;
        let needs_rollback = self.needs_rollback.clone();
        // This work is submitted even if BEGIN's waiter was dropped. The worker
        // checks the flag after BEGIN, so an interrupted begin cannot leak a transaction.
        drop(self.worker.call(move |connection| {
            if needs_rollback.swap(false, Ordering::AcqRel) {
                connection.execute_batch("ROLLBACK")?;
            }
            Ok(())
        }));
    }
}
impl SqliteDatabase for NativeSqliteDatabase {
    fn transaction_raw(
        &self,
        callback: SqliteTransactionCallback,
    ) -> SqliteFuture<SqliteTransactionResult> {
        let worker = self.worker.clone();
        self.queue.submit(move || {
            Box::pin(async move {
                let active = Arc::new(Mutex::new(true));
                let needs_rollback = Arc::new(AtomicBool::new(false));
                let cleanup = Cleanup {
                    worker: worker.clone(),
                    needs_rollback: needs_rollback.clone(),
                    active: active.clone(),
                };
                worker
                    .call({
                        let needs_rollback = needs_rollback.clone();
                        move |c| {
                            c.execute_batch("BEGIN IMMEDIATE")?;
                            needs_rollback.store(true, Ordering::Release);
                            Ok(())
                        }
                    })
                    .await?;
                let executor = Arc::new(Executor {
                    worker: worker.clone(),
                    active: Some(active.clone()),
                });
                let result = std::panic::AssertUnwindSafe(async move { callback(executor).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|panic| Err(panic_error(panic)));
                *active.lock() = false;
                let result = match result {
                    Ok(value) => match worker
                        .call(|c| c.execute_batch("COMMIT").map_err(Into::into))
                        .await
                    {
                        Ok(()) => Ok(value),
                        Err(error) => Err(error),
                    },
                    Err(error) => Err(error),
                };
                let result = if let Err(error) = result {
                    match worker
                        .call(|c| c.execute_batch("ROLLBACK").map_err(Into::into))
                        .await
                    {
                        Ok(()) => Err(error),
                        Err(rollback) => Err(SqliteError::rollback(error, rollback)),
                    }
                } else {
                    result
                };
                needs_rollback.store(false, Ordering::Release);
                drop(cleanup);
                result
            })
        })
    }
    fn close(&self) -> SqliteFuture<()> {
        let closed = self.closed.clone();
        let worker = self.worker.clone();
        self.queue.submit(move || {
            Box::pin(async move {
                if closed.swap(true, Ordering::AcqRel) {
                    return Ok(());
                }
                worker.close().await
            })
        })
    }
}
