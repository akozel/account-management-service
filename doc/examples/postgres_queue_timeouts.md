# PostgreSQL queue-operation timeouts

Illustrative Rust/SQLx 0.8.6 settlement example. The production
[`PostgresOutboxQueue`](../../cqrs-runtime/src/postgres/outbox_queue.rs) now
applies configurable transaction-local `statement_timeout` to claim, recovery,
and settlement. The example's additional `lock_timeout` is optional and is not
a separate service setting: `statement_timeout` already bounds lock waiting.
The mandatory [runtime deadline](../task_processing_contract.md#runtime-queue-operation-deadlines)
is enforced separately by the coordinator.

`SET LOCAL` restricts each timeout setting to this transaction. After commit
or rollback, the connection's previous settings apply again, making this
suitable for a pooled connection. See [PostgreSQL SET](https://www.postgresql.org/docs/18/sql-set.html).

```rust
use sqlx::PgPool;
use uuid::Uuid;

pub async fn complete_task(
    pool: &PgPool,
    task_id: Uuid,
    lock_token: Uuid,
) -> Result<bool, sqlx::Error> {
    let mut tx = pool.begin().await?;

    sqlx::query("SET LOCAL statement_timeout = '4s'")
        .execute(&mut *tx)
        .await?;
    sqlx::query("SET LOCAL lock_timeout = '1s'")
        .execute(&mut *tx)
        .await?;

    let result = sqlx::query(
        r#"
        UPDATE transactional_outbox
        SET status = 'completed', finished_at = statement_timestamp(),
            last_error = NULL,
            locked_by = NULL, lock_token = NULL, locked_until = NULL
        WHERE id = $1 AND lock_token = $2 AND status = 'processing'
          AND locked_until > clock_timestamp()
        "#,
    )
    .bind(task_id)
    .bind(lock_token)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(result.rows_affected() == 1)
}
```

The update retains task ownership and lease checks. `true` means a confirmed
completed transition; `false` means no matching current ownership. An error
is not a confirmed task outcome. SQLx rolls back an unfinished transaction
when it is dropped; this does not establish immediate cleanup or a known
commit outcome after network failure. See [SQLx transactions](https://docs.rs/sqlx/0.8.6/sqlx/struct.Transaction.html).

`statement_timeout = '4s'` bounds each SQL statement once it reaches the
server, including time spent waiting for a lock. `lock_timeout = '1s'` aborts
a statement after that much time waiting for an individual lock acquisition.
See [PostgreSQL timeout settings](https://www.postgresql.org/docs/18/runtime-config-client.html#GUC-STATEMENT-TIMEOUT).

These limits do not bound the whole `complete_task` call: pool acquisition,
network waiting, multiple statements, and transaction completion also consume
time. A runtime deadline must wrap that complete adapter operation. Runtime
and PostgreSQL start their clocks at different points; choosing a shorter SQL
timeout does not guarantee its error reaches the runtime before its deadline.
The production defaults are 5000 ms for `OUTBOX_STORAGE_TIMEOUT_MS` and 4000 ms
for `OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS`; the example's 1-second lock limit
is illustrative. These numbers do not define the complete lease budget.
