CREATE TABLE IF NOT EXISTS transactional_outbox
(
    id                 uuid PRIMARY KEY,
    aggregate_type     text NOT NULL,
    aggregate_id       text NOT NULL,
    aggregate_sequence bigint NOT NULL CHECK (aggregate_sequence > 0),
    task_type       text NOT NULL CHECK (task_type <> ''),
    task_version     integer NOT NULL CHECK (task_version > 0),
    payload            jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    metadata           jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(metadata) = 'object'),
    status             text NOT NULL DEFAULT 'pending'
                       CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    created_at         timestamptz NOT NULL DEFAULT statement_timestamp(),
    available_at       timestamptz NOT NULL DEFAULT statement_timestamp(),
    finished_at        timestamptz,
    attempts           integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts       integer NOT NULL DEFAULT 10 CHECK (max_attempts > 0),
    last_error         text,
    locked_by          text,
    lock_token         uuid,
    locked_until       timestamptz,
    CHECK (attempts <= max_attempts),
    CHECK ((status = 'processing' AND locked_by IS NOT NULL AND lock_token IS NOT NULL AND locked_until IS NOT NULL)
        OR (status <> 'processing' AND locked_by IS NULL AND lock_token IS NULL AND locked_until IS NULL)),
    CHECK ((status IN ('completed', 'failed')) = (finished_at IS NOT NULL)),
    CHECK (status <> 'pending' OR attempts < max_attempts)
);

CREATE INDEX IF NOT EXISTS transactional_outbox_ready_idx
    ON transactional_outbox (task_type, task_version, available_at, id)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS transactional_outbox_expired_idx
    ON transactional_outbox (locked_until, id)
    WHERE status = 'processing';
