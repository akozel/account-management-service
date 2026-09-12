-- Execute with one UUID parameter. Only failed work can be restarted.
-- Keep the ID and payload: an idempotent sender may already know this operation.
UPDATE transactional_outbox
SET status = 'pending', attempts = 0, available_at = statement_timestamp(),
    finished_at = NULL, last_error = NULL,
    locked_by = NULL, lock_token = NULL, locked_until = NULL
WHERE id = $1 AND status = 'failed';
