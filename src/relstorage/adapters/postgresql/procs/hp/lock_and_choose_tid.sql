CREATE OR REPLACE FUNCTION lock_and_choose_tid(
  p_packed BOOLEAN,
  p_username BYTEA,
  p_description BYTEA,
  p_extension BYTEA
)
RETURNS BIGINT
  -- We're in the commit phase of two-phase commit.
  -- It's very important not to error out here.
  -- So we need a very long wait to get the commit lock.
  -- Recall PostgreSQL uses milliseconds
    SET lock_timeout = 500000
AS
$$
DECLARE
    scratch BIGINT;
    current_tid_64 BIGINT;
  next_tid_64 BIGINT;
BEGIN
    SELECT tid
    INTO scratch
    FROM commit_row_lock
    FOR UPDATE;

    SELECT COALESCE(MAX(tid), 0)
    INTO current_tid_64
    FROM transaction;

    next_tid_64 := make_current_tid();

    IF next_tid_64 <= current_tid_64 THEN
        next_tid_64 := current_tid_64 + 1;
    END IF;

    INSERT INTO transaction (
        tid, packed, username, description, extension
    )
    VALUES (
        next_tid_64, p_packed, p_username, p_description, p_extension
    );

  RETURN next_tid_64;
END;
$$
LANGUAGE plpgsql;
