CREATE OR REPLACE FUNCTION lock_and_choose_tid()
RETURNS BIGINT
  -- We're in the commit phase of two-phase commit.
  -- It's very important not to error out here.
  -- So we need a very long wait to get the commit lock.
  -- Recall PostgreSQL uses milliseconds (it can also parse
  -- strings like '10min').
    SET lock_timeout = 600000
AS
$$
DECLARE
  scratch BIGINT;
  current_tid_64 BIGINT;
  next_tid_64 BIGINT;
BEGIN
  -- In a history-free database, if we don't have a lock for the TID,
  -- we could potentially end up with multiple processes choosing the
  -- same TID (because there's no unique index on tids). As long as they were
  -- working with different objects, there would be no database error
  -- to indicate that the TIDs were the same, and a later view of the
  -- transaction would be incorrect (containing multiple distinct
  -- transactions).
  SELECT tid
  INTO scratch
  FROM commit_row_lock
  FOR UPDATE;

  SELECT COALESCE(MAX(tid), 0)
  INTO current_tid_64
  FROM object_state;

  next_tid_64 := make_current_tid();

  IF next_tid_64 <= current_tid_64 THEN
    next_tid_64 := current_tid_64 + 1;
  END IF;

  RETURN next_tid_64;
END;
$$
LANGUAGE PLPGSQL;
