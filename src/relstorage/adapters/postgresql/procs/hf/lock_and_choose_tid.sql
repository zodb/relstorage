CREATE OR REPLACE FUNCTION lock_and_choose_tid()
RETURNS BIGINT
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
  FROM object_state;

  next_tid_64 := make_current_tid();

  IF next_tid_64 <= current_tid_64 THEN
    next_tid_64 := current_tid_64 + 1;
  END IF;

  RETURN next_tid_64;
END;
$$
LANGUAGE PLPGSQL;
