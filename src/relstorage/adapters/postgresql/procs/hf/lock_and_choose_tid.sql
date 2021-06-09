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
  PERFORM pg_advisory_xact_lock(-1);

  SELECT COALESCE(MAX(tid), 0)
  INTO current_tid_64
  FROM object_state;

  next_tid_64 := make_current_tid();

  IF next_tid_64 <= current_tid_64 THEN
    next_tid_64 := current_tid_64 + 1;
  END IF;

  -- An alternate way to do this WITHOUT taking a global
  -- database lock is to store the highest seen TID
  -- (recall tids must monotonically increase) in a
  -- SEQUENCE. Then only this procedure needs to be locked,
  -- and the lock can quickly be released. That has a distinct
  -- advantage over a ``FOR UPDATE`` or table lock in that
  -- we can control releasing it completely server side,
  -- and theoretically gains us much more concurrency.
  /*
  PERFORM pg_advisory_lock(-1);

  SELECT COALESCE(MAX(tid), 0)
  INTO current_tid_64
  FROM object_state;

  next_tid_64 := make_current_tid();

  seq_tid_64 := nextval('rs_tid_seq');

  IF next_tid_64 <= current_tid_64 THEN
    next_tid_64 := current_tid_64 + 1;
  END IF;

  IF next_tid_64 <= seq_tid_64 THEN
      next_tid_64 := seq_tid_64 + 1;
  END IF;

  -- In general this next statement isn't safe, but
  -- so long as we *only* operate on this sequence in this
  -- block protected by the advisory lock, its fine.

  PERFORM setval('rs_tid_seq', next_tid_64);
  RAISE WARNING 'Using new tid %', next_tid_64;
  PERFORM pg_advisory_unlock(-1);
   */

  -- That needs some care taken in the _move procedure, though.
  -- Specifically, if I *just* used a sequence (and ignored make_current_tid()),
  -- and restored the ``ORDER BY zoid`` to the ``INSERT INTO object_state``
  -- statement, zodbshootout worked fine with high concurrency.
  -- But if I used the actual statement above, I got some unexpelainable
  -- RelStorage cache coherency errors, where a row was visible before it
  -- should be (apparently). This needs some thought.

  RETURN next_tid_64;
END;
$$
LANGUAGE PLPGSQL;
