CREATE OR REPLACE FUNCTION lock_objects_and_detect_conflicts(
  read_current_oids BIGINT[],
  read_current_tids BIGINT[]
)
  RETURNS TABLE(zoid BIGINT, tid BIGINT, prev_tid BIGINT, committed_state BYTEA)
AS
$$
BEGIN

  -- Order matters here.
  --
  -- We've tried it both ways: Shared locks and then exclusive locks,
  -- or exclusive locks and then shared locks.
  --
  -- The shared-then-exclusive can fairly easily deadlock; this gets
  -- detected by PostgreSQL which kills one of the transactions to
  -- resolve the situation. How long it takes to find the deadlock is
  -- configurable (up to 1s by default, but typically it happens much
  -- faster because each involved PostgrSQL worker is checking in a
  -- slightly different schedule). Busy servers are sometimes
  -- configured to check less frequently, though. The killed transaction
  -- results in an unresolvable conflict and has to be retried. (There was
  -- going to be a conflict anyway, almost certainly a ReadConflictError,
  -- which is also unresolvable).
  --
  -- The other way, exclusive-then-shared, eliminates the deadlocks.
  -- However, in tests of a busy system, taking the exclusive locks
  -- first and then raising a ReadConflictError from the shared locks,
  -- actually made performance overall *worse*. The theory is that we
  -- spent more time waiting for locks we weren't going to be able to
  -- use anyway.
  --
  -- Thus, take shared locks first.

  IF read_current_oids IS NOT NULL THEN
    -- Caution: SELECT FOR SHARE does disk I/O! This can become expensive
    -- and possibly lead to database issues.
    -- See https://buttondown.email/nelhage/archive/22ab771c-25b4-4cd9-b316-31a86f737acc
    -- We document this in docs/postgresql/index.rst
    --
    -- Partly because of that, and because they benchmark SOOO much faster
    -- than row locks, we use transaction adisory locks, with the ZOID as the
    -- key. NOTE: advisory locks are per-database (not cluster/database server,
    -- named database within a server). This means that we cannot be used in
    -- multiple named *schemas* in a database. But that has never actually been
    -- supported.
    --
    -- Because there are ZODB tests that expect
    -- to get a ReadConflictError with an OID and TID in it
    -- (check_checkCurrentSerialInTransaction), we first run a check
    -- to see if there is definitely an issue. If there is, we just
    -- return. Only if we can't be sure do we go on and take out
    -- shared locks. We need to do this because if the shared locks detect a
    -- conflict, we want to immediately release those locks, and the only way to
    -- do that in PostgreSQL is to raise an exception (unlike MySQL we can't
    -- ROLLBACK the transaction).
    RETURN QUERY -- This just adds to the result table; a final bare ``RETURN`` actually ends execution
    SELECT {CURRENT_OBJECT}.zoid, {CURRENT_OBJECT}.tid, NULL::BIGINT, NULL::BYTEA
    FROM {CURRENT_OBJECT}
    INNER JOIN unnest(read_current_oids, read_current_tids) t(zoid, tid)
      USING (zoid)
    WHERE {CURRENT_OBJECT}.tid <> t.tid
    LIMIT 1;

    IF FOUND THEN
      RETURN;
    END IF;

    -- The inner query has side-effects, so it is guaranteed to execute.
    -- We can break if we actually fail to get a lock, and then immediately
    -- unlock by raising an exception.
    PERFORM got_it FROM (
      SELECT pg_try_advisory_xact_lock_shared(t.zoid) AS got_it
      FROM unnest(read_current_oids) t(zoid)
      ORDER BY t.zoid
    ) locked
    WHERE locked.got_it <> TRUE
    LIMIT 1;
    -- IF that failed to get a shared lock because it is being modified
    -- by another transaction, we need to bail immediately and release the locks
    -- without requiring Python to abort the transaction. Unfortunately,
    -- we have no way of knowing which lock we failed to get.
    IF FOUND THEN
      RAISE EXCEPTION USING ERRCODE = 'lock_not_available',
            HINT = 'readCurrent';
    END IF;

    RETURN QUERY
      SELECT {CURRENT_OBJECT}.zoid, {CURRENT_OBJECT}.tid, NULL::BIGINT, NULL::BYTEA
      FROM {CURRENT_OBJECT}
      INNER JOIN unnest(read_current_oids, read_current_tids) t(zoid, tid)
        USING (zoid)
      WHERE {CURRENT_OBJECT}.tid <> t.tid
      LIMIT 1;
    IF FOUND THEN
      -- We're holding shared locks here, so abort the transaction
      -- and release them; don't wait for Python to do it.
      -- Unfortunately, this means we lose the ability to report exactly the
      -- object that conflicted.
      RAISE EXCEPTION USING ERRCODE = 'lock_not_available',
            HINT = 'readCurrent';
    END IF;
  END IF;

  -- Unlike MySQL, we can simply do the SELECT (with PERFORM) for its
  -- side effects to lock the rows.
  -- This one will block. (We set the PG configuration variable ``lock_timeout``
  -- from the ``commit-lock-timeout`` configuration variable to determine how long.)

  -- A note on the query: PostgreSQL will typcially choose a
  -- sequential scan on the temp_store table and do a nested loop join
  -- against the object_state_pkey index culminating in a sort after
  -- the join. This is the same whether we write a WHERE IN (SELECT)
  -- or a INNER JOIN. (This is without a WHERE prev_tid <> 0 clause.)

  -- That changes substantially if we ANALYZE the temp table;
  -- depending on the data, it might do an MERGE join with an index
  -- scan on temp_store_pkey (lots of data) or it might do a
  -- sequential scan and sort in memory before doing a MERGE join
  -- (little data). (Again without the WHERE prev_tid clause.)

  -- However, ANALYZE takes time, often more time than it takes to actually
  -- do the nested loop join.

  -- If we add a WHERE prev_tid clause, even if we ANALYZE, it will
  -- choose a sequential scan on temp_store with a FILTER. Given that most
  -- transactions contain relatively few objects, and that it would do a
  -- sequential scan /anyway/, this is fine, and worth it to avoid probing
  -- the main index for new objects.

  -- TODO: Is it worth doing partitioning and putting prev_tid = 0 in their own
  -- partition? prev_tid isn't the primary key, zoid is, though, and range
  -- partitioning has to include the primary key when there is one.

  -- This either works or raises an exception.
  PERFORM pg_advisory_xact_lock({CURRENT_OBJECT}.zoid)
  FROM {CURRENT_OBJECT}
  INNER JOIN temp_store USING(zoid)
  WHERE temp_store.prev_tid <> 0
  ORDER BY {CURRENT_OBJECT}.zoid;

  RETURN QUERY
  SELECT cur.zoid, cur.tid,
         temp_store.prev_tid, {OBJECT_STATE_NAME}.state
  FROM {CURRENT_OBJECT} cur
  INNER JOIN temp_store USING (zoid)
  {OBJECT_STATE_JOIN}
  WHERE temp_store.prev_tid <> cur.tid;

  RETURN;

END;
$$
LANGUAGE plpgsql;
