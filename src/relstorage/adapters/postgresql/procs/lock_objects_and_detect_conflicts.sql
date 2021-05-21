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
    -- XXX: SELECT FOR SHARE does disk I/O! This can become expensive
    -- and possibly lead to database issues.
    -- See https://buttondown.email/nelhage/archive/22ab771c-25b4-4cd9-b316-31a86f737acc
    -- We document this in docs/postgresql/index.rst
    --
    -- Because of that, and because there are ZODB tests that expect
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


    -- Doing this in a single query takes some effort to make sure
    -- that the required rows all get locked. The optimizer is smart
    -- enough to push a <> condition from an outer query into a
    -- subquery. It is *not* smart enough to do the same with a CTE...
    -- ...prior to PG12. In PG12, CTEs can be inlined, and if it was,
    -- the same optimizer error would arise.
    --
    -- Fortunately, in PG12, "CTEs are automatically inlined if they
    -- have no side-effects, are not recursive, and are referenced
    -- only once in the query." The ``FOR SHARE`` clause counts as a
    -- side-effect, and so the CTE Is not inlined.

    -- Should this ever change, we could force the issue by using
    -- 'WITH ... AS MATERIALIZED' but that's not valid syntax before
    -- 12. (It also says recursive CTEs are not inlined, and that *is*
    -- valid on older versions, but this isn't actually a recursive
    -- query, even if we use that keyword, and I don't know if the
    -- keyword alone would be enough to fool it (the plan doesn't
    -- change on 11 when we use the keyword)).

    RETURN QUERY
      WITH locked AS (
        SELECT {CURRENT_OBJECT}.zoid, {CURRENT_OBJECT}.tid, t.tid AS desired
        FROM {CURRENT_OBJECT}
        INNER JOIN unnest(read_current_oids, read_current_tids) t(zoid, tid)
          USING (zoid)
        ORDER BY zoid
        FOR SHARE NOWAIT
      )
      SELECT locked.zoid, locked.tid, NULL::BIGINT, NULL::BYTEA
      FROM locked WHERE locked.tid <> locked.desired
      LIMIT 1;
    -- If that failed to get a lock because it is being modified by another transaction,
    -- it raised an exception.
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

  PERFORM {CURRENT_OBJECT}.zoid
  FROM {CURRENT_OBJECT}
  INNER JOIN temp_store USING(zoid)
  WHERE temp_store.prev_tid <> 0
  ORDER BY {CURRENT_OBJECT}.zoid
  FOR UPDATE OF {CURRENT_OBJECT};


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
