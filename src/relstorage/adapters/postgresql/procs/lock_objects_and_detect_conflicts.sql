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
  -- We want to first take exclusive locks for the modified objects
  -- (and wait if needed) and then take NOWAIT shared locks for the
  -- readCurrent objects.
  --
  -- If we do it the other way around (RelStorage < 3.5), we can
  -- *easily* deadlock in lock order on the server, which would result
  -- in us raising ``UnableToLockRows...`` exception, which
  -- immediately aborts the transaction and doesn't allow for any
  -- conflict resolution.
  --
  -- However, taking exclusive first and only then shared NOWAIT
  -- doesn't deadlock (I think), certainly not as easily. This means a
  -- transaction might have to wait, or timeout, but if it gets
  -- through, it has a chance to resolve conflicts and commit.
  --
  -- One of the appealing properties of taking the shared first,
  -- though, is that if we *are* doomed to failure (because of a
  -- ReadCurrent error), we can know it immediately. We can still make
  -- that check before we try to wait for exclusive locks, but since
  -- we can't actually lock those rows yet, we need to perform the
  -- check *again*, after taking the exclusive locks (and this time
  -- lock them to be sure they don't change).

  -- lock in share should NOWAIT
  IF read_current_oids IS NOT NULL THEN
    -- A pre-check: If we can detect a single readCurrent conflict,
    -- do so, send it to the server where it will raise VoteConflictError,
    -- and just bail. We could make this raise an exception and abort the
    -- transaction, but we're not holding any locks here, and the error message
    -- from VoteConflictError is more informative.
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

  IF read_current_oids IS NOT NULL THEN

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

    -- XXX: SELECT FOR SHARE does disk I/O! This can become expensive
    -- and possibly lead to database issues.
    -- See https://buttondown.email/nelhage/archive/22ab771c-25b4-4cd9-b316-31a86f737acc
    -- We document this in docs/postgresql/index.rst
    RETURN QUERY -- This just adds to the result table; a final bare ``RETURN`` actually ends execution
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
      -- We're holding exclusive locks here, so abort the transaction
      -- and release them; don't wait for Python to do it.
      RAISE EXCEPTION USING ERRCODE = 'lock_not_available'
            HINT = 'readCurrent';
    END IF;
  END IF;


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
