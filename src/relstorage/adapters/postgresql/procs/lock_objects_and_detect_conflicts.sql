CREATE OR REPLACE FUNCTION lock_objects_and_detect_conflicts(
  read_current_oids BIGINT[],
  read_current_tids BIGINT[]
)
  RETURNS TABLE(zoid BIGINT, tid BIGINT, prev_tid BIGINT, committed_state BYTEA)
AS
$$
BEGIN

  -- lock in share should NOWAIT

  IF read_current_oids IS NOT NULL THEN
    -- readCurrent conflicts first so we don't waste time resolving
    -- state conflicts if we are going to fail the transaction. We only need to return
    -- the first conflict; it will immediately raise an exception.
    -- Up through PG12, RETURN QUERY does *NOT* stream to the client, it buffers
    -- everything. So we must manually break and not do the locking.

    -- Doing this in a single query takes some effort to make sure
    -- that the required rows all get locked. The optimizer is smart
    -- enough to push a <> condition from an outer query into a
    -- subquery. It is *not* smart enough to do the same with a CTE...
    -- ...prior to PG12. In PG12, CTEs can be inlined and presumably
    -- the same optimizer issue would arise. This can be fixed by
    -- saying 'WITH AS NOT MATERIALIZED', but that's not valid syntax
    -- before 12. It also says recursive CTEs are not inlined, and
    -- that is valid on older versions, but this isn't actually a
    -- recursive query, even if we use that keyword, and I don't know
    -- if the keyword alone would be enough to fool it (the plan
    -- doesn't change on 11 when we use the keyword). 12 isn't
    -- released yet; we'll cross that bridge when we get there.
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
    IF FOUND THEN
      RETURN;
    END IF;
  END IF;

  -- Unlike MySQL, we can simply do the SELECT (with PERFORM) for its
  -- side effects to lock the rows.
  -- This one will block.
  PERFORM {CURRENT_OBJECT}.zoid
  FROM {CURRENT_OBJECT}
  WHERE {CURRENT_OBJECT}.zoid IN (
      SELECT temp_store.zoid
      FROM temp_store
  )
  ORDER BY {CURRENT_OBJECT}.zoid
  FOR UPDATE;

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
