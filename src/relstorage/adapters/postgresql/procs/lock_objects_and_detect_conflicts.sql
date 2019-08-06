CREATE OR REPLACE FUNCTION lock_objects_and_detect_conflicts(
  read_current_oids_tids BIGINT[][]
)
  RETURNS TABLE(zoid BIGINT, tid BIGINT, prev_tid BIGINT)
AS
$$
BEGIN
  -- Unlike MySQL, we can simply do the SELECT (with PERFORM) for its
  -- side effects to lock the rows.

  PERFORM {CURRENT_OBJECT}.zoid
  FROM {CURRENT_OBJECT}
  WHERE {CURRENT_OBJECT}.zoid IN (
      SELECT temp_store.zoid
      FROM temp_store
  )
  ORDER BY {CURRENT_OBJECT}.zoid
  FOR UPDATE;


  -- lock in share should NOWAIT

  IF read_current_oids_tids IS NOT NULL THEN
    PERFORM {CURRENT_OBJECT}.zoid
    FROM {CURRENT_OBJECT}
    WHERE {CURRENT_OBJECT}.zoid IN (
        SELECT read_current_oids_tids[i][1]
        FROM generate_subscripts(read_current_oids_tids, 1) g1(i)
    )
    ORDER BY {CURRENT_OBJECT}.zoid
    FOR SHARE NOWAIT;

  END IF;

  -- readCurrent conflicts first so we don't waste time resolving
  -- state conflicts if we are going to fail the transaction.
  RETURN QUERY
  SELECT {CURRENT_OBJECT}.zoid, {CURRENT_OBJECT}.tid, NULL::bigint
  FROM {CURRENT_OBJECT}
  INNER JOIN (SELECT read_current_oids_tids[i][1], read_current_oids_tids[i][2]
              FROM generate_subscripts(read_current_oids_tids, 1) g1(i)) t(zoid, tid) USING (zoid)
  WHERE t.tid <> {CURRENT_OBJECT}.tid;

  RETURN QUERY
  SELECT {CURRENT_OBJECT}.zoid, {CURRENT_OBJECT}.tid, temp_store.prev_tid
  FROM {CURRENT_OBJECT}
  INNER JOIN temp_store USING (zoid)
  WHERE temp_store.prev_tid <> {CURRENT_OBJECT}.tid;

  RETURN;

END;
$$
LANGUAGE plpgsql;
