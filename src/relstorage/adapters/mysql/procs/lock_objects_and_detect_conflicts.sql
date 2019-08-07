CREATE PROCEDURE lock_objects_and_detect_conflicts(
  read_current_oids_tids JSON
)
  COMMENT '{CHECKSUM}'
BEGIN
  DECLARE len INT;
  DECLARE i INT DEFAULT 0;

  -- We have to force the server to materialize the results,
  -- otherwise not all rows actually get locked.
  CREATE TEMPORARY TABLE IF NOT EXISTS temp_locked_zoid (
    zoid BIGINT PRIMARY KEY
  );

  DELETE FROM temp_locked_zoid;

  INSERT INTO temp_locked_zoid
  SELECT zoid
  FROM {CURRENT_OBJECT}
  WHERE zoid IN (
      SELECT zoid
      FROM temp_store
  )
  ORDER BY zoid
  FOR UPDATE;


  -- lock in share should NOWAIT
  -- or have a minimum lock timeout.
  -- We detect the MySQL version when we install the
  -- procedure and choose the appropriate statements.

  IF read_current_oids_tids IS NOT NULL THEN
    SET len = JSON_LENGTH(read_current_oids_tids);
    WHILE i < len DO
      INSERT INTO temp_read_current (zoid, tid)
      SELECT JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][0]')),
             JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][1]'));
      SET i = i + 1;
    END WHILE;

    {SET_LOCK_TIMEOUT}

    DELETE FROM temp_locked_zoid;

    INSERT INTO temp_locked_zoid
    SELECT zoid
    FROM {CURRENT_OBJECT}
    WHERE zoid IN (
        SELECT zoid
        FROM temp_read_current
    )
    ORDER BY zoid
    {FOR_SHARE};

  END IF;

  -- readCurrent conflicts first so we don't waste time resolving
  -- state conflicts if we are going to fail the transaction.

  SELECT zoid, {CURRENT_OBJECT}.tid, NULL, NULL
  FROM {CURRENT_OBJECT}
  INNER JOIN temp_read_current USING (zoid)
  WHERE temp_read_current.tid <> {CURRENT_OBJECT}.tid
  UNION ALL
  SELECT cur.zoid, cur.tid, temp_store.prev_tid, {OBJECT_STATE_NAME}.state
  FROM {CURRENT_OBJECT} cur
  INNER JOIN temp_store USING (zoid)
  {OBJECT_STATE_JOIN}
  WHERE temp_store.prev_tid <> cur.tid;


END;
