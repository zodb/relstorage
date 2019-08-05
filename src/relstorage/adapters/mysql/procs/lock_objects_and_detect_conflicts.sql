CREATE PROCEDURE lock_objects_and_detect_conflicts(
  read_current_oids_tids_text TEXT
)
  COMMENT '{CHECKSUM}'
BEGIN
  DECLARE dummy BIGINT;
  DECLARE len INT;
  DECLARE i INT DEFAULT 0;
  DECLARE read_current_oids_tids JSON;

  SELECT COUNT(*)
  FROM (
    SELECT zoid
    FROM {CURRENT_OBJECT}
    WHERE zoid IN (
      SELECT zoid
      FROM temp_store
    )
    ORDER BY zoid
    FOR UPDATE
  ) t
  INTO dummy;

  -- lock in share should NOWAIT
  -- or have a minimum lock timeout.
  -- We detect the MySQL version when we install the
  -- procedure and choose the appropriate statements.

  IF read_current_oids_tids_text IS NOT NULL THEN
    -- We must send the parameter in as TEXT; on Python 2 with
    -- mysqlclient, trying to send it directly as JSON always fails
    -- with "Cannot create a JSON value from a string with CHARACTER
    -- SET 'binary'.", no matter what the character_set_client or
    -- character_set_connection is, and no matter whether the
    -- parameter in Python is bytes or unicode. This has also been seen
    -- with Python 3 on Travis CI, but not locally on the mac, so there's
    -- some system dependent behaviour involved.
    SET read_current_oids_tids = CAST(read_current_oids_tids_text AS JSON);
    SET len = JSON_LENGTH(read_current_oids_tids);
    WHILE i < len DO
      INSERT INTO temp_read_current (zoid, tid)
      SELECT JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][0]')),
             JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][1]'));
      SET i = i + 1;
    END WHILE;

    {SET_LOCK_TIMEOUT}

    SELECT COUNT(*)
    FROM (
      SELECT zoid
      FROM {CURRENT_OBJECT}
      WHERE zoid IN (
        SELECT zoid
        FROM temp_read_current
      )
      ORDER BY zoid
      {FOR_SHARE}
    ) t
    INTO dummy;

  END IF;

  -- readCurrent conflicts first so we don't waste time resolving
  -- state conflicts if we are going to fail the transaction.

  SELECT zoid, {CURRENT_OBJECT}.tid, NULL
  FROM {CURRENT_OBJECT}
  INNER JOIN temp_read_current USING (zoid)
  WHERE temp_read_current.tid <> {CURRENT_OBJECT}.tid
  UNION ALL
  SELECT zoid, tid, prev_tid
  FROM {CURRENT_OBJECT}
  INNER JOIN temp_store USING (zoid)
  WHERE temp_store.prev_tid <> {CURRENT_OBJECT}.tid;


END;
