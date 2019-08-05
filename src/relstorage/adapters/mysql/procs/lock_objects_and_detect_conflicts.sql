CREATE PROCEDURE lock_objects_and_detect_conflicts(
  read_current_oids_tids JSON
)
  COMMENT '{CHECKSUM}'
BEGIN
  DECLARE dummy BIGINT;
  DECLARE len INT;
  DECLARE i INT DEFAULT 0;

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
  -- we use the timeout because it works on 5.7 and 8
  -- while NOWAIT only works on 8.
  IF read_current_oids_tids IS NOT NULL THEN
    SET len = JSON_LENGTH(read_current_oids_tids);
    WHILE i < len DO
      INSERT INTO temp_read_current (zoid, tid)
      SELECT JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][0]')),
             JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][1]'));
      SET i = i + 1;
    END WHILE;

    SET SESSION innodb_lock_wait_timeout = 1;

    SELECT COUNT(*)
    FROM (
      SELECT zoid
      FROM {CURRENT_OBJECT}
      WHERE zoid IN (
        SELECT zoid
        FROM temp_read_current
      )
      ORDER BY zoid
      LOCK IN SHARE MODE
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
