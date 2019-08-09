CREATE PROCEDURE lock_objects_and_detect_conflicts(
  read_current_oids_tids JSON
)
  COMMENT '{CHECKSUM}'
label_proc:BEGIN
  DECLARE len INT;
  DECLARE i INT DEFAULT 0;
  DECLARE prev_timeout INT;
  DECLARE EXIT HANDLER FOR 1205, 1213
     BEGIN
       -- For 'lock_timeout' or deadlock errors, restore the old timeout
       -- (if it was a shared lock that failed),
       -- completely rollback the transaction to drop any locks we do have,
       -- (important if we had already taken shared locks then timed out on
       -- exclusive locks.)
       -- and then reraise the exception.
       SET @@innodb_lock_wait_timeout = prev_timeout;
       -- The server depends on these error messages.
       -- TODO: Write test cases that verify we rollback our shared locks if we
       -- can't get the exclusive locks.
       IF prev_timeout IS NULL THEN
         SET @msg = 'Failed to get exclusive locks.';
       ELSE
         SET @msg = 'Failed to get shared locks.';
       END IF;
       ROLLBACK;
       RESIGNAL SET MESSAGE_TEXT = @msg;
     END;

  -- We have to force the server to materialize the results, otherwise
  -- not all rows actually get locked. Also, it is critically
  -- important to do ``eq_ref`` joins against the table we're locking,
  -- and only require a PRIMARY KEY access, to avoid locking more
  -- index records than necessary (any sort of range access can lead
  -- to unexpected locking between transactions). See mysql/locker.py
  -- for more.
  CREATE TEMPORARY TABLE IF NOT EXISTS temp_locked_zoid (
    zoid BIGINT PRIMARY KEY
  );



  -- TODO: Do the temp table creation in the mover.py
  -- TODO: Investigate these plans. I was seeing them both use filesort and
  -- temporary table. That can't be right. Or good.

  -- lock in share should NOWAIT
  -- or have a minimum lock timeout.
  -- We detect the MySQL version when we install the
  -- procedure and choose the appropriate statements.

  IF read_current_oids_tids IS NOT NULL THEN
    -- We don't make the server do a round-trip to put this data
    -- into a temp table because we're specifically trying to limit round trips.
    SET len = JSON_LENGTH(read_current_oids_tids);
    WHILE i < len DO
      INSERT INTO temp_read_current (zoid, tid)
      SELECT JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][0]')),
             JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][1]'));
      SET i = i + 1;
    END WHILE;

    -- SIGNAL SQLSTATE 'HY000' SET  MYSQL_ERRNO = 1205;

    -- The timeout only goes to 1; this procedure seems to always take roughtly 2s
    -- to actually detect a lock timeout problem, however.
    SET prev_timeout = @@innodb_lock_wait_timeout;
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

    SET @@innodb_lock_wait_timeout = prev_timeout;
    -- Signal that we handled shared locks, so any lock failure to come is exclusive locks.
    SET prev_timeout = NULL;

    -- Now return any such rows that we locked
    -- that are in conflict. This forms its own result set.
    -- No point doing any more work taking exclusive locks, etc,
    -- because we will immediately abort.
    SELECT zoid, {CURRENT_OBJECT}.tid, NULL as prev_tid, NULL as state
    FROM {CURRENT_OBJECT}
    INNER JOIN temp_read_current USING (zoid)
    WHERE temp_read_current.tid <> {CURRENT_OBJECT}.tid
    LIMIT 1;

    IF FOUND_ROWS() > 0 THEN
      ROLLBACK; -- release locks.
      LEAVE label_proc; -- return
    END IF;

  END IF;

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

  SELECT cur.zoid, cur.tid, temp_store.prev_tid, {OBJECT_STATE_NAME}.state
  FROM {CURRENT_OBJECT} cur
  INNER JOIN temp_store USING (zoid)
  {OBJECT_STATE_JOIN}
  WHERE temp_store.prev_tid <> cur.tid;


END;
