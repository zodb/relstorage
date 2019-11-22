CREATE PROCEDURE lock_objects_and_detect_conflicts(
  read_current_oids_tids JSON,
  constant INT -- A version check to prevent older versions from running
)
  COMMENT '{CHECKSUM}'
label_proc:BEGIN
  DECLARE len INT;
  DECLARE i INT DEFAULT 0;
  DECLARE con_oid BIGINT;
  DECLARE con_tid BIGINT;
  DECLARE prev_timeout INT;
  DECLARE EXIT HANDLER FOR 1205, 1213, 3572
     BEGIN
       -- 3572, ER_LOCK_NOWAIT, "Statement aborted because lock(s)
       -- could not be acquired immediately and NOWAIT is set." is raised in MySQL 8
       -- (at least 8.0.18) if we use FOR SHARE NOWAIT instead of the older syntax.

       -- For 'lock_timeout' (1205) or deadlock errors (1213), restore the old timeout
       -- (if it was a shared lock that failed),
       -- completely rollback the transaction to drop any locks we do have,
       -- (important if we had already taken shared locks then timed out on
       -- exclusive locks.)
       -- and then reraise the exception.
       SET SESSION innodb_lock_wait_timeout = prev_timeout;
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
  -- for more. All plans we've seen have used the PRIMARY key index,
  -- but just in case some combination of conditions might want to choose
  -- something else, we force it.

  -- Speaking of plans: exact column names and order by matter.

  -- When we write the query as ``SELECT zoid FROM current_object
  -- WHERE zoid IN (select ZOID from temp_store) ORDER BY zoid`` the
  -- optimizer transforms that using the semijoin optimization into a
  -- `JOIN temp_store`. Both that transformation and manually writing it that way
  -- produce the terrible plan below, using a temporary table and a filesort.

  -- +----+-------------+--------------++--------++---------+-+-----------------------------------+------+----------+----------------------------------------------+
  -- | id | select_type | table        || type   || key     | | ref                               | rows | filtered | Extra                                        |
  -- +----+-------------+--------------++--------++---------+-+-----------------------------------+------+----------+----------------------------------------------+
  -- | 1  | SIMPLE      | temp_store   || index  || PRIMARY | | <null>                            | 2    | 100.0    | Using index; Using temporary; Using filesort |
  -- | 1  | SIMPLE      | object_state || eq_ref || PRIMARY | | relstoragetest_hf.temp_store.zoid | 1    | 100.0    | Using where; Using index                     |
  -- +----+-------------+--------------++--------++---------+-+-----------------------------------+------+----------+----------------------------------------------+

  -- However, when we explicitly write it to specify a join and to sort using
  -- the temp table, the 'temporary' and 'filesort' go away and we get just 'using index'.
  -- Hooray!

  -- lock in share should NOWAIT
  -- or have a minimum lock timeout.
  -- We detect the MySQL version when we install the
  -- procedure and choose the appropriate statements.

  IF read_current_oids_tids IS NOT NULL THEN
    -- We don't make the server do a round-trip to put this data
    -- into a temp table because we're specifically trying to limit round trips.
    DELETE FROM temp_read_current;
    SET len = JSON_LENGTH(read_current_oids_tids);
    WHILE i < len DO
      INSERT INTO temp_read_current (zoid, tid)
      SELECT JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][0]')),
             JSON_EXTRACT(read_current_oids_tids, CONCAT('$[', i, '][1]'));
      SET i = i + 1;
    END WHILE;

    -- The timeout only goes to 1; this procedure seems to always take roughtly 2s
    -- to actually detect a lock timeout problem, however.
    -- TODO: Can we apply the MAX_EXECUTION_TIME() optimizer hint?
    -- https://dev.mysql.com/doc/refman/5.7/en/optimizer-hints.html#optimizer-hints-execution-time
    -- That's not exactly the same thing as a lock timeout...
    SET prev_timeout = @@innodb_lock_wait_timeout;
    {SET_LOCK_TIMEOUT}

    DELETE FROM temp_locked_zoid;

    INSERT INTO temp_locked_zoid
    SELECT o.zoid
    FROM {CURRENT_OBJECT} o FORCE INDEX (PRIMARY)
    INNER JOIN temp_read_current t FORCE INDEX (PRIMARY)
        ON o.zoid = t.zoid
    ORDER BY t.zoid
    {FOR_SHARE};

    SET SESSION innodb_lock_wait_timeout = prev_timeout;
    -- Signal that we handled shared locks, so any lock failure to come is exclusive locks.
    SET prev_timeout = NULL;

    -- Now return any such rows that we locked
    -- that are in conflict.
    -- No point doing any more work taking exclusive locks, etc,
    -- because we will immediately abort, but we must be careful not to return
    -- an extra result set that's empty: that intereferes with our socket IO
    -- waiting for query results.
    SELECT zoid, {CURRENT_OBJECT}.tid
    INTO con_oid, con_tid
    FROM {CURRENT_OBJECT}
    INNER JOIN temp_read_current USING (zoid)
    WHERE temp_read_current.tid <> {CURRENT_OBJECT}.tid
    LIMIT 1;

    IF con_oid IS NOT NULL THEN
      SELECT con_oid, con_tid, NULL as prev_tid, NULL as state;
      ROLLBACK; -- release locks.
      LEAVE label_proc; -- return
    END IF;

  END IF;

  DELETE FROM temp_locked_zoid;

  INSERT INTO temp_locked_zoid
  SELECT o.zoid
  FROM {CURRENT_OBJECT} o FORCE INDEX (PRIMARY)
  INNER JOIN temp_store t FORCE INDEX (PRIMARY)
      ON o.zoid = t.zoid
  WHERE t.prev_tid <> 0
  ORDER BY t.zoid
  FOR UPDATE;

  SELECT cur.zoid, cur.tid, temp_store.prev_tid, {OBJECT_STATE_NAME}.state
  FROM {CURRENT_OBJECT} cur
  INNER JOIN temp_store USING (zoid)
  {OBJECT_STATE_JOIN}
  WHERE temp_store.prev_tid <> cur.tid;


END;
