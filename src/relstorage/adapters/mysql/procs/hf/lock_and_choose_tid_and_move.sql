CREATE PROCEDURE lock_and_choose_tid_and_move(
  p_committing_tid BIGINT,
  p_commit BOOLEAN
)
COMMENT '{CHECKSUM}'
BEGIN
  IF p_committing_tid IS NULL THEN
    CALL lock_and_choose_tid_p(p_committing_tid);
  END IF;

  -- move_from_temp()
  -- First the state for objects

  INSERT INTO object_state (
    zoid,
    tid,
    state_size,
    state
  )
  SELECT zoid,
         p_committing_tid,
         COALESCE(LENGTH(state), 0),
         state
  FROM temp_store
  ON DUPLICATE KEY UPDATE
     tid = VALUES(tid),
     state_size = VALUES(state_size),
     state = VALUES(state);

  -- Then blob chunks. First delete in case we shrunk. The MySQL
  -- optimizer, up through at least 5.7.17 doesn't like actual
  -- subqueries in a DELETE statement. See
  -- https://github.com/zodb/relstorage/issues/175 The suggested
  -- workaround is to use multi-table deletes
  -- (https://dev.mysql.com/doc/refman/5.7/en/subquery-optimization.html)
  IF EXISTS (SELECT 1 FROM temp_blob_chunk) THEN
    DELETE bc
    FROM blob_chunk bc
    INNER JOIN temp_store sq
        ON bc.zoid = sq.zoid;

    INSERT INTO blob_chunk (
      zoid,
      tid,
      chunk_num,
      chunk
    )
    SELECT zoid, p_committing_tid, chunk_num, chunk
    FROM temp_blob_chunk;
  END IF;

  -- History free has no current_object to update.


  IF p_commit THEN
    COMMIT;
    -- Clean up all our temp state, *after* releasing our locks.
    CALL clean_temp_state(false);
    COMMIT;
  END IF;

  SELECT p_committing_tid;
END;
