CREATE PROCEDURE lock_and_choose_tid_and_move()
COMMENT '{CHECKSUM}'
BEGIN
  DECLARE tid_64 BIGINT;

  CALL lock_and_choose_tid_p(tid_64);

  -- move_from_temp()
  -- First the state for objects

  INSERT INTO object_state (
    zoid,
    tid,
    state_size,
    state
  )
  SELECT zoid,
         tid_64,
         COALESCE(LENGTH(state), 0),
         state
  FROM temp_store
  ORDER BY zoid
  ON DUPLICATE KEY UPDATE
     tid = VALUES(tid),
     state_size = VALUES(state_size),
     state = VALUES(state);

  -- Then blob chunks. First delete in case we shrunk.
  -- The MySQL optimizer, up
  -- through at least 5.7.17 doesn't like actual subqueries in a DELETE
  -- statement. See https://github.com/zodb/relstorage/issues/175
  DELETE bc
  FROM blob_chunk bc
  INNER JOIN (SELECT zoid FROM temp_store) sq
         ON bc.zoid = sq.zoid;

  INSERT INTO blob_chunk (
    zoid,
    tid,
    chunk_num,
    chunk
  )
  SELECT zoid, tid_64, chunk_num, chunk
  FROM temp_blob_chunk;

  -- History free has no current_object to update.

  SELECT tid_64;
END;
