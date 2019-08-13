CREATE PROCEDURE lock_and_choose_tid_and_move(
    p_committing_tid BIGINT,
    p_commit BOOLEAN,
    p_username BLOB,
    p_description BLOB,
    p_extension BLOB
)
COMMENT '{CHECKSUM}'
BEGIN
  IF p_committing_tid IS NULL THEN
    CALL lock_and_choose_tid_p(p_committing_tid, FALSE, p_username, p_description, p_extension);
  END IF;

  -- move_from_temp()
  -- First the object state.
  INSERT INTO object_state (
    zoid,
    tid,
    prev_tid,
    md5,
    state_size,
    state
  )
  SELECT zoid,
         p_committing_tid,
         prev_tid,
         md5,
         COALESCE(LENGTH(state), 0),
         state
  FROM temp_store;


  -- Now blob chunks.
  INSERT INTO blob_chunk (
    zoid,
    tid,
    chunk_num,
    chunk
  )
  SELECT zoid, p_committing_tid, chunk_num, chunk
  FROM temp_blob_chunk;

  -- update_current
  INSERT INTO current_object (zoid, tid)
  SELECT zoid, tid
  FROM object_state
  WHERE tid = p_committing_tid
  ON DUPLICATE KEY UPDATE
     tid = VALUES(tid);

  IF p_commit THEN
    COMMIT;
    CALL clean_temp_state(false);
    COMMIT;
  END IF;



  SELECT p_committing_tid;
END;
