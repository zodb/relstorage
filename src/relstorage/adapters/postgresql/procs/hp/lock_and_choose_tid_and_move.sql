CREATE OR REPLACE FUNCTION lock_and_choose_tid_and_move(
    p_committing_tid BIGINT,
    p_commit BOOLEAN,
    p_username BYTEA,
    p_description BYTEA,
    p_extension BYTEA
)
RETURNS BIGINT
AS
$$
BEGIN
  IF p_committing_tid IS NULL THEN
    p_committing_tid := lock_and_choose_tid(FALSE, p_username, p_description, p_extension);
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
  FROM temp_store
  ORDER BY zoid;

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
  ORDER BY zoid
  ON CONFLICT (zoid) DO UPDATE SET
     tid = excluded.tid;

  RETURN p_committing_tid;
END;
$$
LANGUAGE plpgsql;
