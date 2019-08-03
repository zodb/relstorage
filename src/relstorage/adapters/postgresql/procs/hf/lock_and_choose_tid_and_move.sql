CREATE OR REPLACE FUNCTION lock_and_choose_tid_and_move(
  p_committing_tid BIGINT,
  p_commit BOOLEAN
)
RETURNS BIGINT
AS
$$
BEGIN
  IF p_committing_tid IS NULL THEN
    p_committing_tid := lock_and_choose_tid();
  END IF;

  -- move_from_temp()
  -- First the state for objects
  -- TODO: We probably do not need to order the
  -- temp rows. The locks we need are already held.
  -- Skipping that appears to make a difference in plans and
  -- benchmarks. Confirm that and remove.
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
  ORDER BY zoid
  ON CONFLICT (zoid) DO UPDATE SET
     tid = excluded.tid,
     state_size = excluded.state_size,
     state = excluded.state;

  -- Then blob chunks.
  DELETE FROM blob_chunk
  WHERE zoid IN (
    SELECT zoid FROM temp_store
  );

  INSERT INTO blob_chunk (
    zoid,
    tid,
    chunk_num,
    chunk
  )
  SELECT zoid, p_committing_tid, chunk_num, chunk
  FROM temp_blob_chunk;

  -- History free has no current_object to update.
  RETURN p_committing_tid;
END;
$$
LANGUAGE plpgsql;
