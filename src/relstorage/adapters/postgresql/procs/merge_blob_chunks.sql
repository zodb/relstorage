CREATE OR REPLACE FUNCTION merge_blob_chunks()
  RETURNS VOID
AS
$$
  -- Merge blob chunks into one entirely on the server.
DECLARE
  masterfd integer;
  masterloid oid;
  chunkfd integer;
  rec record;
  buf bytea;
BEGIN
  -- In history-free mode, zoid and chunk_num is the key.
  -- in history-preserving mode, zoid/tid/chunk_num is the key.
  -- for chunks, zoid and tid must always be used to look up the master.
  FOR rec IN SELECT zoid, tid, chunk_num, chunk
    FROM blob_chunk WHERE chunk_num > 0
    ORDER BY zoid, chunk_num LOOP
    -- Find the master and open it.
    SELECT chunk
    INTO STRICT masterloid
    FROM blob_chunk
    WHERE zoid = rec.zoid and tid = rec.tid AND chunk_num = 0;

    -- open master for writing
    SELECT lo_open(masterloid, 131072) -- 0x20000, AKA INV_WRITE
    INTO STRICT masterfd;

    -- position at the end
    PERFORM lo_lseek(masterfd, 0, 2);
    -- open the child for reading
    SELECT lo_open(rec.chunk, 262144) -- 0x40000 AKA INV_READ
    INTO STRICT chunkfd;
    -- copy the data
    LOOP
      SELECT loread(chunkfd, 8192)
      INTO buf;

      EXIT WHEN LENGTH(buf) = 0;

      PERFORM lowrite(masterfd, buf);
    END LOOP;
    -- close the files
    PERFORM lo_close(chunkfd);
    PERFORM lo_close(masterfd);

  END LOOP;

  -- Finally, remove the redundant chunks. Our trigger
  -- takes care of removing the large objects.
  DELETE FROM blob_chunk WHERE chunk_num > 0;

END;
$$
LANGUAGE plpgsql;
