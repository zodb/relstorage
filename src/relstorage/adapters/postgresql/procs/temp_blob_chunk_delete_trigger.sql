CREATE OR REPLACE FUNCTION temp_blob_chunk_delete_trigger()
  RETURNS TRIGGER
AS $temp_blob_chunk_delete_trigger$
  -- Unlink large object data file after temp_blob_chunk row deletion
  DECLARE
  cnt integer;
BEGIN
  SELECT count(*) into cnt FROM blob_chunk WHERE chunk=OLD.chunk;
  IF (cnt = 0) THEN
    -- No more references to this oid, unlink
    PERFORM lo_unlink(OLD.chunk);
  END IF;
  RETURN OLD;
END;
$temp_blob_chunk_delete_trigger$
LANGUAGE plpgsql;
