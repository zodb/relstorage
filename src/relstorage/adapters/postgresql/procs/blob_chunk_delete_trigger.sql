CREATE OR REPLACE FUNCTION blob_chunk_delete_trigger()
  RETURNS TRIGGER
AS $blob_chunk_delete_trigger$
  -- Unlink large object data file after blob_chunk row deletion
  DECLARE
  cnt integer;
BEGIN
  SELECT count(*) into cnt FROM blob_chunk WHERE chunk=OLD.chunk;
  IF (cnt = 1) THEN
    -- Last reference to this oid, unlink
    PERFORM lo_unlink(OLD.chunk);
  END IF;
  RETURN OLD;
END;
$blob_chunk_delete_trigger$
LANGUAGE plpgsql;
