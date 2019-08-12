CREATE PROCEDURE clean_temp_state(IN p_truncate BOOLEAN)
COMMENT '{CHECKSUM}'
BEGIN

IF p_truncate THEN
  TRUNCATE temp_store;
  TRUNCATE temp_read_current;
  TRUNCATE temp_blob_chunk;
  TRUNCATE temp_locked_zoid;
ELSE
  DELETE FROM temp_store;
  DELETE FROM temp_read_current;
  DELETE FROM temp_blob_chunk;
  DELETE FROM temp_locked_zoid;
END IF;
END;
