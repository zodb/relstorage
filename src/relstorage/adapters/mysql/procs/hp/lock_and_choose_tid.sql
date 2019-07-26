CREATE PROCEDURE lock_and_choose_tid(
    p_packed BOOLEAN,
    p_username BLOB,
    p_description BLOB,
    p_extension BLOB
)
COMMENT '{CHECKSUM}'
BEGIN
  DECLARE next_tid_64 BIGINT;
  CALL lock_and_choose_tid_p(
    next_tid_64,
    p_packed,
    p_username,
    p_description,
    p_extension
  );
  SELECT next_tid_64;
END;
