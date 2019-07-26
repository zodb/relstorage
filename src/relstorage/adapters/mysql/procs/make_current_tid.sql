CREATE PROCEDURE make_current_tid(OUT tid_64 BIGINT)
COMMENT '{CHECKSUM}'
BEGIN
  CALL make_tid_for_epoch(
    UNIX_TIMESTAMP(UTC_TIMESTAMP(6)),
    tid_64
  );
END;
