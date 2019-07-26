CREATE PROCEDURE lock_and_choose_tid()
COMMENT '{CHECKSUM}'
BEGIN
    DECLARE next_tid_64 BIGINT;
    CALL lock_and_choose_tid_p(next_tid_64);
    SELECT next_tid_64;
END;
