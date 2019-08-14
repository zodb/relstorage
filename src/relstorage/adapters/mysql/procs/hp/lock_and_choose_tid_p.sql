CREATE PROCEDURE lock_and_choose_tid_p(
  OUT next_tid_64 BIGINT,
  IN p_packed BOOLEAN,
  IN p_username BLOB,
  IN p_description BLOB,
  IN p_extension BLOB
)
COMMENT '{CHECKSUM}'
BEGIN
    DECLARE committed_tid_64 BIGINT;

    CALL lock_database_for_commit();

    SELECT COALESCE(MAX(tid), 0)
    INTO committed_tid_64
    FROM transaction;

    CALL make_current_tid(next_tid_64);

    IF next_tid_64 <= committed_tid_64 THEN
        SET next_tid_64 = committed_tid_64 + 1;
    END IF;

    INSERT INTO transaction (
        tid, packed, username, description, extension
    )
    VALUES (
        next_tid_64, p_packed, p_username, p_description, p_extension
    );
END;
