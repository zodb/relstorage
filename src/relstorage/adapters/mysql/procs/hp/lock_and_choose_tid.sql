CREATE PROCEDURE lock_and_choose_tid(
    p_packed BOOLEAN,
    p_username BLOB,
    p_description BLOB,
    p_extension BLOB
)
COMMENT '{CHECKSUM}'
BEGIN
    DECLARE scratch BIGINT;
    DECLARE next_tid_64, current_tid_64 BIGINT;

    SELECT tid
    INTO scratch
    FROM commit_row_lock
    FOR UPDATE;

    SELECT COALESCE(MAX(tid), 0)
    INTO current_tid_64
    FROM transaction;

    CALL make_current_tid(next_tid_64);

    IF next_tid_64 <= current_tid_64 THEN
        SET next_tid_64 = current_tid_64 + 1;
    END IF;

    INSERT INTO transaction (
        tid, packed, username, description, extension
    )
    VALUES (
        next_tid_64, p_packed, p_username, p_description, p_extension
    );

    SELECT next_tid_64;
END;
