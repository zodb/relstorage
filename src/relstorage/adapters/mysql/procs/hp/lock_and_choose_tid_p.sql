CREATE PROCEDURE lock_and_choose_tid_p(
  OUT next_tid_64 BIGINT,
  IN p_packed BOOLEAN,
  IN p_username BLOB,
  IN p_description BLOB,
  IN p_extension BLOB
)
COMMENT '{CHECKSUM}'
BEGIN
    DECLARE scratch BIGINT;
    DECLARE current_tid_64 BIGINT;

    -- We're in the commit phase of two-phase commit.
    -- It's very important not to error out here.
    -- So we need a very long wait to get the commit lock.
    SET SESSION innodb_lock_wait_timeout = 500;

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
END;
