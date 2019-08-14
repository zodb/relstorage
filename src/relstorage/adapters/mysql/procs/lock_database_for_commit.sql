CREATE PROCEDURE lock_database_for_commit()
COMMENT '{CHECKSUM}'
BEGIN
  DECLARE scratch BIGINT;
  DECLARE prev_timeout INT;
  DECLARE EXIT HANDLER FOR 1205, 1213
     BEGIN
       -- For 'lock_timeout' or deadlock errors, restore the old timeout
       -- completely rollback the transaction to drop any locks we do have,
       -- (important if we had already taken shared locks then timed out on
       -- exclusive locks.)
       -- and then reraise the exception.
       -- We should NEVER get here.
       SET SESSION innodb_lock_wait_timeout = prev_timeout;
       ROLLBACK;
       RESIGNAL SET MESSAGE_TEXT = 'Failed to get commit lock.';
     END;

    -- We're in the commit phase of two-phase commit.
    -- It's very important not to error out here.
    -- So we need a very long wait to get the commit lock.
    SET prev_timeout = @@innodb_lock_wait_timeout;
    SET SESSION innodb_lock_wait_timeout = 500;

    SELECT tid
    INTO scratch
    FROM commit_row_lock
    FOR UPDATE;

    -- As soon as we're locked, we can restore the timeout.
    -- We should never have a violation on the rows we've already
    -- locked for update.
    SET SESSION innodb_lock_wait_timeout = prev_timeout;
END;
