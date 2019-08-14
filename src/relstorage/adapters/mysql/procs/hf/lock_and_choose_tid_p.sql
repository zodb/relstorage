CREATE PROCEDURE lock_and_choose_tid_p(OUT next_tid_64 BIGINT)
COMMENT '{CHECKSUM}'
BEGIN
  DECLARE committed_tid_64 BIGINT;
  CALL lock_database_for_commit();

    SELECT COALESCE(MAX(tid), 0)
    INTO committed_tid_64
    FROM object_state;

    CALL make_current_tid(next_tid_64);

    IF next_tid_64 <= committed_tid_64 THEN
        SET next_tid_64 = committed_tid_64 + 1;
    END IF;
END;
