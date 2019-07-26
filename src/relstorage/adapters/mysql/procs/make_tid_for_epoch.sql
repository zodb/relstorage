CREATE PROCEDURE make_tid_for_epoch(
      IN unix_ts REAL,
      OUT tid_64 BIGINT)
COMMENT '{CHECKSUM}'
BEGIN
  /*
    Procedure to generate a new 64-bit TID based on the current time.
    Not called from Python, only from SQL.

    We'd really prefer to use the database clock, as there's only one
    of it and it's more likely to be consistent than clocks spread
    across many client machines. Our test cases tend to assume that
    time.time() moves forward at exactly the same speed as the TID
    clock, though, especially if we don't commit anything. This
    doesn't hold true if we don't use the local clock for the TID
    clock.

   */

  DECLARE ts TIMESTAMP;
  DECLARE year, month, day, hour, minute INT;
  DECLARE a1, a, b BIGINT;
  DECLARE b1, second REAL;

  SET ts = FROM_UNIXTIME(unix_ts) + 0.0;

  SET year   = EXTRACT(YEAR from ts),
      month  = EXTRACT(MONTH from ts),
      day    = EXTRACT(DAY from ts),
      hour   = EXTRACT(hour from ts),
      minute = EXTRACT(minute from ts),
      second = unix_ts % 60;


  SET a1 = (((year - 1900) * 12 + month - 1) * 31 + day - 1);
  SET a = (a1 * 24 + hour) *60 + minute;
  -- This is a magic constant; see _timestamp.c
  SET b1 = second / 1.3969838619232178e-08;
  -- CAST(AS INTEGER) rounds, but the C and Python TimeStamp
  -- simply truncate, and we must match them.
  SET b = TRUNCATE(b1, 0);

  SET tid_64 = (a << 32) + b;
END;
