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
  DECLARE year, month, day, hour INT;
  -- MySQL 5.7 and probably 8 has a bug: it wants to get the minute
  -- value from a timestamp by *rounding* it following its usual
  -- rules, instead of truncating it (truncation is correct: it
  -- represents "the number of whole minutes past the hour, 0 - 59").
  -- So instead of getting 29 for XX:29:59, we get 30. Needless to say
  -- that messes up computations that happen in the last 0.5 seconds
  -- of the minute (they jump ahead by 60 seconds, aka 4,294,967,296
  -- in the generated int). Therefore we have to manually adjust for
  -- this. (No combination of datatypes for the field, EXTRACT() vs
  -- MINUTE() made a difference.)
  DECLARE tm_min INT;
  DECLARE a1, a, b BIGINT;
  DECLARE b1, tm_sec REAL;

  SET ts = FROM_UNIXTIME(unix_ts) + 0.0;

  SET year   = EXTRACT(YEAR from ts),
      month  = EXTRACT(MONTH from ts),
      day    = EXTRACT(DAY from ts),
      hour   = EXTRACT(hour from ts),
      tm_min = EXTRACT(MINUTE from ts),
      tm_sec = MOD(unix_ts, 60.0);

  IF tm_sec >= 59.5 THEN
    SET tm_min = tm_min - 1;
  END IF;

  SET a1 = (((year - 1900) * 12 + month - 1) * 31 + day - 1);
  SET a = (a1 * 24 + hour) *60 + tm_min;
  -- This is a magic constant; see _timestamp.c
  SET b1 = tm_sec / 1.3969838619232178e-08;
  -- CAST(AS INTEGER) rounds, but the C and Python TimeStamp
  -- simply truncate, and we must match them.
  SET b = TRUNCATE(b1, 0);

  SET tid_64 = (a << 32) + b;

END;
