CREATE OR REPLACE FUNCTION make_tid_for_epoch(unix_ts double precision)
  RETURNS BIGINT
AS
$$
DECLARE
  ts TIMESTAMP;
  year INT;
  month INT;
  day INT;
  hour INT;
  minute INT;
  a1 BIGINT;
  a BIGINT;
  b BIGINT;
  tid_64 BIGINT;
  b1 double precision;
  second double precision;
BEGIN
  ts := timezone('UTC', to_timestamp(unix_ts));

  year   := EXTRACT(YEAR from ts);
  month  := EXTRACT(MONTH from ts);
  day    := EXTRACT(DAY from ts);
  hour   := EXTRACT(hour from ts);
  minute := EXTRACT(minute from ts);
  -- The MOD function and % operator are not defined for
  -- double precision, and EXTRACT(second from TS) doesn't
  -- keep faith with the C and Python impls that use doubles
  -- and invent precision.
  -- second := mod(unix_ts, 60.0::double precision);
  -- second := EXTRACT(second from ts);
  -- So we're left to do it ourself.
  second = unix_ts - floor(unix_ts / 60.0) * 60.0;


  a1 := (((year - 1900) * 12 + month - 1) * 31 + day - 1);
  a := (a1 * 24 + hour) *60 + minute;
  -- This is a magic constant; see _timestamp.c
  b1 := second / 1.3969838619232178e-08::numeric;
  -- CAST(AS INTEGER) rounds, but the C and Python TimeStamp
  -- simply truncate, and we must match them.
  b := TRUNC(b1);

  -- RAISE NOTICE 'year % month % day % hour % minute % second % ts %', year, month, day, hour, minute, second, ts;
  -- RAISE NOTICE 'a1 %, a %, b1 %, b %', a1, a, b1, b;
  tid_64 := (a << 32) + b;
  RETURN tid_64;
END;
$$
LANGUAGE plpgsql IMMUTABLE;
