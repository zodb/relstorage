CREATE PROCEDURE set_min_oid(min_oid BIGINT)
COMMENT '{CHECKSUM}'
BEGIN
  -- Set the current allowed minimum OID in a single trip to the
  -- server.

  -- In order to avoid deadlocks, we only do this if
  -- the number we want to insert is strictly greater than
  -- what the current sequence value is. If we use a value less
  -- than that, there's a chance a different session has already allocated
  -- and inserted that value into the table, meaning its locked.
  -- We obviously cannot JUST use MAX(zoid) to find this value, we can't see
  -- what other sessions have done. But if that's already >= to the min_oid,
  -- then we don't have to do anything.
  DECLARE current_max_oid BIGINT;

  SELECT COALESCE(MAX(ZOID), 0)
  INTO current_max_oid
  FROM new_oid;

  IF current_max_oid >= min_oid THEN
    -- Sweet. The values visible to us (already committed at the
    -- time we opened this transaction) are in advance of
    -- the requested minimum.
    SET current_max_oid = NULL;
  ELSE
    -- Can't say for sure. Just because we can only see values
    -- less doesn't mean they're not there in another transaction.

    -- So we find out what the next allocation would be.
    -- This will never block.
    INSERT INTO new_oid VALUES ();

    SELECT LAST_INSERT_ID()
    INTO current_max_oid;

    -- If we still need to go higher, then we have no choice but to do
    -- an insert. This is /unlikely/ to block or conflict. We just
    -- confirmed that the sequence value is strictly less than this,
    -- so no one else should be doing this *unless* two threads are
    -- trying to copy transactions in at the same time and happened to
    -- hit the same OID value. That's likely to fail at higher levels, but
    -- perhaps it's possible. That means that our IGNORE modifier here could
    -- let one thread insert data and the other pretend to: but either way, it means
    -- it is not safe for us to assume we allocated this value and try to
    -- generate more OIDs with it. (There is a warning issued in that case:
    -- "Warning (Code 1062): Duplicate entry '123456' for key 'PRIMARY'"; can we detect
    -- and deal with that?)
    IF min_oid > current_max_oid THEN
      INSERT IGNORE INTO new_oid (zoid)
      VALUES (min_oid);
    END IF;
  END IF;


END;
