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
  DECLARE next_oid BIGINT;

  SELECT COALESCE(MAX(ZOID), 0)
  INTO next_oid
  FROM new_oid;

  IF next_oid < min_oid THEN
    -- Can't say for sure. Just because we can only see values
    -- less doesn't mean they're not there in another transaction.

    -- This will never block.
    INSERT INTO new_oid VALUES ();
    SELECT LAST_INSERT_ID()
    INTO next_oid;

    IF min_oid > next_oid THEN
      -- This is unlikely to block. We just confirmed that the
      -- sequence value is strictly less than this, so no one else
      -- should be doing this.
      INSERT IGNORE INTO new_oid (zoid)
      VALUES (min_oid);

      SET next_oid = min_oid;
    END IF;
  ELSE
    -- Return a NULL value to signal that this value cannot
    -- be cached and used because we didn't allocate it.
    SET next_oid = NULL;
  END IF;

  SELECT next_oid;
END;
