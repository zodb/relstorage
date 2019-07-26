CREATE OR REPLACE FUNCTION lock_and_choose_tid(
    p_packed BOOLEAN,
    p_username BYTEA,
    p_description BYTEA,
    p_extension BYTEA
)
RETURNS BIGINT
AS
$$
  SELECT lock_and_choose_tid_p(
    p_packed,
    p_username,
    p_description,
    p_extension
  );
$$
LANGUAGE SQL;
