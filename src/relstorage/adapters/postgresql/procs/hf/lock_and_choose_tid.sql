CREATE OR REPLACE FUNCTION lock_and_choose_tid()
RETURNS BIGINT
AS
$$
  SELECT lock_and_choose_tid_p();
$$
LANGUAGE SQL;
