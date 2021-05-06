SET search_path = 'hangfire';



DO
$$
BEGIN
  IF EXISTS (SELECT 1 FROM "schema" WHERE "version"::integer >= 14) THEN
    RAISE EXCEPTION 'version-already-applied';
  END IF;
END
$$;

ALTER SEQUENCE job_id_seq as bigint;

RESET search_path;