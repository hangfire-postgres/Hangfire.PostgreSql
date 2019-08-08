SET search_path = 'hangfire';



DO
$$
BEGIN
  IF EXISTS (SELECT 1 FROM "schema" WHERE "version"::integer >= 12) THEN
    RAISE EXCEPTION 'version-already-applied';
  END IF;
END
$$;

ALTER TABLE hangfire.counter ALTER COLUMN "key" TYPE TEXT;
ALTER TABLE hangfire.hash ALTER COLUMN "key" TYPE TEXT;
ALTER TABLE hangfire.hash ALTER COLUMN field TYPE TEXT;
ALTER TABLE hangfire.job ALTER COLUMN statename TYPE TEXT;
ALTER TABLE hangfire.list ALTER COLUMN "key" TYPE TEXT;
ALTER TABLE hangfire.set ALTER COLUMN "key" TYPE TEXT;
ALTER TABLE hangfire.jobparameter ALTER COLUMN "name" TYPE TEXT;
ALTER TABLE hangfire.state ALTER COLUMN "name" TYPE TEXT;
ALTER TABLE hangfire.state ALTER COLUMN reason TYPE TEXT;
