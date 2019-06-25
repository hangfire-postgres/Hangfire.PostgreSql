SET search_path = 'hangfire';



DO
$$
BEGIN
  IF EXISTS (SELECT 1 FROM "schema" WHERE "version"::integer >= 11) THEN
    RAISE EXCEPTION 'version-already-applied';
  END IF;
END
$$;

ALTER TABLE hangfire.counter ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.counter_id_seq AS BIGINT; 
ALTER TABLE hangfire.hash ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.hash_id_seq AS BIGINT;
ALTER TABLE hangfire.job ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.job_id_seq AS BIGINT;
ALTER TABLE hangfire.job ALTER COLUMN stateid TYPE BIGINT;
ALTER TABLE hangfire.state ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.state_id_seq AS BIGINT;
ALTER TABLE hangfire.state ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE hangfire.jobparameter ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.jobparameter_id_seq AS BIGINT;
ALTER TABLE hangfire.jobparameter ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE hangfire.jobqueue ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.jobqueue_id_seq AS BIGINT;
ALTER TABLE hangfire.jobqueue ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE hangfire.list ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.list_id_seq AS BIGINT;
ALTER TABLE hangfire.set ALTER COLUMN id TYPE BIGINT;
ALTER SEQUENCE hangfire.set_id_seq AS BIGINT;
