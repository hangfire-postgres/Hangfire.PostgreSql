SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 11) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE "counter"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "hash"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "job"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "job"
    ALTER COLUMN stateid TYPE BIGINT;
ALTER TABLE "state"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "state"
    ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE "jobparameter"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "jobparameter"
    ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE "jobqueue"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "jobqueue"
    ALTER COLUMN jobid TYPE BIGINT;
ALTER TABLE "list"
    ALTER COLUMN id TYPE BIGINT;
ALTER TABLE "set"
    ALTER COLUMN id TYPE BIGINT;

RESET search_path;