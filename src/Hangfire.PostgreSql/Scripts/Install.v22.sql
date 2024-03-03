SET search_path = 'hangfire';

DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 22) THEN
        RAISE EXCEPTION 'version-already-applied';
END IF;
END $$;

DROP INDEX IF EXISTS jobqueue_queue_fetchat_jobId;
CREATE INDEX IF NOT EXISTS ix_hangfire_jobqueue_fetchedat_queue_jobid ON jobqueue USING btree (fetchedat nulls first, queue, jobid);

RESET search_path;
