SET search_path = 'hangfire';



DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 13) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

CREATE INDEX IF NOT EXISTS jobqueue_queue_fetchat_jobId ON jobqueue USING btree (queue asc, fetchedat asc nulls last, jobid asc);

RESET search_path;