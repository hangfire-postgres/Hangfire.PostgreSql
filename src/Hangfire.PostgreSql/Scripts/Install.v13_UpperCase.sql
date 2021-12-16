SET search_path = 'hangfire';



DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 13) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

CREATE INDEX IF NOT EXISTS "JOBQUEUE_QUEUE_FETCHAT_JOBIDJOBQUEUE_QUEUE_FETCHAT_JOBID" ON "JOBQUEUE" USING btree ("QUEUE" asc, "FETCHEDAT" asc nulls last, "JOBID" asc);

RESET search_path;