SET search_path = 'hangfire';
--
-- Adds indices, greatly speeds-up deleting old jobs.
--

DO
$$
    BEGIN
           IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 6) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;


DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_COUNTER_EXPIREAT" ON "COUNTER" ("EXPIREAT");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX ix_hangfire_counter_expireat already exists.';
        END;
    END;
$$;

DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_JOBQUEUE_JOBIDANDQUEUE" ON "JOBQUEUE" ("JOBID", "QUEUE");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_jobqueue_jobidandqueue" already exists.';
        END;
    END;
$$;

RESET search_path;