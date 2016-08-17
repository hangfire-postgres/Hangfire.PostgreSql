SET search_path = 'hangfire';
--
-- Adds indices, greatly speeds-up deleting old jobs.
--

DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM "schema" WHERE "version"::integer >= 6) THEN
        RAISE EXCEPTION 'version-already-applied';
    END IF;
END
$$;


DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_counter_expireat" ON "counter" ("expireat");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX ix_hangfire_counter_expireat already exists.';
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_jobqueue_jobidandqueue" ON "jobqueue" ("jobid","queue");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_jobqueue_jobidandqueue" already exists.';
    END;
END;
$$;

