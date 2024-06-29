SET search_path = 'hangfire';

DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 23) THEN
        RAISE EXCEPTION 'version-already-applied';
END IF;
END $$;

DROP INDEX IF EXISTS ix_hangfire_job_statename_is_not_null;
CREATE INDEX ix_hangfire_job_statename_is_not_null ON job USING btree(statename) INCLUDE (id) WHERE statename IS NOT NULL;

RESET search_path;
