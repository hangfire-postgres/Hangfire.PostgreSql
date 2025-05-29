SET search_path = 'hangfire';

DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 23) THEN
        RAISE EXCEPTION 'version-already-applied';
    END IF;
END $$;

DROP INDEX IF EXISTS ix_hangfire_job_statename_is_not_null;

DO $$
BEGIN
    IF current_setting('server_version_num')::int >= 110000 THEN
        EXECUTE 'CREATE INDEX ix_hangfire_job_statename_is_not_null ON job USING btree(statename) INCLUDE (id) WHERE statename IS NOT NULL';
    ELSE
		CREATE INDEX ix_hangfire_job_statename_is_not_null ON job USING btree(statename) WHERE statename IS NOT NULL;
    END IF;
END $$;

RESET search_path;
