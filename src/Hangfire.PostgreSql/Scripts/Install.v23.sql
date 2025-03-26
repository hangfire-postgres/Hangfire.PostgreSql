SET search_path = 'hangfire';

DO $$
DECLARE v_version INTEGER;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'schema' AND column_name = 'version'
    ) THEN
        SELECT COALESCE(MAX("version")::integer, 0) INTO v_version FROM "schema";

        IF v_version >= 23 THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END IF;
END $$;

DROP INDEX IF EXISTS ix_hangfire_job_statename_is_not_null;

DO $$
BEGIN
    BEGIN
        EXECUTE 'CREATE INDEX ix_hangfire_job_statename_is_not_null ON job USING btree(statename) INCLUDE (id) WHERE statename IS NOT NULL';
    EXCEPTION
        WHEN others THEN
            EXECUTE 'CREATE INDEX ix_hangfire_job_statename_is_not_null ON job USING btree(statename) WHERE statename IS NOT NULL';
    END;
END $$;

RESET search_path;
