SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 17) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

CREATE INDEX IF NOT EXISTS ix_hangfire_set_key_score ON "set" (key, score);

RESET search_path;
