SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 16) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE counter ALTER COLUMN expireat TYPE timestamptz;
ALTER TABLE hash ALTER COLUMN expireat TYPE timestamptz;
ALTER TABLE job ALTER COLUMN createdat TYPE timestamptz;
ALTER TABLE job ALTER COLUMN expireat TYPE timestamptz;
ALTER TABLE jobqueue ALTER COLUMN fetchedat TYPE timestamptz;
ALTER TABLE list ALTER COLUMN expireat TYPE timestamptz;
ALTER TABLE lock ALTER COLUMN acquired TYPE timestamptz;
ALTER TABLE server ALTER COLUMN lastheartbeat TYPE timestamptz;
ALTER TABLE "set" ALTER COLUMN expireat TYPE timestamptz;
ALTER TABLE state ALTER COLUMN createdat TYPE timestamptz;

RESET search_path;
