SET search_path = 'hangfire';

DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 19) THEN
        RAISE EXCEPTION 'version-already-applied';
END IF;
END $$;

ALTER TABLE "aggregatedcounter" ALTER COLUMN "expireat" TYPE timestamp with time zone;
ALTER TABLE "counter" ALTER COLUMN "expireat" TYPE timestamp with time zone;
ALTER TABLE "hash" ALTER COLUMN "expireat" TYPE timestamp with time zone;
ALTER TABLE "job" ALTER COLUMN "createdat" TYPE timestamp with time zone;
ALTER TABLE "job" ALTER COLUMN "expireat" TYPE timestamp with time zone;
ALTER TABLE "jobqueue" ALTER COLUMN "fetchedat" TYPE timestamp with time zone;
ALTER TABLE "list" ALTER COLUMN "expireat" TYPE timestamp with time zone;
ALTER TABLE "lock" ALTER COLUMN "acquired" TYPE timestamp with time zone;
ALTER TABLE "server" ALTER COLUMN "lastheartbeat" TYPE timestamp with time zone;
ALTER TABLE "set" ALTER COLUMN "expireat" TYPE timestamp with time zone;
ALTER TABLE "state" ALTER COLUMN "createdat" TYPE timestamp with time zone;

RESET search_path;
