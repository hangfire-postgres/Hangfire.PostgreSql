SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 8) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE "COUNTER"
    ALTER COLUMN "VALUE" TYPE bigint;
ALTER TABLE "COUNTER"
    DROP COLUMN "UPDATECOUNT" RESTRICT;

RESET search_path;