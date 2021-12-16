SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 10) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE "JOBQUEUE"
    ALTER COLUMN "QUEUE" TYPE TEXT;

RESET search_path;