SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 9) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE "LOCK"
    ALTER COLUMN "RESOURCE" TYPE TEXT;

RESET search_path;