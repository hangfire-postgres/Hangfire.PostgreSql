SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 5) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE "SERVER"
    ALTER COLUMN "ID" TYPE VARCHAR(100);

RESET search_path;