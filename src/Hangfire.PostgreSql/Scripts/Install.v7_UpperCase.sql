SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 7) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

ALTER TABLE "LOCK"
    ADD COLUMN "ACQUIRED" timestamp without time zone;

RESET search_path;