SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM "schema" WHERE "version" = '5') THEN
        RAISE EXCEPTION 'version-already-applied';
    END IF;
END
$$;

UPDATE "schema" SET "version" = '5' WHERE "version" = '4';

ALTER TABLE server ALTER COLUMN id TYPE varchar(64);
