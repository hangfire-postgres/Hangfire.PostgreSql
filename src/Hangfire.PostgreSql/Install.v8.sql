SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM "schema" WHERE "version"::integer >= 8) THEN
        RAISE EXCEPTION 'version-already-applied';
    END IF;
END
$$;
DROP INDEX "ix_hangfire_counter_expireat";
ALTER TABLE "counter" ALTER COLUMN value TYPE integer;
ALTER TABLE "counter" DROP COLUMN expireat RESTRICT;
ALTER TABLE "counter" DROP COLUMN updatecount RESTRICT;
