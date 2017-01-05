SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

DO
$$
BEGIN
    IF EXISTS (SELECT 1 FROM "schema" WHERE "version"::integer >= 4) THEN
        RAISE EXCEPTION 'version-already-applied';
    END IF;
END
$$;

ALTER TABLE "counter" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "lock" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "hash" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "job" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "jobparameter" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "jobqueue" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "list" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "server" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "set" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
ALTER TABLE "state" ADD COLUMN "updatecount" integer NOT NULL DEFAULT 0;
