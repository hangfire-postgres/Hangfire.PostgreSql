SET search_path = 'hangfire';

DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 20) THEN
        RAISE EXCEPTION 'version-already-applied';
END IF;
END $$;

-- Update existing jobs, if any have empty values first
UPDATE "job" SET "invocationdata" = '{}' WHERE "invocationdata" = '';
UPDATE "job" SET "arguments" = '[]' WHERE "arguments" = '';

-- Change the type

ALTER TABLE "job" ALTER COLUMN "invocationdata" TYPE jsonb USING "invocationdata"::jsonb;
ALTER TABLE "job" ALTER COLUMN "arguments" TYPE jsonb USING "arguments"::jsonb;
ALTER TABLE "server" ALTER COLUMN "data" TYPE jsonb USING "data"::jsonb;
ALTER TABLE "state" ALTER COLUMN "data" TYPE jsonb USING "data"::jsonb;

RESET search_path;
