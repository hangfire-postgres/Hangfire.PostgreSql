SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 18) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

CREATE TABLE aggregatedcounter (
    "id" bigserial PRIMARY KEY NOT NULL,
    "key" text NOT NULL UNIQUE,
    "value" int8 NOT NULL,
    "expireat" timestamp
);

RESET search_path;
