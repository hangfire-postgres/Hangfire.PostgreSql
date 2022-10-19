SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 18) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

create table aggregatedcounter (
    "id" bigserial primary key not null,
    "key" text not null unique,
    "value" int8 not null,
    "expireat" timestamp
);

RESET search_path;
