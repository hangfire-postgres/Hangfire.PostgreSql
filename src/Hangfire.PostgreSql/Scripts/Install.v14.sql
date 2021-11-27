SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 14) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

do
$$
    DECLARE
    BEGIN
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".job_id_seq AS bigint MAXVALUE 9223372036854775807');
    EXCEPTION
        WHEN syntax_error THEN
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".job_id_seq MAXVALUE 9223372036854775807');
    END;
$$;

RESET search_path;
