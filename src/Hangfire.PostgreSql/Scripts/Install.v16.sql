SET search_path = 'hangfire';

DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "schema" WHERE "version"::integer >= 16) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

-- Note: job_id_seq is already bigint as per migration script v14
DO
$$
    DECLARE
    BEGIN
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".counter_id_seq AS bigint MAXVALUE 9223372036854775807');
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".hash_id_seq AS bigint MAXVALUE 9223372036854775807');
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".jobparameter_id_seq AS bigint MAXVALUE 9223372036854775807');
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".jobqueue_id_seq AS bigint MAXVALUE 9223372036854775807');
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".list_id_seq AS bigint MAXVALUE 9223372036854775807');
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".set_id_seq AS bigint MAXVALUE 9223372036854775807');
        EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".state_id_seq AS bigint MAXVALUE 9223372036854775807');
    EXCEPTION
        WHEN syntax_error THEN
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".counter_id_seq MAXVALUE 9223372036854775807');
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".hash_id_seq MAXVALUE 9223372036854775807');
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".jobparameter_id_seq MAXVALUE 9223372036854775807');
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".jobqueue_id_seq MAXVALUE 9223372036854775807');
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".list_id_seq MAXVALUE 9223372036854775807');
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".set_id_seq MAXVALUE 9223372036854775807');
            EXECUTE ('ALTER SEQUENCE "' || 'hangfire' || '".state_id_seq MAXVALUE 9223372036854775807');
    END
$$;

RESET search_path;
