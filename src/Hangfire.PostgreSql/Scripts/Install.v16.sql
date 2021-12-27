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
ALTER SEQUENCE counter_id_seq AS bigint;
ALTER SEQUENCE hash_id_seq AS bigint;
ALTER SEQUENCE jobparameter_id_seq AS bigint;
ALTER SEQUENCE jobqueue_id_seq AS bigint;
ALTER SEQUENCE list_id_seq AS bigint;
ALTER SEQUENCE set_id_seq AS bigint;
ALTER SEQUENCE state_id_seq AS bigint;

RESET search_path;
