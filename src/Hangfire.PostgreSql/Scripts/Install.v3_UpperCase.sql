DO
$$
    BEGIN
        IF NOT EXISTS(
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = 'hangfire'
            )
        THEN
            EXECUTE 'CREATE SCHEMA "hangfire";';
        END IF;

    END
$$;

SET search_path = 'hangfire';
--
-- Table structure for table `Schema`
--

CREATE TABLE IF NOT EXISTS "SCHEMA"
(
    "VERSION" INT NOT NULL,
    PRIMARY KEY ("VERSION")
);


DO
$$
    BEGIN
        IF EXISTS(SELECT 1 FROM "SCHEMA" WHERE "VERSION"::integer >= 3) THEN
            RAISE EXCEPTION 'version-already-applied';
        END IF;
    END
$$;

INSERT INTO "SCHEMA"("VERSION")
VALUES ('1');

--
-- Table structure for table `Counter`
--

CREATE TABLE IF NOT EXISTS "COUNTER"
(
    "ID"       SERIAL       NOT NULL,
    "KEY"      VARCHAR(100) NOT NULL,
    "VALUE"    SMALLINT     NOT NULL,
    "EXPIREAT" TIMESTAMP    NULL,
    PRIMARY KEY ("ID")
);

DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_COUNTER_KEY" ON "COUNTER" ("KEY");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX ix_hangfire_counter_key already exists.';
        END;
    END;
$$;

--
-- Table structure for table `Hash`
--

CREATE TABLE IF NOT EXISTS "HASH"
(
    "ID"       SERIAL       NOT NULL,
    "KEY"      VARCHAR(100) NOT NULL,
    "FIELD"    VARCHAR(100) NOT NULL,
    "VALUE"    TEXT         NULL,
    "EXPIREAT" TIMESTAMP    NULL,
    PRIMARY KEY ("ID"),
    UNIQUE ("KEY", "FIELD")
);


--
-- Table structure for table `Job`
--

CREATE TABLE IF NOT EXISTS "JOB"
(
    "ID"             SERIAL      NOT NULL,
    "STATEID"        INT         NULL,
    "STATENAME"      VARCHAR(20) NULL,
    "INVOCATIONDATA" TEXT        NOT NULL,
    "ARGUMENTS"      TEXT        NOT NULL,
    "CREATEDAT"      TIMESTAMP   NOT NULL,
    "EXPIREAT"       TIMESTAMP   NULL,
    PRIMARY KEY ("ID")
);

DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_JOB_STATENAME" ON "JOB" ("STATENAME");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_job_statename" already exists.';
        END;
    END;
$$;

--
-- Table structure for table `State`
--

CREATE TABLE IF NOT EXISTS "STATE"
(
    "ID"        SERIAL       NOT NULL,
    "JOBID"     INT          NOT NULL,
    "NAME"      VARCHAR(20)  NOT NULL,
    "REASON"    VARCHAR(100) NULL,
    "CREATEDAT" TIMESTAMP    NOT NULL,
    "DATA"      TEXT         NULL,
    PRIMARY KEY ("ID"),
    FOREIGN KEY ("JOBID") REFERENCES "JOB" ("ID") ON UPDATE CASCADE ON DELETE CASCADE
);

DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_STATE_JOBID" ON "STATE" ("JOBID");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_state_jobid" already exists.';
        END;
    END;
$$;



--
-- Table structure for table `JobQueue`
--

CREATE TABLE IF NOT EXISTS "JOBQUEUE"
(
    "ID"        SERIAL      NOT NULL,
    "JOBID"     INT         NOT NULL,
    "QUEUE"     VARCHAR(20) NOT NULL,
    "FETCHEDAT" TIMESTAMP   NULL,
    PRIMARY KEY ("ID")
);

DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_JOBQUEUE_QUEUEANDFETCHEDAT" ON "JOBQUEUE" ("QUEUE", "FETCHEDAT");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_jobqueue_queueandfetchedat" already exists.';
        END;
    END;
$$;


--
-- Table structure for table `List`
--

CREATE TABLE IF NOT EXISTS "LIST"
(
    "ID"       SERIAL       NOT NULL,
    "KEY"      VARCHAR(100) NOT NULL,
    "VALUE"    TEXT         NULL,
    "EXPIREAT" TIMESTAMP    NULL,
    PRIMARY KEY ("ID")
);


--
-- Table structure for table `Server`
--

CREATE TABLE IF NOT EXISTS "SERVER"
(
    "ID"            VARCHAR(50) NOT NULL,
    "DATA"          TEXT        NULL,
    "LASTHEARTBEAT" TIMESTAMP   NOT NULL,
    PRIMARY KEY ("ID")
);


--
-- Table structure for table `Set`
--

CREATE TABLE IF NOT EXISTS "SET"
(
    "ID"       SERIAL       NOT NULL,
    "KEY"      VARCHAR(100) NOT NULL,
    "SCORE"    FLOAT8       NOT NULL,
    "VALUE"    TEXT         NOT NULL,
    "EXPIREAT" TIMESTAMP    NULL,
    PRIMARY KEY ("ID"),
    UNIQUE ("KEY", "VALUE")
);


--
-- Table structure for table `JobParameter`
--

CREATE TABLE IF NOT EXISTS "JOBPARAMETER"
(
    "ID"    SERIAL      NOT NULL,
    "JOBID" INT         NOT NULL,
    "NAME"  VARCHAR(40) NOT NULL,
    "VALUE" TEXT        NULL,
    PRIMARY KEY ("ID"),
    FOREIGN KEY ("JOBID") REFERENCES "JOB" ("ID") ON UPDATE CASCADE ON DELETE CASCADE
);

DO
$$
    BEGIN
        BEGIN
            CREATE INDEX "IX_HANGFIRE_JOBPARAMETER_JOBIDANDNAME" ON "JOBPARAMETER" ("JOBID", "NAME");
        EXCEPTION
            WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_jobparameter_jobidandname" already exists.';
        END;
    END;
$$;

CREATE TABLE IF NOT EXISTS "LOCK"
(
    "RESOURCE" VARCHAR(100) NOT NULL,
    UNIQUE ("RESOURCE")
);

RESET search_path;