SET search_path = 'hangfire';

DELETE FROM hangfire."aggregatedcounter";
DELETE FROM hangfire."counter";
DELETE FROM hangfire."hash";
DELETE FROM hangfire."job";
DELETE FROM hangfire."jobparameter";
DELETE FROM hangfire."jobqueue";
DELETE FROM hangfire."list";
DELETE FROM hangfire."lock";
DELETE FROM hangfire."server";
DELETE FROM hangfire."set";
DELETE FROM hangfire."state";