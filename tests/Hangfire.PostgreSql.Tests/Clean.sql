﻿SET search_path = 'hangfire';

delete from hangfire."counter";
delete from hangfire."hash";
delete from hangfire."job";
delete from hangfire."jobparameter";
delete from hangfire."jobqueue";
delete from hangfire."list";
delete from hangfire."lock";
delete from hangfire."server";
delete from hangfire."set";
delete from hangfire."state";