CREATE TABLE "DJANGO_CELERY_RESULTS_TASK0CC3" ("ID" NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY,
"TASK_ID" NVARCHAR2(255) NULL UNIQUE, "STATUS" NVARCHAR2(50) NULL, "CONTENT_TYPE" NVARCHAR2(128) NULL,
"TASK_ARGS" NCLOB NULL, "TASK_KWARGS" NCLOB NULL, "TASK_NAME" NVARCHAR2(255) NULL, "WORKER" NVARCHAR2(100) NULL,
"CONTENT_ENCODING" NVARCHAR2(64) NULL, "RESULT" NCLOB NULL, "DATE_DONE" TIMESTAMP NOT NULL,
"DATE_CREATED" TIMESTAMP  NOT NULL,
"TRACEBACK" NCLOB NULL, "META" NCLOB NULL);
CREATE INDEX "DJANGO_CEL_HIDDEN_CD77412F" ON "DJANGO_CELERY_RESULTS_TASK0CC3" ("HIDDEN");
CREATE INDEX "DJANGO_CEL_WORKER_F8711389" ON "DJANGO_CELERY_RESULTS_TASK0CC3" ("WORKER");
CREATE INDEX "DJANGO_CEL_DATE_CREAT_099F3424" ON "DJANGO_CELERY_RESULTS_TASK0CC3" ("DATE_CREATED");

--
-- Create model CrontabSchedule
--
CREATE TABLE "DJANGO_CELERY_BEAT_CRONTAB0BF9" ("ID" NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY,
"MINUTE" NVARCHAR2(240) NULL, "HOUR" NVARCHAR2(96) NULL, "DAY_OF_WEEK" NVARCHAR2(64) NULL,
"DAY_OF_MONTH" NVARCHAR2(124) NULL, "MONTH_OF_YEAR" NVARCHAR2(64) NULL, "TIMEZONE" NVARCHAR2(63) DEFAULT NULL);
--
-- Create model IntervalSchedule
--
CREATE TABLE "DJANGO_CELERY_BEAT_INTERVA94E2" ("ID" NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY,
"EVERY" NUMBER(11) NOT NULL, "PERIOD" NVARCHAR2(24) NULL);
--
-- Create model PeriodicTask
--
CREATE TABLE "DJANGO_CELERY_BEAT_PERIODI8E4A" ("ID" NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY,
"NAME" NVARCHAR2(200) NULL UNIQUE, "TASK" NVARCHAR2(200) NULL, "ARGS" NCLOB NULL, "KWARGS" NCLOB NULL, "QUEUE" NVARCHAR2(200) NULL,
"EXCHANGE" NVARCHAR2(200) NULL, "ROUTING_KEY" NVARCHAR2(200) NULL, "EXPIRES" TIMESTAMP NULL, "ENABLED" NUMBER(1) NOT NULL CHECK ("ENABLED" IN (0,1)),
 "LAST_RUN_AT" TIMESTAMP NULL, "TOTAL_RUN_COUNT" NUMBER(11) NOT NULL CHECK ("TOTAL_RUN_COUNT" >= 0), "DATE_CHANGED" TIMESTAMP NOT NULL,
 "DESCRIPTION" NCLOB NULL, "CRONTAB_ID" NUMBER(11) NULL, "INTERVAL_ID" NUMBER(11) NULL, "SOLAR_ID" NUMBER(11) NULL,
 "CLOCKED_ID" NUMBER(11) NULL, "EXPIRE_SECONDS" NUMBER(11) NULL,"ONE_OFF" NUMBER(1)  NULL, "START_TIME" TIMESTAMP NULL,  "HEADERS" NCLOB NUL);
--
-- Create model PeriodicTasks
--

ALTER TABLE "DJANGO_CELERY_BEAT_PERIODI8E4A" ADD CONSTRAINT "DJANGO_CE_CRONTAB_I_D3CBA168_F" FOREIGN KEY ("CRONTAB_ID") REFERENCES "DJANGO_CELERY_BEAT_CRONTAB0BF9" ("ID") DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE "DJANGO_CELERY_BEAT_PERIODI8E4A" ADD CONSTRAINT "DJANGO_CE_INTERVAL__A8CA27DA_F" FOREIGN KEY ("INTERVAL_ID") REFERENCES "DJANGO_CELERY_BEAT_INTERVA94E2" ("ID") DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX "DJANGO_CEL_CRONTAB_ID_D3CBA168" ON "DJANGO_CELERY_BEAT_PERIODI8E4A" ("CRONTAB_ID");
CREATE INDEX "DJANGO_CEL_INTERVAL_I_A8CA27DA" ON "DJANGO_CELERY_BEAT_PERIODI8E4A" ("INTERVAL_ID");

CREATE TABLE "DJANGO_CELERY_BEAT_SOLARSCF323" ("ID" NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY,
"EVENT" NVARCHAR2(24) NULL, "LATITUDE" NUMBER(9, 6) NOT NULL, "LONGITUDE" NUMBER(9, 6) NOT NULL);
CREATE INDEX "DJANGO_CEL_SOLAR_ID_A87CE72C" ON "DJANGO_CELERY_BEAT_PERIODI8E4A" ("SOLAR_ID");
ALTER TABLE "DJANGO_CELERY_BEAT_PERIODI8E4A" ADD CONSTRAINT "DJANGO_CE_SOLAR_ID_A87CE72C_F" FOREIGN KEY ("SOLAR_ID") REFERENCES "DJANGO_CELERY_BEAT_SOLARSCF323" ("ID") DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE "DJANGO_CELERY_BEAT_SOLARSCF323" ADD CONSTRAINT "DJANGO_CE_EVENT_LAT_BA64999A_U" UNIQUE ("EVENT", "LATITUDE", "LONGITUDE");

CREATE TABLE "DJANGO_CELERY_BEAT_CLOCKED1760" ("ID" NUMBER(11) GENERATED BY DEFAULT ON NULL AS IDENTITY NOT NULL PRIMARY KEY,
"CLOCKED_TIME" TIMESTAMP NOT NULL, "ENABLED" NUMBER(1) NOT NULL CHECK ("ENABLED" IN (0,1)));

CREATE INDEX "DJANGO_CEL_CLOCKED_ID_47A69F82" ON "DJANGO_CELERY_BEAT_PERIODI8E4A" ("CLOCKED_ID");
ALTER TABLE "DJANGO_CELERY_BEAT_PERIODI8E4A" ADD CONSTRAINT "DJANGO_CE_CLOCKED_I_47A69F82_F" FOREIGN KEY ("CLOCKED_ID") REFERENCES "DJANGO_CELERY_BEAT_CLOCKED1760" ("ID") DEFERRABLE INITIALLY DEFERRED;
