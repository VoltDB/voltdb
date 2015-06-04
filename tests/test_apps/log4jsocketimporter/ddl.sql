-- log_events table holds all log events
CREATE TABLE log_events
(
  log_event_host    varchar(256) NOT NULL
, logger_name       varchar(256) NOT NULL
, log_level         varchar(25)  NOT NULL
, logging_thread    varchar(25)  NOT NULL
, log_timestamp     timestamp    NOT NULL
, log_message       varchar(1024)
, throwable_str_rep varchar(4096)
);

PARTITION TABLE log_events ON COLUMN log_event_host;

LOAD CLASSES log4jsocketimporter-client.jar;
CREATE PROCEDURE FROM CLASS log4jsocketimporter.FetchLogRowsProcedure;
