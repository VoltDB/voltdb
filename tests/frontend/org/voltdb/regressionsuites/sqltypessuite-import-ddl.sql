--
-- The following four tables are used to test insert and select of
-- min, max, 0, NULL and some normal values of each SQL type. These
-- tables are not designed to test any partitioning effects.
--

-- Attribute for ea. supported type: nulls allowed
CREATE TABLE importTable (
 PKEY          BIGINT       NOT NULL,
 A_INTEGER_VALUE     BIGINT,
);

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
