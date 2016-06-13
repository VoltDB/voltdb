--
-- The following four tables are used to test insert and select of
-- min, max, 0, NULL and some normal values of each SQL type. These
-- tables are not designed to test any partitioning effects.
--

-- Attribute for ea. supported type: nulls allowed
CREATE TABLE importTable (
 PKEY          BIGINT       NOT NULL,
 A_INTEGER_VALUE  BIGINT
);

create table importCSVTable (
      clm_integer integer not null,
      clm_tinyint tinyint default 0,
      clm_smallint smallint default 0,
      clm_bigint bigint default 0,
      clm_string varchar(20) default null,
      clm_decimal decimal default null,
      clm_float float default null,
      clm_timestamp timestamp default null,
      clm_point geography_point default null,
      clm_geography geography default null
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
