CREATE TABLE A (
 PKEY          INTEGER NOT NULL,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);
PARTITION TABLE A ON COLUMN PKEY;

CREATE TABLE B (
 PKEY          INTEGER,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);

CREATE TABLE C (
 PKEY          INTEGER,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);

CREATE TABLE D (
 PKEY          INTEGER,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);

CREATE TABLE RPT_P (
   CLIENT_ID INTEGER NOT NULL,
   CONFIG_ID INTEGER,
   COST DECIMAL
);
PARTITION TABLE RPT_P ON COLUMN CLIENT_ID;

CREATE TABLE RPT_COPY_P (
   CLIENT_ID integer NOT NULL,
   CONFIG_ID integer,
   COST decimal
);
PARTITION TABLE RPT_COPY_P ON COLUMN CLIENT_ID;

CREATE PROCEDURE testunion_p PARTITION ON TABLE RPT_P COLUMN client_id PARAMETER 0 AS
select client_id, config_id, sum(cost) as cost
from RPT_P 
where client_id=?
group by client_id, config_id
UNION
select client_id, config_id, sum(cost) as cost
from rpt_copy_p
where client_id=?
group by client_id, config_id;


-- ENG-6291: UNION with inline/non-inline
CREATE TABLE my_votes
(
  phone_number       bigint       NOT NULL
, state              varchar(2)   NOT NULL
, state100           varchar(100) NOT NULL
, state_b            varchar(2 bytes) NOT NULL
, state100_b         varchar(100 bytes) NOT NULL
, binary2            varbinary(2) NOT NULL
, binary100          varbinary(100) NOT NULL
);

CREATE TABLE area_code_state
(
  area_code smallint   NOT NULL
, state     varchar(2) NOT NULL
, CONSTRAINT PK_area_code_state PRIMARY KEY
  (
    area_code
  )
);