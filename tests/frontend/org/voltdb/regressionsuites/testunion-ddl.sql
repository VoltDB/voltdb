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
, state2             varchar(2)   NOT NULL
, state15            varchar(15)   NOT NULL
, state16            varchar(16)   NOT NULL
, state63            varchar(63)   NOT NULL
, state64            varchar(64)   NOT NULL
, state100           varchar(100) NOT NULL
, state2_b           varchar(2 bytes) NOT NULL
, state15_b          varchar(15 bytes) NOT NULL
, state16_b          varchar(16 bytes) NOT NULL
, state63_b          varchar(63 bytes) NOT NULL
, state64_b          varchar(64 bytes) NOT NULL
, state100_b         varchar(100 bytes) NOT NULL
, binary2            varbinary(2) NOT NULL
, binary15           varbinary(15) NOT NULL
, binary16           varbinary(16) NOT NULL
, binary63           varbinary(63) NOT NULL
, binary64           varbinary(64) NOT NULL
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

CREATE TABLE T0_ENG_12941 (
       STR VARCHAR(32) PRIMARY KEY NOT NULL,
       V BIGINT
);

CREATE TABLE T1_ENG_12941 (
       STR VARCHAR(32) PRIMARY KEY NOT NULL,
       V BIGINT
);

CREATE TABLE T0_ENG_13536 (
    id  integer
);
