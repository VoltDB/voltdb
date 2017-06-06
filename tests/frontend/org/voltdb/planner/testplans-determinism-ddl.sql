create table tonecolumn (
  a bigint not null primary key
);

create table ttree (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create index cover2_TREE on ttree (a, b);
create index cover3_TREE on ttree (a, c, b);

create table ttree_with_key (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  d bigint not null,
  e bigint not null,
  id bigint not null,
  primary key (a, b, c)
);

create table thash (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create index cover2 on thash (a, b);
create index cover3 on thash (a, c, b);

create table tunique (
  a bigint not null unique,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create table tuniqcombo (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

create unique index cover3_UNIQCOMBO on tuniqcombo (a, c, b);

create table tpk (
  a bigint not null primary key,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

CREATE TABLE eng4155 (
  id bigint NOT NULL,
  ts bigint NOT NULL,
  foo bigint NOT NULL,
  CONSTRAINT PK_id_eng4155 PRIMARY KEY (id)
);
CREATE INDEX TSINDEX ON eng4155 (ts DESC);

create table ptree (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table ptree on column a;

create index pcover2_TREE on ptree (a, b);
create index pcover3_TREE on ptree (a, c, b);

create table phash (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table phash on column a;

create index pcover2 on phash (a, b);
create index pcover3 on phash (a, c, b);

create table punique (
  a bigint not null unique,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table punique on column a;

create table puniqcombo (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table puniqcombo on column a;

create unique index pcover3_UNIQCOMBO on puniqcombo (a, c, b);

create table ppk (
  a bigint not null primary key,
  b bigint not null,
  c bigint not null,
  z bigint not null
);

partition table ppk on column a;

create table floataggs (
    alpha float,
    beta  float,
    gamma float
);

CREATE TABLE T_PAYMENT50 (
   SEQ_NO varchar(32 BYTES) NOT NULL,
   PID varchar(20 BYTES) NOT NULL,
   UID varchar(12 BYTES),
   CLT_NUM varchar(10 BYTES),
   DD_APDATE timestamp,
   ACCT_NO varchar(32 BYTES) NOT NULL,
   AUTH_TYPE varchar(1 BYTES),
   DEV_TYPE varchar(1 BYTES),
   TRX_CODE varchar(10 BYTES),
   AUTH_ID_TYPE varchar(1 BYTES),
   AUTH_ID varchar(32 BYTES),
   PHY_ID_TYPE varchar(2 BYTES),
   PHY_ID varchar(250 BYTES),
   CLIENT_IP varchar(32 BYTES),
   ACCT_TYPE varchar(1 BYTES),
   ACCT_BBK varchar(4 BYTES),
   TRX_CURRENCY varchar(2 BYTES),
   TRX_AMOUNT decimal,
   MCH_BBK varchar(4 BYTES),
   MCH_NO varchar(10 BYTES),
   BLL_NO varchar(10 BYTES),
   BLL_DATE varchar(8 BYTES),
   EXT_DATA varchar(20 BYTES),
   LBS_DISTANCE decimal,
   SAFE_DISTANCE_FLAG varchar(2 BYTES),
   LBS varchar(64 BYTES),
   LBS_CITY varchar(6 BYTES),
   LBS_COUNTRY varchar(3 BYTES),
   CONSTRAINT IDX_PAYMENT50_PKEY PRIMARY KEY (PID, SEQ_NO)
);
PARTITION TABLE T_PAYMENT50 ON COLUMN PID;
CREATE INDEX IDX_PAYMENT50_ACCT_NO ON T_PAYMENT50 (PID, ACCT_NO);
CREATE INDEX IDX_PAYMENT50_CLT_NUM ON T_PAYMENT50 (PID, CLT_NUM);
CREATE INDEX IDX_PAYMENT50_TIME ON T_PAYMENT50 (DD_APDATE);
CREATE INDEX IDX_PAYMENT50_UID ON T_PAYMENT50 (PID, UID);
DR TABLE T_PAYMENT50;
