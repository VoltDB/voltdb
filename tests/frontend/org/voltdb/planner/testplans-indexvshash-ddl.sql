create table t (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  d bigint not null,
  e bigint not null
);

create index IDX_1_TREE on t (a, b, c, d);
create index IDX_2_TREE on t (e, a, b, c, d);

create index COVER2_TREE on t (a, b);
create index COVER3_TREE on t (a, c, b);


CREATE TABLE data_reports (
  reportID BIGINT NOT NULL,
  appID BIGINT NOT NULL,
  metricID BIGINT NOT NULL,
  time TIMESTAMP NOT NULL,
  value FLOAT DEFAULT '0' NOT NULL,
  field VARCHAR(10) DEFAULT 'value' NOT NULL,
  CONSTRAINT IDX_reportData_PK PRIMARY KEY (reportID,metricID,time,field, appID)
);
PARTITION TABLE data_reports ON COLUMN appID;
CREATE INDEX assumeunique_PK_index on data_reports (reportID,metricID,time,field);


create table R (
  a bigint ,
  b bigint ,
  c bigint ,
  d bigint ,
  e bigint , 
  f bigint , 
);

create index R1_TREE on R (a);
create index R2_TREE on R (b,c);
create index R3_TREE on R (d, e, f);