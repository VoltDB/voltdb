CREATE TABLE TU1 (
       ID INTEGER NOT NULL,
       POINTS INTEGER,
       PRIMARY KEY (ID)
);
partition table TU1 on column ID;
create assumeunique index idx_U1_TREE on TU1 (POINTS);
create assumeunique index idx2_U1_TREE on TU1 (ABS(POINTS));
create assumeunique index idx3_U1_TREE on TU1 (POINTS + ID);

CREATE TABLE TU2 (
       ID INTEGER NOT NULL ASSUMEUNIQUE,
       POINTS INTEGER,
       UNAME VARCHAR(10) NOT NULL,
       PRIMARY KEY (ID, UNAME)
);
partition table TU2 on column UNAME;
create unique index idx_U2_TREE on TU2 (UNAME,POINTS);
create unique index idx2_U2_TREE ON TU2 (UNAME, POINTS + 10);
create unique index idx3_U2_TREE ON TU2 (POINTS * 2, UNAME);
create assumeunique index idx4_U2_TREE ON TU2 (ABS(POINTS), CHAR_LENGTH(UNAME));

CREATE TABLE T1 (
       ID INTEGER NOT NULL,
       POINTS INTEGER NOT NULL
);
create index idx_T1_HASH on T1 (ID);
create index id2_T1_HASH on T1 (ID, POINTS);

CREATE TABLE TU3 (
       ID INTEGER NOT NULL ASSUMEUNIQUE,
       POINTS INTEGER,
       TEL INTEGER NOT NULL,
       PRIMARY KEY (ID, TEL)
);
partition table TU3 on column TEL;
create unique index idx_U3_TREE on TU3 (TEL,POINTS);
create unique index idx2_U3_TREE on TU3 (POINTS + 100, TEL);

CREATE TABLE TU4 (
       ID INTEGER NOT NULL ASSUMEUNIQUE,
       POINTS INTEGER,
       UNAME VARCHAR(10) NOT NULL,
       SEX TINYINT NOT NULL,
       PRIMARY KEY (ID, UNAME)
);
partition table TU4 on column UNAME;
create unique index idx_U4_TREE on TU4 (UNAME,SEX,POINTS);

CREATE TABLE TU5 (
        ID INTEGER NOT NULL,
        POINTS FLOAT
);
create index idx_tu5_TREE on TU5 (ID, POINTS);

CREATE TABLE TM1 (
       ID INTEGER NOT NULL,
       POINTS INTEGER,
       PRIMARY KEY (ID)
);
partition table TM1 on column ID;
create index idx_M1_TREE on TM1 (POINTS);
    
CREATE TABLE TM2 (
       ID INTEGER NOT NULL ASSUMEUNIQUE,
       POINTS INTEGER,
       UNAME VARCHAR(10) NOT NULL,
       PRIMARY KEY (ID, UNAME)
);
partition table TM2 on column UNAME;
create index idx_M2_TREE on TM2 (UNAME,POINTS);

CREATE TABLE TM3 (
       ID INTEGER NOT NULL,
       POINTS INTEGER NOT NULL,
       TEL INTEGER NOT NULL,
       PRIMARY KEY (ID)
);
create index idx_M3_TREE on TM3 (TEL,POINTS);

CREATE TABLE TM4 (
       ID INTEGER NOT NULL,
       POINTS INTEGER NOT NULL,
       UNAME VARCHAR(10) NOT NULL,
       SEX TINYINT NOT NULL,
       PRIMARY KEY (ID)
);
create index idx_M4_TREE on TM4 (UNAME,SEX,POINTS);

CREATE TABLE P1 (
       ID INTEGER DEFAULT '0' NOT NULL,
       DESC VARCHAR(300),
       NUM INTEGER NOT NULL,
       RATIO FLOAT NOT NULL,
       PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;
CREATE INDEX P1_IDX_NUM_TREE ON P1 (NUM);

CREATE TABLE P2 (
       P2_ID INTEGER DEFAULT '0' NOT NULL,
       P2_DESC VARCHAR(300),
       P2_NUM1 INTEGER NOT NULL,
       P2_NUM2 INTEGER NOT NULL,
       PRIMARY KEY (P2_ID)
);
CREATE INDEX P2_IDX_P2NUM_TREE ON P2 (P2_NUM1,P2_NUM2);

CREATE TABLE P3 (
       P3_ID INTEGER DEFAULT '0' NOT NULL,
       P3_DESC VARCHAR(300),
       P3_NUM1 INTEGER NOT NULL,
       P3_NUM2 INTEGER NOT NULL,
       PRIMARY KEY (P3_ID)
);
---CREATE INDEX P3_IDX_P3NUM_TREE ON P2 (P3_NUM1,P3_NUM2);

CREATE TABLE TMIN (
       A INTEGER NOT NULL,
       B INTEGER,
       C INTEGER
);
create index idx1_TMIN_TREE on TMIN (B);
create index idx2_TMIN_TREE on TMIN (A,B);
create index idx3_TMIN_TREE on TMIN (A,B,C);

create index idx4_TMIN_TREE on TMIN (ABS(B));
create index idx5_TMIN_TREE on TMIN (A,ABS(B));
create index idx6_TMIN_TREE on TMIN (ABS(A),B);


