create table narrow_short_noix (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(60)
);
PARTITION TABLE narrow_short_noix ON COLUMN a;

create table narrow_short_ix (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(60)
);
PARTITION TABLE narrow_short_ix ON COLUMN a;
CREATE INDEX IX_narrow_short_ix on narrow_short_ix (a);

create table narrow_short_cmpix (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(60)
);
PARTITION TABLE narrow_short_cmpix ON COLUMN a;
CREATE INDEX IX_narrow_short_cmpix on narrow_short_cmpix (a,b);

create table narrow_short_hasview (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(60)
);
PARTITION TABLE narrow_short_hasview ON COLUMN a;
CREATE VIEW V_narrow_short_hasview ( a,b,c,d,e,f ) AS
SELECT a,b,c,d,e,count(*) as f
FROM narrow_short_hasview
GROUP BY a,b,c,d,e;

create table narrow_long_noix (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(512)
);
PARTITION TABLE narrow_long_noix ON COLUMN a;

create table narrow_long_ix (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(512)
);
PARTITION TABLE narrow_long_ix ON COLUMN a;
CREATE INDEX IX_narrow_long_ix on narrow_long_ix (a);

create table narrow_long_cmpix (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(512)
);
PARTITION TABLE narrow_long_cmpix ON COLUMN a;
CREATE INDEX IX_narrow_long_cmpix on narrow_long_cmpix (a,b);

create table narrow_long_hasview (
      a integer NOT NULL
    , b smallint
    , c tinyint
    , d bigint
    , e varchar(512)
);
PARTITION TABLE narrow_long_hasview ON COLUMN a;
CREATE VIEW V_narrow_long_hasview ( a,b,c,d,e,f ) AS
SELECT a,b,c,d,e,count(*) as f
FROM narrow_long_hasview
GROUP BY a,b,c,d,e;

create table generic_noix (
      a integer NOT NULL
    , b tinyint
    , c smallint
    , d varchar(1)
    , e timestamp
    , f timestamp
    , h varchar(60)
    , i varchar(60)
    , j varchar(60)
    , k varchar(1024)
    , l varchar(1024)
    , m varchar(1024)
    , n float
    , o bigint
    , p varchar(1)
    , r bigint
    --, s decimal(32,4)
    --, t decimal(32,4)
    --, u decimal(32,4)
);
PARTITION TABLE generic_noix ON COLUMN a;

create table generic_ix (
      a integer NOT NULL
    , b tinyint
    , c smallint
    , d varchar(1)
    , e timestamp
    , f timestamp
    , h varchar(60)
    , i varchar(60)
    , j varchar(60)
    , k varchar(1024)
    , l varchar(1024)
    , m varchar(1024)
    , n float
    , o bigint
    , p varchar(1)
    , r bigint
    --, s decimal(32,4)
    --, t decimal(32,4)
    --, u decimal(32,4)
);
PARTITION TABLE generic_ix ON COLUMN a;
CREATE INDEX IX_generic_ix on generic_ix (a);

--CREATE PROCEDURE FROM CLASS csvbenchmark.procedures.DoNothingProcedure;
