CREATE TABLE c3
(
  g integer NOT NULL
, a integer NOT NULL
, b integer NOT NULL
, factor decimal NOT NULL
, ts timestamp NOT NULL
, PRIMARY KEY (g, a, b, ts)
);

CREATE TABLE c1
(
  e integer NOT NULL
, y integer NOT NULL
, name1 varchar(255) NOT NULL
, name2 varchar(50) NOT NULL
, PRIMARY KEY (e)
);

CREATE TABLE c5
(
  partitionkey integer NOT NULL
, g integer NOT NULL
, PRIMARY KEY (partitionkey)
);
PARTITION TABLE c5 ON COLUMN partitionkey;

CREATE TABLE c4
(
  f integer     NOT NULL
, partitionkey integer     NOT NULL
, PRIMARY KEY (f, partitionkey)
);
PARTITION TABLE c4 ON COLUMN partitionkey;

CREATE TABLE c2
(
  partitionkey integer     NOT NULL
, a integer     NOT NULL
, e integer     NOT NULL
, ts timestamp     NOT NULL
, value decimal     NOT NULL
, PRIMARY KEY (partitionkey, ts, a, e)
);
PARTITION TABLE c2 ON COLUMN partitionkey;
