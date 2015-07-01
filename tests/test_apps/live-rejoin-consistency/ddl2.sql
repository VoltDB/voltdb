CREATE TABLE counters_ptn
(
  id int NOT NULL,
  counter bigint NOT NULL,
  PRIMARY KEY(ID)
);
PARTITION TABLE counters_ptn ON COLUMN id;

CREATE TABLE counters_rep
(
  id int NOT NULL,
  counter bigint NOT NULL,
  PRIMARY KEY(ID)
);

-- this table used to test @LoadTable procedures
-- it has the same shape as counters and the same 
-- distribution but no primary key
CREATE TABLE like_counters_ptn
(
  id int NOT NULL,
  counter bigint NOT NULL
);
PARTITION TABLE like_counters_ptn ON COLUMN id;

CREATE TABLE like_counters_rep
(
  id int NOT NULL,
  counter bigint NOT NULL
);

CREATE TABLE joiner
(
    id int NOT NULL,
    PRIMARY KEY(ID)
);
PARTITION TABLE joiner ON COLUMN id;

-- these two dummy tables are for the second catalog only
-- used in test for update application catalog
CREATE TABLE partitioned
(
    id int NOT NULL,
    bar bigint NOT NULL,
    PRIMARY KEY(ID)
);
PARTITION TABLE partitioned ON COLUMN id;
CREATE TABLE replicated
(
    id int NOT NULL,
    foo bigint NOT NULL,
    PRIMARY KEY(ID)
);

