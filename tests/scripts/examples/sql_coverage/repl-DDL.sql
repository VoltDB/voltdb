-- Eventually, this should evolve into a superset of the DDL for all the schema in all the configurations
-- used with sqlcoverage. Then we could avoid bouncing the server between templates.

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE R2 (
  ID INTEGER NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE TABLE R3 (
  ID INTEGER NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  PRIMARY KEY (ID)
);

CREATE INDEX ID_R3 ON R3 (ID);
