CREATE TABLE P1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(5000 BYTES),
  DESC_INLINE VARCHAR(150 BYTES),
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);

CREATE TABLE R1 (
  ID INTEGER DEFAULT '0' NOT NULL,
  DESC VARCHAR(5000 BYTES),
  DESC_INLINE VARCHAR(150 BYTES),
  RATIO FLOAT NOT NULL,
  PRIMARY KEY (ID)
);
PARTITION TABLE P1 ON COLUMN ID;
