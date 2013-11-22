CREATE TABLE P1 (
  ID INTEGER NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  CONSTRAINT P1_PK_TREE PRIMARY KEY (ID)
);
CREATE INDEX P1_IDX_NUM_DESC_RATIO_TREE ON P1 (NUM, DESC, RATIO);
CREATE INDEX P1_IDX_RATIO_DESC_NUM_TREE ON P1 (RATIO, DESC, NUM);
CREATE INDEX P1_IDX_DESC_NUM_RATIO_TREE ON P1 (DESC, NUM, RATIO);
CREATE INDEX P1_IDX_NUM_RATIO_DESC_TREE ON P1 (NUM, RATIO, DESC);

PARTITION TABLE P1 ON COLUMN ID;

CREATE TABLE R1 (
  ID INTEGER NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  CONSTRAINT R1_PK_TREE PRIMARY KEY (ID)
--  PRIMARY KEY (ID)
);
CREATE INDEX R1_IDX_NUM_DESC_RATIO_TREE ON R1 (NUM, DESC, RATIO);
CREATE INDEX R1_IDX_RATIO_DESC_NUM_TREE ON R1 (RATIO, DESC, NUM);
CREATE INDEX R1_IDX_DESC_NUM_RATIO_TREE ON R1 (DESC, NUM, RATIO);
CREATE INDEX R1_IDX_NUM_RATIO_DESC_TREE ON R1 (NUM, RATIO, DESC);

CREATE TABLE R2 (
  ID INTEGER NOT NULL,
  DESC VARCHAR(300),
  NUM INTEGER,
  RATIO FLOAT,
  CONSTRAINT R2_PK_TREE PRIMARY KEY (ID)
--  PRIMARY KEY (ID)
);
CREATE INDEX R2_IDX_NUM_DESC_RATIO_TREE ON R2 (NUM, DESC);
CREATE INDEX R2_IDX_RATIO_DESC_NUM_TREE ON R2 (RATIO, DESC);
CREATE INDEX R2_IDX_DESC_NUM_RATIO_TREE ON R2 (DESC, NUM);
CREATE INDEX R2_IDX_NUM_RATIO_DESC_TREE ON R2 (NUM, RATIO);



