CREATE TABLE R1
(
  a bigint,
  b bigint,
  c bigint,
  d bigint,
  e bigint NOT NULL,
  CONSTRAINT R1_PK PRIMARY KEY ( e )
);

CREATE UNIQUE INDEX r1_pidx_1 ON R1 (a) where b is not null;
CREATE INDEX r1_pidx_2 ON R1 (d) where a > 0;
CREATE UNIQUE INDEX r1_pidx_hash_1 ON R1 (c) where b is not null;
CREATE INDEX r1_pidx_hash_2 ON R1 (d) where a < 0;

CREATE TABLE P1
(
  a bigint NOT NULL,
  b bigint,
  c bigint,
  d bigint
);
PARTITION TABLE P1 ON COLUMN a;
CREATE UNIQUE INDEX p1_pidx_1 ON P1 (a) where b is not null;
CREATE UNIQUE INDEX p1_pidx_2 ON P1 (a) where a > 4;
CREATE UNIQUE INDEX p1_pidx_3 ON P1 (a) where a > c and d > 3;