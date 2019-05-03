create table t (
  a bigint not null,
  b bigint not null,
  c bigint not null,
  d bigint not null,
  e bigint not null
);

create index idx_1_hash on t (a, b, c, d);
create index idx_2_TREE on t (e, a, b, c, d);

create index cover2_TREE on t (a, b);
create index cover3_TREE on t (a, c, b);

create table l
(
    id bigint NOT NULL,
    lname varchar(32) NOT NULL,
    a tinyint NOT NULL,
    b tinyint NOT NULL,
    CONSTRAINT PK_LOG PRIMARY KEY ( lname, id )
);

create index idx_c on l (lname, a, b, id);
create index idx_b on l (lname, b, id);
create index idx_a on l (lname, a, id);

CREATE INDEX casewhen_idx1 ON l (CASE WHEN a > b THEN a ELSE b END);
CREATE INDEX casewhen_idx2 ON l (CASE WHEN a < 10 THEN a*5 ELSE a + 5 END);

CREATE INDEX decode_idx3 ON l (b, DECODE(a, null, 0, a), id);

CREATE TABLE a
(
    id BIGINT NOT NULL,
    deleted TINYINT NOT NULL,
    updated_date BIGINT NOT NULL,
    CONSTRAINT id PRIMARY KEY (id)
);

CREATE INDEX deleted_since_idx ON a (deleted, updated_date, id);

CREATE TABLE c
(
  a bigint not null,
  b bigint not null,
  c bigint not null,
  d bigint not null,
  e bigint,
  f bigint not null,
  g bigint
);
CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
CREATE INDEX a_partial_idx_not_null_d_e ON c (a + b) where (d + e) is not null;
CREATE UNIQUE INDEX z_full_idx_a ON c (a);
CREATE INDEX partial_idx_not_null_e_dup ON c (a) where e is not null;
CREATE INDEX partial_idx_null_e ON c (a) where e is null;
CREATE INDEX partial_idx_or_expr ON c (f) where e > 0 or d < 5;
CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1;
CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5;
CREATE INDEX partial_idx_3 ON c (b) where d > 0;
CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f;
CREATE INDEX partial_idx_5 ON c (b) where d > f;
CREATE INDEX partial_idx_6 ON c (g) where g < 0;
CREATE INDEX partial_idx_7 ON c (g) where g is not null;
CREATE INDEX partial_idx_8 ON c (b) WHERE abs(a) > 0;

CREATE TABLE polypoints (
  poly geography(1024),
  point geography_point,
  primarykey int primary key, -- index 1
  uniquekey int unique, -- index 2
  uniquehashable int,
  nonuniquekey int,
  component1 int,
  component2unique int,
  component2non int);

-- index 3
CREATE INDEX polypointspoly ON polypoints ( poly );
-- index 4
CREATE INDEX nonunique ON polypoints ( nonuniquekey );

-- index 5
CREATE UNIQUE INDEX compoundunique ON polypoints ( component1, component2unique );
-- index 6
CREATE INDEX compoundnon ON polypoints ( component1, component2non );
-- index 7
CREATE UNIQUE INDEX HASHUNIQUEHASH ON polypoints ( uniquehashable );

CREATE TABLE R (
  ID      INTEGER  DEFAULT 0,
  TINY    TINYINT  DEFAULT 0,
  VCHAR_INLINE_MAX  VARCHAR(63 BYTES) DEFAULT '0',
  VCHAR_OUTLINE_MIN VARCHAR(64 BYTES) DEFAULT '0' NOT NULL,
  POLYGON GEOGRAPHY,
  PRIMARY KEY (ID, VCHAR_OUTLINE_MIN));

CREATE INDEX IDX ON R (R.POLYGON) WHERE NOT R.TINY IS NULL;
