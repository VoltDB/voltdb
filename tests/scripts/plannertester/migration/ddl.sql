CREATE TABLE with_ttl migrate to target foo (
  i timestamp NOT NULL,
  j int NOT NULL
) USING TTL 1 minutes ON COLUMN i;
PARTITION table with_ttl on column j;
CREATE INDEX idex1 ON with_ttl(j) WHERE NOT MIGRATING;

CREATE TABLE without_ttl migrate to target foo (
  i int NOT NULL,
  j int NOT NULL
);
PARTITION table without_ttl on column j;
CREATE INDEX idex2 ON without_ttl(j) WHERE NOT MIGRATING;

CREATE TABLE with_ttl_replicated migrate to target foo (
  i timestamp NOT NULL,
  j int NOT NULL
) USING TTL 1 minutes ON COLUMN i;
CREATE INDEX idex3 ON with_ttl_replicated(j) WHERE NOT MIGRATING;

CREATE TABLE without_ttl_replicated migrate to target foo (
  i int NOT NULL,
  j int NOT NULL
);
CREATE INDEX idex4 ON without_ttl_replicated(j) WHERE NOT MIGRATING;

CREATE TABLE with_ttl_no_index migrate to target foo (
  i timestamp NOT NULL,
  j int NOT NULL
) USING TTL 1 minutes ON COLUMN i;
PARTITION table with_ttl_no_index on column j;

CREATE TABLE without_ttl_no_index migrate to target foo (
  i int NOT NULL,
  j int NOT NULL
);
PARTITION table without_ttl_no_index on column j;

CREATE TABLE with_ttl_replicated_no_index migrate to target foo (
  i timestamp NOT NULL,
  j int NOT NULL
) USING TTL 1 minutes ON COLUMN i;

CREATE TABLE without_ttl_replicated_no_index migrate to target foo (
  i int NOT NULL,
  j int NOT NULL
);
