CREATE TABLE a (
  erbi VARCHAR(50) NOT NULL,
  event_id BIGINT NOT NULL,
  race_id BIGINT NOT NULL,
  bracket_id BIGINT NOT NULL,
  interval_id BIGINT NOT NULL,
  entry_id BIGINT NOT NULL,
  iv_time_millis BIGINT NOT NULL,
  CONSTRAINT a_pk PRIMARY KEY (erbi, entry_id)
);

CREATE INDEX ix_a_erbit on a (erbi, iv_time_millis);
PARTITION TABLE a ON COLUMN erbi;

CREATE TABLE b (
  erbi VARCHAR(50) NOT NULL,
  event_id BIGINT NOT NULL,
  race_id BIGINT,
  bracket_id BIGINT NOT NULL,
  interval_id BIGINT NOT NULL,
  entry_id BIGINT NOT NULL,
  iv_time_millis BIGINT NOT NULL,
  CONSTRAINT b_pk PRIMARY KEY (erbi, entry_id)
);

CREATE INDEX ix_b_erbit on b (erbi, iv_time_millis);
PARTITION TABLE b ON COLUMN erbi;
