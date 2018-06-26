CREATE TABLE app_session (
  appid            INTEGER      NOT NULL,
  deviceid         BIGINT       NOT NULL,
  ts               TIMESTAMP    DEFAULT NOW
);
PARTITION TABLE app_session ON COLUMN deviceid;
