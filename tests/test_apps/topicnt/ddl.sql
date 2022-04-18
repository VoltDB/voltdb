CREATE TABLE cookies (
  cookieid bigint not null primary key
, username varchar(100) not null
);
partition table cookies on column cookieid;

CREATE PROCEDURE GetUsernameFromCookie
PARTITION ON TABLE cookies COLUMN cookieid
AS
SELECT username
FROM   cookies
WHERE  cookieid = ?;

-- Table of accounts: domain, account id, count of hits, last user
CREATE TABLE accounts EXPORT TO TOPIC account_hits ON update_new (
  domain varchar(100) not null
, accountid bigint not null
, hitcount bigint default 0
, lastuser varchar(100) default 'nobody'
);
partition table accounts on column domain;

CREATE PROCEDURE GetAccountIdFromDomain
PARTITION ON TABLE accounts COLUMN domain
AS
SELECT accountid
FROM   accounts
WHERE  domain = ?;

CREATE PROCEDURE UpdateAccount
PARTITION ON TABLE accounts COLUMN domain PARAMETER 1
AS
UPDATE accounts
SET hitcount = hitcount + 1, lastuser = ?
WHERE  domain = ?;

-- Any updates will export the next action to topic 'next_actions'
CREATE TABLE users EXPORT TO TOPIC next_actions ON update_new
  WITH KEY(username) VALUE (next_best_action) (
  username varchar(100) not null primary key
, email    varchar(100)
, next_best_action varchar(100) default 'none'
);
partition table users on column username;

-- Any inserts will migrate after 10s to topic 'user_hits'
CREATE TABLE user_hits MIGRATE TO TOPIC user_hits
  WITH KEY(username) VALUE (accountid, url) (
  username varchar(100) not null
, created TIMESTAMP DEFAULT NOW NOT NULL
, category varchar(10) not null
, cookieid bigint not null
, accountid bigint not null
, url      varchar(100) not null
) USING TTL 10 SECONDS ON COLUMN created;
partition table user_hits on column username;
CREATE INDEX userIdx ON user_hits (created) WHERE NOT MIGRATING;

CREATE STREAM cookie_errors partition on column cookieid
  EXPORT TO TOPIC cookie_errors
  WITH KEY (cookieid) VALUE (url, message) (
  cookieid bigint not null
  , url      varchar(100) not null
  , message  varchar(1024)
);

LOAD CLASSES topicnt-server.jar;
CREATE PROCEDURE
   PARTITION ON TABLE users COLUMN username
   FROM CLASS server.GetNextBestAction;

CREATE COMPOUND PROCEDURE
   FROM CLASS server.HandleHit;
