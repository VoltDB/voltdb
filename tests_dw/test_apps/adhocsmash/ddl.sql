CREATE TABLE votes
(
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, CONSTRAINT PK_phone_number PRIMARY KEY
  (
  phone_number
  )
-- PARTITION BY ( phone_number )
);
