file -inlinebatch END_OF_BATCH

CREATE TABLE votes
(
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
, CONSTRAINT PK_phone_number PRIMARY KEY
  (
  phone_number
  )
--
);
PARTITION TABLE votes ON COLUMN phone_number;

CREATE PROCEDURE Delete AS delete from votes where contestant_number=?;

END_OF_BATCH
