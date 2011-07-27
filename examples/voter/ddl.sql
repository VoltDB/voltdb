-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE contestants
(
  contestant_number integer     NOT NULL
, contestant_name   varchar(50) NOT NULL
, PRIMARY KEY
  (
    contestant_number
  )
);

-- votes table holds every valid vote.
--   voters are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes
(
  phone_number       bigint     NOT NULL
, area_code          smallint   NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
-- PARTITION BY ( phone_number )
);

-- Map of Area Codes and States for geolocation classification of incoming calls
CREATE TABLE area_code_state
(
  area_code smallint   NOT NULL
, state     varchar(2) NOT NULL
, PRIMARY KEY
  (
    area_code
  )
);

-- Supporting index on the vote table for business rule validation ("no more than <x> votes per phone number")
CREATE INDEX idx_votes ON votes (phone_number);

-- rollup of votes by phone number, used to reject excessive voting
CREATE VIEW v_votes_by_phone_number
(
  phone_number
, num_votes
)
AS
   SELECT phone_number
        , COUNT(*)
     FROM votes
 GROUP BY phone_number
;

-- rollup of votes by contestant, used to determine winner
CREATE VIEW v_votes_by_contestant_number
(
  contestant_number
, num_votes
)
AS
   SELECT contestant_number
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
;

-- rollup of votes by area-code
CREATE VIEW v_votes_by_area_code
(
  area_code
, num_votes
)
AS
   SELECT area_code
        , COUNT(*)
     FROM votes
 GROUP BY area_code
;

-- rollup of votes by contestant and area-code
CREATE VIEW v_votes_by_contestant_number_area_code
(
  contestant_number
, area_code
, num_votes
)
AS
   SELECT contestant_number
        , area_code
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
        , area_code
;

-- rollup of votes by state
CREATE VIEW v_votes_by_state
(
  state
, num_votes
)
AS
   SELECT state
        , COUNT(*)
     FROM votes
 GROUP BY state
;

-- rollup of votes by contestant and state
CREATE VIEW v_votes_by_contestant_number_state
(
  contestant_number
, state
, num_votes
)
AS
   SELECT contestant_number
        , state
        , COUNT(*)
     FROM votes
 GROUP BY contestant_number
        , state
;


