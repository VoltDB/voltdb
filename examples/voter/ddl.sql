create table text_narrow_noix (
      a bigint NOT NULL
    , b varchar(512)
);
PARTITION TABLE text_narrow_noix ON COLUMN a;
create table text_all_with_idx (
      a bigint NOT NULL
    , b varchar(512)
    , c int
    , d timestamp
    , e varchar(1024)
);
PARTITION TABLE text_all_with_idx ON COLUMN a;
CREATE INDEX idx_col_one ON text_all_with_idx(a,c);
CREATE INDEX idx_col_two ON text_all_with_idx(a,b);
CREATE INDEX idx_col_three ON text_all_with_idx(a,d);

create table text_narrow_mp (
      a bigint NOT NULL
    , b varchar(512)
);

-- contestants table holds the contestants numbers (for voting) and names
CREATE TABLE contestants
(
  contestant_number integer     NOT NULL
, contestant_name   varchar(50) NOT NULL
, CONSTRAINT PK_contestants PRIMARY KEY
  (
    contestant_number
  )
);

-- votes table holds every valid vote.
--   voters are not allowed to submit more than <x> votes, x is passed to client application
CREATE TABLE votes
(
  phone_number       bigint     NOT NULL
, state              varchar(2) NOT NULL
, contestant_number  integer    NOT NULL
);

PARTITION TABLE votes ON COLUMN phone_number;

-- Map of Area Codes and States for geolocation classification of incoming calls
CREATE TABLE area_code_state
(
  area_code smallint   NOT NULL
, state     varchar(2) NOT NULL
, CONSTRAINT PK_area_code_state PRIMARY KEY
  (
    area_code
  )
);

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

-- rollup of votes by contestant and state for the heat map and results
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

-- stored procedures
CREATE PROCEDURE FROM CLASS voter.procedures.Initialize;
CREATE PROCEDURE FROM CLASS voter.procedures.Results;
CREATE PROCEDURE FROM CLASS voter.procedures.Vote;
CREATE PROCEDURE FROM CLASS voter.procedures.ContestantWinningStates;
CREATE PROCEDURE FROM CLASS voter.procedures.GetStateHeatmap;
CREATE PROCEDURE FROM CLASS voter.procedures.CSVLoad;
CREATE PROCEDURE FROM CLASS voter.procedures.CSVLoadRaw;
