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

-- track how many rejected votes each phone number has done
CREATE TABLE rejected_votes_by_phone_number
(
  phone_number   bigint NOT NULL
, rejected_votes bigint NOT NULL
, CONSTRAINT PK_rejected_votes_by_phone_number PRIMARY KEY
  (
    phone_number
  )
);
PARTITION TABLE rejected_votes_by_phone_number ON COLUMN phone_number;

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

CREATE ROLE dbuser WITH adhoc, defaultproc;
CREATE ROLE adminuser WITH sysproc,adhoc;
CREATE ROLE hockey WITH adhoc;

-- make sure to load the Java code for the procedures, before creating them
LOAD CLASSES voter.jar;

CREATE PROCEDURE ALLOW dbuser FROM CLASS voter.procedures.Initialize;
CREATE PROCEDURE ALLOW dbuser FROM CLASS voter.procedures.Results;
CREATE PROCEDURE ALLOW dbuser PARTITION ON TABLE votes COLUMN phone_number FROM CLASS voter.procedures.Vote;
CREATE PROCEDURE ALLOW dbuser FROM CLASS voter.procedures.ContestantWinningStates;
CREATE PROCEDURE ALLOW dbuser FROM CLASS voter.procedures.GetStateHeatmap;
