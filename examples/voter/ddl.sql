file -inlinebatch END_OF_DROP_BATCH

DROP PROCEDURE Initialize                      IF EXISTS;
DROP PROCEDURE Results                         IF EXISTS;
DROP PROCEDURE Vote                            IF EXISTS;
DROP PROCEDURE ContestantWinningStates         IF EXISTS;
DROP PROCEDURE GetStateHeatmap                 IF EXISTS;
DROP VIEW  v_votes_by_phone_number             IF EXISTS;
DROP VIEW  v_votes_by_contestant_number        IF EXISTS;
DROP VIEW  v_votes_by_contestant_number_state  IF EXISTS;
DROP TABLE contestants                         IF EXISTS;
DROP TABLE votes                               IF EXISTS;
DROP TABLE area_code_state                     IF EXISTS;

END_OF_DROP_BATCH

-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
file -inlinebatch END_OF_BATCH

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

END_OF_BATCH

-- Update classes from jar so that the server will know about classes
-- but not procedures yet.
-- This command cannot be part of a DDL batch.
LOAD CLASSES voter-procs.jar;

-- The following CREATE PROCEDURE statements can all be batched.
file -inlinebatch END_OF_2ND_BATCH

-- stored procedures
CREATE PROCEDURE FROM CLASS voter.Initialize;
CREATE PROCEDURE FROM CLASS voter.Results;
CREATE PROCEDURE PARTITION ON TABLE votes COLUMN phone_number FROM CLASS voter.Vote;
CREATE PROCEDURE FROM CLASS voter.ContestantWinningStates;
CREATE PROCEDURE FROM CLASS voter.GetStateHeatmap;

END_OF_2ND_BATCH
