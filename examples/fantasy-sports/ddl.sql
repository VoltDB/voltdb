CREATE TABLE nfl_player_game_score (
  player_id        INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  score            INTEGER      NOT NULL,
  PRIMARY KEY (player_id, game_id)
);

CREATE TABLE nfl_contest_small (
  contest_id       INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  PRIMARY KEY (contest_id)
);
PARTITION TABLE nfl_contest_small ON COLUMN contest_id;

CREATE TABLE nfl_contest_large (
  contest_id       INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  PRIMARY KEY (contest_id)
);

CREATE TABLE fantasy_user (
  user_id      INTEGER      NOT NULL,
  name             VARCHAR(30)  NOT NULL,
  PRIMARY KEY (user_id)  
);
PARTITION TABLE fantasy_user ON COLUMN user_id;

CREATE TABLE user_contest_score (
  user_id      INTEGER      NOT NULL,
  contest_id       INTEGER      NOT NULL,
  score            INTEGER      NOT NULL,
  rank             INTEGER,
  PRIMARY KEY (user_id, contest_id)
);
PARTITION TABLE user_contest_score ON COLUMN user_id;
CREATE INDEX idx_user_contest_score ON user_contest_score (contest_id, score);

-- CREATE TABLE user_contest_rank (
--   contest_id       INTEGER      NOT NULL,
--   user_id      INTEGER      NOT NULL,
--   rank             INTEGER      NOT NULL,
--   PRIMARY KEY (user_id, contest_id)
-- );
-- PARTITION TABLE user_contest_rank ON COLUMN user_id;
--CREATE INDEX idx_ranks ON user_contest_rank (contest_id, score);

CREATE TABLE user_contest_roster (
  contest_id       INTEGER      NOT NULL,
  user_id      INTEGER      NOT NULL,
  player_id        INTEGER      NOT NULL,
  score            INTEGER,
  PRIMARY KEY (contest_id, user_id, player_id)
);
PARTITION TABLE user_contest_roster ON COLUMN user_id;
CREATE INDEX idx_roster_by_contest ON user_contest_roster (contest_id, user_id);

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES procs.jar;

-- Define procedures
CREATE PROCEDURE PARTITION ON TABLE fantasy_user COLUMN user_id FROM CLASS procedures.SelectAllScoresInPartition;
CREATE PROCEDURE PARTITION ON TABLE fantasy_user COLUMN user_id FROM CLASS procedures.SelectContestScoresInPartition;
CREATE PROCEDURE PARTITION ON TABLE fantasy_user COLUMN user_id FROM CLASS procedures.UpsertUserScores;

