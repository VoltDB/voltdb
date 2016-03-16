CREATE TABLE player_game_score (
  player_id        INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  score            INTEGER      NOT NULL,
  PRIMARY KEY (player_id, game_id)
);

CREATE TABLE contest (
  contest_id       INTEGER      NOT NULL,
  game_id          INTEGER      NOT NULL,
  PRIMARY KEY (contest_id)
);

CREATE TABLE fantasy_user (
  user_id          INTEGER      NOT NULL,
  name             VARCHAR(30)  NOT NULL,
  PRIMARY KEY (user_id)
);
PARTITION TABLE fantasy_user ON COLUMN user_id;

CREATE TABLE user_contest_score (
  user_id          INTEGER      NOT NULL,
  contest_id       INTEGER      NOT NULL,
  score            INTEGER      NOT NULL,
  rank             INTEGER,
  PRIMARY KEY (user_id, contest_id)
);
PARTITION TABLE user_contest_score ON COLUMN user_id;
CREATE INDEX idx_user_contest_score ON user_contest_score (contest_id, score);

CREATE TABLE user_contest_roster (
  contest_id       INTEGER      NOT NULL,
  user_id          INTEGER      NOT NULL,
  player_id        INTEGER      NOT NULL,
  score            INTEGER,
  PRIMARY KEY (contest_id, user_id, player_id)
);
PARTITION TABLE user_contest_roster ON COLUMN user_id;
CREATE INDEX idx_roster_by_contest ON user_contest_roster (contest_id, user_id);

-- Update classes from jar to that server will know about classes but not procedures yet.
--LOAD CLASSES fantasysports-procs.jar;

-- Define procedures
CREATE PROCEDURE SelectContestScores AS
    SELECT r.user_id, SUM(p.score) AS score
    FROM user_contest_roster r
    INNER JOIN contest c ON r.contest_id = c.contest_id
    INNER JOIN player_game_score p ON r.player_id = p.player_id AND c.game_id = p.game_id
    WHERE r.contest_id = ?
    GROUP BY r.user_id
    ORDER BY score DESC
    LIMIT ?
    OFFSET ?;
