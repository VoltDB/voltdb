-- partitioned in test on pkey
CREATE TABLE A (
 PKEY          INTEGER NOT NULL,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);

-- replicated in test
CREATE TABLE B (
 PKEY          INTEGER,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);

CREATE TABLE score
(
    user_id 		bigint			NOT NULL
,   user_name 		varchar(50)		NOT NULL 
,   score_value 	bigint			NOT NULL
,   score_date 		bigint			NOT NULL
,   score_hour      bigint          NOT NULL
,   score_day       bigint          NOT NULL
);
PARTITION TABLE score ON COLUMN user_id;

CREATE TABLE C (
 ID            INTEGER NOT NULL,
 DEPT          INTEGER NOT NULL,
 NAME          VARCHAR(20),
);
PARTITION TABLE C ON COLUMN dept;

CREATE INDEX IDX_User_Id ON score(user_id);
CREATE INDEX IDX_score_date ON score (score_date, user_id);
CREATE INDEX IDX_score_value_user ON score (score_value, user_id);

CREATE PROCEDURE GetTopScores AS 
 SELECT user_id, score_value 
 FROM score WHERE score_date > ? AND score_date <= ? 
 ORDER BY score_value DESC, user_id DESC LIMIT ?;
