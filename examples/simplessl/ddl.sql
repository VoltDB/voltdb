file -inlinebatch END_OF_DROP_BATCH

DROP VIEW  v_final_scores IF EXISTS;
DROP TABLE athletes       IF EXISTS;
DROP TABLE scores         IF EXISTS;

END_OF_DROP_BATCH

-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
file -inlinebatch END_OF_BATCH

-- Stores the list of athletes
CREATE TABLE athletes
(
  name varchar(50) NOT NULL,
);

-- Stores the scores submitted by each referee for each athlete.
CREATE TABLE scores
(
  referee_name  varchar(50) NOT NULL,
  athlete_name  varchar(50) NOT NULL,
  athlete_score integer     NOT NULL, CHECK(athlete_score >= 0 and athlete_score <= 10), -- FIXME VoltDB ignores CHECK() constraints
);

-- calculates official results by combining scores from each referee
-- CREATE VIEW v_final_scores
-- (
--   athlete_name
-- , total_score
-- )
-- AS
--    SUM( SELECT athlete_score, COUNT(*) )
--      FROM scores
-- GROUP BY athlete_name
--;

END_OF_BATCH

INSERT INTO athletes VALUES ('Adam');
INSERT INTO athletes VALUES ('Bob');
