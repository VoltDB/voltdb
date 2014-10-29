-- This DDL demonstrates how to work with just DDLs and use VoltDB like a traditional database by updating your schema.
-- This DDL rebuilds Voter schema without changes but one can change schema by using ALTER commands.
-- Drop Procedures - procedures on tables must be dropped before table.
DROP PROCEDURE voter.procedures.GetStateHeatmap;
DROP PROCEDURE voter.procedures.ContestantWinningStates;
DROP PROCEDURE voter.procedures.Vote;
DROP PROCEDURE voter.procedures.Results;
DROP PROCEDURE voter.procedures.Initialize;
-- Drop views
DROP VIEW v_votes_by_phone_number;
DROP VIEW v_votes_by_contestant_number_state;
-- Drop tables - Now drop tables.
DROP TABLE votes;
DROP TABLE contestants;
DROP TABLE area_code_state;
-- Create tables
CREATE TABLE contestants (contestant_number integer NOT NULL, contestant_name varchar(50) NOT NULL, CONSTRAINT PK_contestants PRIMARY KEY (contestant_number));
CREATE TABLE votes (phone_number bigint NOT NULL, state varchar(2) NOT NULL ,contestant_number integer NOT NULL); 
-- Partition Table
PARTITION TABLE votes ON COLUMN phone_number;
-- Create more tables
CREATE TABLE area_code_state (area_code smallint NOT NULL, state varchar(2) NOT NULL, CONSTRAINT PK_area_code_state PRIMARY KEY (area_code));
-- Create Views
CREATE VIEW v_votes_by_phone_number (phone_number, num_votes) AS SELECT phone_number, COUNT(*) FROM votes GROUP BY phone_number;
CREATE VIEW v_votes_by_contestant_number_state (contestant_number, state, num_votes) AS SELECT contestant_number, state, COUNT(*) FROM votes GROUP BY contestant_number, state;
-- Update classes from jar to that server will know about classes but not procedures yet.
exec @UpdateClasses voter-procs.jar ''
-- Now create procedures.
CREATE PROCEDURE FROM CLASS voter.procedures.Initialize;
CREATE PROCEDURE FROM CLASS voter.procedures.Results;
CREATE PROCEDURE FROM CLASS voter.procedures.Vote;
-- You can partition procedures.
PARTITION PROCEDURE Vote ON TABLE votes COLUMN phone_number;
-- And create more procedures.
CREATE PROCEDURE FROM CLASS voter.procedures.ContestantWinningStates;
CREATE PROCEDURE FROM CLASS voter.procedures.GetStateHeatmap;
-- Done your schema should be reloaded run client to see updated changes
