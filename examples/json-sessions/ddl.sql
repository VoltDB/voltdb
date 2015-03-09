CREATE TABLE user_session_table (
    username           varchar(200)        UNIQUE NOT NULL,
    password           varchar(100)        NOT NULL,
    global_session_id  varchar(200)        NOT NULL,
    last_accessed      TIMESTAMP,
    json_data          varchar(2048)
);
PARTITION TABLE user_session_table ON COLUMN username;

CREATE UNIQUE INDEX username_idx ON user_session_table (username);

CREATE INDEX session_site_moderator ON user_session_table (field(json_data, 'site'), field(json_data, 'moderator'), username);

CREATE INDEX session_props ON user_session_table (field(json_data, 'props.download_version'), field(json_data, 'props.client_language'), username);

-- Update classes from jar to that server will know about classes but not procedures yet.
LOAD CLASSES json-procs.jar;

CREATE PROCEDURE PARTITION ON TABLE user_session_table COLUMN username FROM CLASS jsonsessions.Login;
