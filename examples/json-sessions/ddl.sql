CREATE TABLE user_session_table (
    username           varchar(200)        UNIQUE NOT NULL,
    password           varchar(100)        NOT NULL,
    global_session_id  varchar(200)        UNIQUE NOT NULL,
    last_accessed      TIMESTAMP,
    json_data          varchar(2048)
);
PARTITION TABLE user_session_table ON COLUMN username;

CREATE INDEX session_site_moderator  ON user_session_table (field(json_data, 'site'), field(json_data, 'moderator'), username);

CREATE INDEX session_props ON user_session_table (field(field(json_data, 'props'), 'download_version'), field(field(json_data, 'props'), 'client_language'), username);

CREATE PROCEDURE FROM CLASS json.procedures.Login;
