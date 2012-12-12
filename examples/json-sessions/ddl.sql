CREATE TABLE user_session_table (
    username           varchar(200)        UNIQUE NOT NULL,
    password           varchar(100)        NOT NULL,
    global_session_id  varchar(200)        UNIQUE NOT NULL,
    last_accessed      TIMESTAMP,
    json_data          varchar(2048)
);
PARTITION TABLE user_session_table ON COLUMN username;

CREATE PROCEDURE FROM CLASS json.procedures.Login;
