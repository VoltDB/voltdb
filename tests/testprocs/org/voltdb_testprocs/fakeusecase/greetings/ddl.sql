CREATE TABLE greetings
(
    language VARCHAR(32) NOT NULL PRIMARY KEY,
    english_version VARCHAR(32) NOT NULL,
    native_version VARCHAR(64) NOT NULL,
    querycount BIGINT NOT NULL
);

CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fakeusecase.greetings.GetGreetingExactMatch;
CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fakeusecase.greetings.GetGreetingCaseInsensitive;
CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.fakeusecase.greetings.LoadGreetings;