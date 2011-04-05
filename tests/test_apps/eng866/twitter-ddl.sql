CREATE TABLE hashtags (
    hashtag VARCHAR(256) NOT NULL,
    tweet_timestamp BIGINT NOT NULL
);

CREATE TABLE tweets (
    username VARCHAR(256) NOT NULL,
    tweet_timestamp BIGINT NOT NULL
);
