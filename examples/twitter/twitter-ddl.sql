CREATE TABLE hashtags (
    hashtag VARCHAR(32) NOT NULL,
    tweet_timestamp TIMESTAMP NOT NULL
);

CREATE TABLE partitioner (
    gotoall INT NOT NULL
);
