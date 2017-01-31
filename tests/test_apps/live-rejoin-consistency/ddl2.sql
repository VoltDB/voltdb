-- these two dummy tables are for the second catalog only
-- used in test for update application catalog
CREATE TABLE partitioned
(
    id int NOT NULL,
    bar bigint NOT NULL,
    PRIMARY KEY(ID)
);
PARTITION TABLE partitioned ON COLUMN id;
CREATE TABLE replicated
(
    id int NOT NULL,
    foo bigint NOT NULL,
    PRIMARY KEY(ID)
);

