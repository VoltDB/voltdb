CREATE TABLE movies
(
    id INTEGER NOT NULL,
    name VARCHAR(32) NOT NULL,
    PRIMARY KEY(id) 
);

PARTITION TABLE movies ON COLUMN id;

CREATE PROCEDURE FROM CLASS mytest.procedures.Initialize;
CREATE PROCEDURE FROM CLASS mytest.procedures.InsertTwoDuplicate;
PARTITION PROCEDURE InsertTwoDuplicate ON TABLE movies COLUMN id;
