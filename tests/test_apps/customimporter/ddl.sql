create table Example (
    a varchar(100) not null,
    b varchar(100),
    c varchar(100),
    CONSTRAINT pk_example PRIMARY KEY ( A )
);

PARTITION TABLE Example ON COLUMN A;

CREATE PROCEDURE InsertOnly as INSERT into Example(a, b, c) VALUES(?, ?, ?);
