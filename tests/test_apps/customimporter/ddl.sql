create table Example(
    a varchar(10),
    b varchar(10),
    c varchar(10),
    CONSTRAINT pk_example PRIMARY KEY ( A )
);

PARTITION TABLE Example ON COLUMN A;

