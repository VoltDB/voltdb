CREATE TABLE ALL_VALUES (
    bigcol    BIGINT NOT NULL,
    tinycol   TINYINT,
    smallcol  SMALLINT,
    intcol    INT,
    floatcol  FLOAT,
    strcol    VARCHAR(128),
    tscol     TIMESTAMP,
    bincol    VARBINARY(128)
);

EXPORT TABLE ALL_VALUES;

CREATE PROCEDURE ExportInsert AS 
   INSERT INTO ALL_VALUES
     (bigcol,tinycol,smallcol,intcol,floatcol,strcol,tscol,bincol)
     VALUES (?,?,?,?,?,?,?,?);

PARTITION TABLE ALL_VALUES ON COLUMN bigcol;
PARTITION PROCEDURE ExportInsert ON TABLE ALL_VALUES COLUMN bigcol;