CREATE TABLE empty_r
(
  id integer NOT NULL
);

CREATE TABLE empty_p
(
  id integer NOT NULL
);
PARTITION TABLE empty_p ON COLUMN id;

-- make sure to load the Java code for the procedures, before creating them
LOAD CLASSES measure-overhead.jar;

-- stored procedures
CREATE PROCEDURE PARTITION ON TABLE EMPTY_P COLUMN ID FROM CLASS measureoverhead.procedures.MO_ROSP;
CREATE PROCEDURE PARTITION ON TABLE EMPTY_P COLUMN ID FROM CLASS measureoverhead.procedures.MO_RWSP;
CREATE PROCEDURE FROM CLASS measureoverhead.procedures.MO_ROMP;
CREATE PROCEDURE FROM CLASS measureoverhead.procedures.MO_RWMP;
