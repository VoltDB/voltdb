CREATE TABLE empty_r
(
  id integer NOT NULL
);

CREATE TABLE empty_p
(
  id integer NOT NULL
);
PARTITION TABLE empty_p ON COLUMN id;

-- stored procedures
CREATE PROCEDURE FROM CLASS measureoverhead.procedures.MO_ROSP;
CREATE PROCEDURE FROM CLASS measureoverhead.procedures.MO_RWSP;
CREATE PROCEDURE FROM CLASS measureoverhead.procedures.MO_ROMP;
CREATE PROCEDURE FROM CLASS measureoverhead.procedures.MO_RWMP;
