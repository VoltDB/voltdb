DROP PROCEDURE Insert         IF EXISTS;
DROP PROCEDURE Select         IF EXISTS;
DROP PROCEDURE SelectGeo      IF EXISTS;
DROP PROCEDURE InsertGeo      IF EXISTS;

DROP TABLE HELLOWORLD         IF EXISTS;
DROP TABLE GEOTABLE           IF EXISTS;

CREATE TABLE HELLOWORLD (
   HELLO VARCHAR(15),
   WORLD VARCHAR(15),
   DIALECT VARCHAR(15) NOT NULL,
   PRIMARY KEY (DIALECT)
);

CREATE TABLE GEOTABLE (
   ID           BIGINT PRIMARY KEY NOT NULL,
   GEO          GEOGRAPHY,
   GEO_PT       GEOGRAPHY_POINT,
);

INSERT INTO GEOTABLE VALUES (100, 
                             POLYGONFROMTEXT('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))'), 
                             POINTFROMTEXT('POINT(0 0)'));
INSERT INTO GEOTABLE VALUES (101, 
                             POLYGONFROMTEXT('POLYGON((0 0, 45 0, 45 45, 0 45, 0 0), (10 10, 10 30, 30 30, 30 10, 10 10))'), 
                             POINTFROMTEXT('POINT(20 20)'));
INSERT INTO GEOTABLE VALUES (102, 
                             null,
                             POINTFROMTEXT('POINT(0 0)'));
INSERT INTO GEOTABLE VALUES (103, 
                             POLYGONFROMTEXT('POLYGON((0 0, 45 0, 45 45, 0 45, 0 0), (10 10, 10 30, 30 30, 30 10, 10 10))'), 
                             null);
INSERT INTO GEOTABLE VALUES (104, 
                             null,
                             null);

CREATE PROCEDURE Insert AS INSERT INTO HELLOWORLD (Hello, World, Dialect) VALUES (?, ?, ?);
CREATE PROCEDURE Select AS SELECT HELLO, WORLD FROM HELLOWORLD WHERE DIALECT = ?;

-- Executing this will send values of both geo types from the client to the server
-- in the request, and send both geo types from the server to the client in the
-- response.  We can also test receiving nulls here.
--
-- The "WHERE ID = ?" is to limit the output, since the CONTAINS predicate
-- does not depend on the table rows at all.
CREATE PROCEDURE SelectGeo AS SELECT * FROM GEOTABLE WHERE ID = ?;
-- Executing this will send values of both geo types from the client to the server.
-- We can also test sending nulls reliably.
CREATE PROCEDURE InsertGeo AS INSERT INTO GEOTABLE VALUES(?, ?, ?);
