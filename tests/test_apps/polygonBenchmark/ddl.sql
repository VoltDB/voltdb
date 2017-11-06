DROP PROCEDURE InsertPolygonAsObject        IF EXISTS;
DROP PROCEDURE InsertPolygonAsString        IF EXISTS;
DROP TABLE polygons                         IF EXISTS;

-- Tell sqlcmd to batch the following commands together,
-- so that the schema loads quickly.
CREATE TABLE polygons
(
  id BIGINT not null primary key,
  poly GEOGRAPHY
);

PARTITION TABLE polygons ON COLUMN id;

LOAD CLASSES polygonBenchmark-procedures.jar;

CREATE PROCEDURE 
  PARTITION ON TABLE polygons COLUMN id
  FROM CLASS polygonBenchmark.InsertPolygonAsString;
CREATE PROCEDURE 
  PARTITION ON TABLE polygons COLUMN id
  FROM CLASS polygonBenchmark.InsertPolygonAsObject;


