CREATE TABLE ids
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWithMatView
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWithMinMatView
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWithMinMatViewOpt
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWith2MinMatView
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWith2MinMatViewOpt
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWith4MinMatView
(
  id bigint NOT NULL,
  group_id bigint,
  v1 bigint,
  v2 bigint,
  v3 bigint,
  v4 bigint,
  PRIMARY KEY (id)
);

CREATE TABLE idsWith4MinMatViewOpt
(
  id bigint NOT NULL,
  group_id bigint,
  v1 bigint,
  v2 bigint,
  v3 bigint,
  v4 bigint,
  PRIMARY KEY (id)
);

PARTITION TABLE ids ON COLUMN id;
PARTITION TABLE idsWithMatView ON COLUMN id;
PARTITION TABLE idsWithMinMatView ON COLUMN id;
PARTITION TABLE idsWithMinMatViewOpt ON COLUMN id;
PARTITION TABLE idsWith2MinMatView ON COLUMN id;
PARTITION TABLE idsWith2MinMatViewOpt ON COLUMN id;
PARTITION TABLE idsWith4MinMatView ON COLUMN id;
PARTITION TABLE idsWith4MinMatViewOpt ON COLUMN id;

CREATE PROCEDURE ids_insert AS
  INSERT INTO ids VALUES (?,?,?);
CREATE PROCEDURE idsWithMatView_insert AS
  INSERT INTO idsWithMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatView_insert AS
  INSERT INTO idsWithMinMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatViewOpt_insert AS
  INSERT INTO idsWithMinMatViewOpt VALUES (?,?,?);
CREATE PROCEDURE idsWith2MinMatView_insert AS
  INSERT INTO idsWith2MinMatView VALUES (?,?,?);
CREATE PROCEDURE idsWith2MinMatViewOpt_insert AS
  INSERT INTO idsWith2MinMatViewOpt VALUES (?,?,?);
CREATE PROCEDURE idsWith4MinMatView_insert AS
  INSERT INTO idsWith4MinMatView VALUES (?,?,?,?,?,?);
CREATE PROCEDURE idsWith4MinMatViewOpt_insert AS
  INSERT INTO idsWith4MinMatViewOpt VALUES (?,?,?,?,?,?);
PARTITION PROCEDURE ids_insert ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_insert ON TABLE idsWithMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatView_insert ON TABLE idsWithMinMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatViewOpt_insert ON TABLE idsWithMinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWith2MinMatView_insert ON TABLE idsWith2MinMatView COLUMN id;
PARTITION PROCEDURE idsWith2MinMatViewOpt_insert ON TABLE idsWith2MinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWith4MinMatView_insert ON TABLE idsWith4MinMatView COLUMN id;
PARTITION PROCEDURE idsWith4MinMatViewOpt_insert ON TABLE idsWith4MinMatViewOpt COLUMN id;

CREATE PROCEDURE ids_group_id_update AS
  UPDATE ids SET group_id = ? WHERE id = ?;
CREATE PROCEDURE idsWithMatView_group_id_update AS
  UPDATE idsWithMatView SET group_id = ? WHERE id = ?;
PARTITION PROCEDURE ids_group_id_update ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_group_id_update ON TABLE idsWithMatView COLUMN id;

CREATE PROCEDURE ids_value_update AS
  UPDATE ids SET value = ? WHERE id = ?;
CREATE PROCEDURE idsWithMatView_value_update AS
  UPDATE idsWithMatView SET value = ? WHERE id = ?;
PARTITION PROCEDURE ids_value_update ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_value_update ON TABLE idsWithMatView COLUMN id;

CREATE PROCEDURE ids_delete AS
  DELETE FROM ids WHERE (id = ?);
CREATE PROCEDURE idsWithMatView_delete AS
  DELETE FROM idsWithMatView WHERE (id = ?);
CREATE PROCEDURE idsWithMinMatView_delete AS
  DELETE FROM idsWithMinMatView WHERE (id = ?);
CREATE PROCEDURE idsWithMinMatViewOpt_delete AS
  DELETE FROM idsWithMinMatViewOpt WHERE (id = ?);
CREATE PROCEDURE idsWith2MinMatView_delete AS
  DELETE FROM idsWith2MinMatView WHERE (id = ?);
CREATE PROCEDURE idsWith2MinMatViewOpt_delete AS
  DELETE FROM idsWith2MinMatViewOpt WHERE (id = ?);
CREATE PROCEDURE idsWith4MinMatView_delete AS
  DELETE FROM idsWith4MinMatView WHERE (id = ?);
CREATE PROCEDURE idsWith4MinMatViewOpt_delete AS
  DELETE FROM idsWith4MinMatViewOpt WHERE (id = ?);
PARTITION PROCEDURE ids_delete ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_delete ON TABLE idsWithMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatView_delete ON TABLE idsWithMinMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatViewOpt_delete ON TABLE idsWithMinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWith2MinMatView_delete ON TABLE idsWith2MinMatView COLUMN id;
PARTITION PROCEDURE idsWith2MinMatViewOpt_delete ON TABLE idsWith2MinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWith4MinMatView_delete ON TABLE idsWith4MinMatView COLUMN id;
PARTITION PROCEDURE idsWith4MinMatViewOpt_delete ON TABLE idsWith4MinMatViewOpt COLUMN id;

CREATE VIEW id_count (
	group_id,
	total_id,
	sum_value
) AS SELECT 
	group_id,
	COUNT(*),
	SUM(value) 
FROM idsWithMatView GROUP BY group_id;

CREATE VIEW id_min (
	group_id,
	total_id,
	min_id
) AS SELECT 
	group_id,
	COUNT(*),
	MIN(id) 
FROM idsWithMinMatView GROUP BY group_id;
CREATE INDEX idWithMinMatView_idx ON idsWithMinMatView (group_id);
CREATE INDEX idWithMinMatView_idx_nouse ON idsWithMinMatView (group_id, value);

CREATE VIEW id_min_opt (
  group_id,
  total_id,
  min_id
) AS SELECT 
  group_id,
  COUNT(*),
  MIN(id) 
FROM idsWithMinMatViewOpt GROUP BY group_id;
CREATE INDEX idWithMinMatViewOpt_idx ON idsWithMinMatViewOpt (group_id, id);
CREATE INDEX idWithMinMatViewOpt_idx_nouse ON idsWithMinMatViewOpt (group_id);

CREATE VIEW id_2min (
  group_id,
  total_id,
  min_id,
  min_val
) AS SELECT 
  group_id,
  COUNT(*),
  MIN(id),
  MIN(value)
FROM idsWith2MinMatView GROUP BY group_id;
CREATE INDEX idWith2MinMatView_idx ON idsWith2MinMatView (group_id);

CREATE VIEW id_2min_opt (
  group_id,
  total_id,
  min_id,
  min_val
) AS SELECT 
  group_id,
  COUNT(*),
  MIN(id),
  MIN(value)
FROM idsWith2MinMatViewOpt GROUP BY group_id;
CREATE INDEX idWith2MinMatViewOpt_idx1 ON idsWith2MinMatViewOpt (group_id, id);
CREATE INDEX idWith2MinMatViewOpt_idx2 ON idsWith2MinMatViewOpt (group_id, value);

CREATE VIEW id_4min (
  group_id,
  total_id,
  min_v1,
  min_v2,
  min_v3,
  min_v4
) AS SELECT 
  group_id,
  COUNT(*),
  MIN(v1),
  MIN(v2),
  MIN(v3),
  MIN(v4),
FROM idsWith4MinMatView GROUP BY group_id;
CREATE INDEX idWith4MinMatView_idx ON idsWith4MinMatView (group_id);

CREATE VIEW id_4min_opt (
  group_id,
  total_id,
  min_v1,
  min_v2,
  min_v3,
  min_v4
) AS SELECT 
  group_id,
  COUNT(*),
  MIN(v1),
  MIN(v2),
  MIN(v3),
  MIN(v4),
FROM idsWith4MinMatViewOpt GROUP BY group_id;
CREATE INDEX idWith4MinMatViewOpt_idx1 ON idsWith4MinMatViewOpt (group_id, v1);
CREATE INDEX idWith4MinMatViewOpt_idx2 ON idsWith4MinMatViewOpt (group_id, v2);
CREATE INDEX idWith4MinMatViewOpt_idx3 ON idsWith4MinMatViewOpt (group_id, v3);
CREATE INDEX idWith4MinMatViewOpt_idx4 ON idsWith4MinMatViewOpt (group_id, v4);
