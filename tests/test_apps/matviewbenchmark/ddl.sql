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

PARTITION TABLE ids ON COLUMN id;
PARTITION TABLE idsWithMatView ON COLUMN id;
PARTITION TABLE idsWithMinMatView ON COLUMN id;
PARTITION TABLE idsWithMinMatViewOpt ON COLUMN id;

CREATE PROCEDURE ids_insert AS
  INSERT INTO ids VALUES (?,?,?);
CREATE PROCEDURE idsWithMatView_insert AS
  INSERT INTO idsWithMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatView_insert AS
  INSERT INTO idsWithMinMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatViewOpt_insert AS
  INSERT INTO idsWithMinMatViewOpt VALUES (?,?,?);
PARTITION PROCEDURE ids_insert ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_insert ON TABLE idsWithMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatView_insert ON TABLE idsWithMinMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatViewOpt_insert ON TABLE idsWithMinMatViewOpt COLUMN id;

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
PARTITION PROCEDURE ids_delete ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_delete ON TABLE idsWithMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatView_delete ON TABLE idsWithMinMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatViewOpt_delete ON TABLE idsWithMinMatViewOpt COLUMN id;

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
