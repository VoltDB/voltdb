CREATE TABLE ids
(
  id BIGINT NOT NULL,
  group_id BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE ids ON COLUMN id;

CREATE TABLE idsWithMatView
(
  id BIGINT NOT NULL,
  group_id BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWithMatView ON COLUMN id;

CREATE TABLE idsWithMinMatView
(
  id BIGINT NOT NULL,
  group_id BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWithMinMatView ON COLUMN id;

CREATE TABLE idsWithMinMatViewOpt
(
  id BIGINT NOT NULL,
  group_id BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWithMinMatViewOpt ON COLUMN id;

CREATE TABLE idsWith4MinMatView
(
  id BIGINT NOT NULL,
  group_id BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  v3 BIGINT,
  v4 BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWith4MinMatView ON COLUMN id;

CREATE TABLE idsWith4MinMatViewOpt
(
  id BIGINT NOT NULL,
  group_id BIGINT,
  v1 BIGINT,
  v2 BIGINT,
  v3 BIGINT,
  v4 BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWith4MinMatViewOpt ON COLUMN id;

CREATE TABLE idsWithMultiGroupsMinMatView
(
  id BIGINT NOT NULL,
  group_id_1 BIGINT,
  group_id_2 BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWithMultiGroupsMinMatView ON COLUMN id;

CREATE TABLE idsWithMultiGroupsMinMatViewOpt
(
  id BIGINT NOT NULL,
  group_id_1 BIGINT,
  group_id_2 BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWithMultiGroupsMinMatViewOpt ON COLUMN id;

CREATE TABLE idsWithMultiGroupsMinMatViewBestOpt
(
  id BIGINT NOT NULL,
  group_id_1 BIGINT,
  group_id_2 BIGINT,
  value BIGINT,
  PRIMARY KEY (id)
);
PARTITION TABLE idsWithMultiGroupsMinMatViewBestOpt ON COLUMN id;

CREATE TABLE noJoinedViewSrc1 (
  id BIGINT NOT NULL PRIMARY KEY,
  G0 BIGINT DEFAULT '0' NOT NULL,
  G1 BIGINT DEFAULT '0' NOT NULL,
  C2 BIGINT DEFAULT '0' NOT NULL,
  C3 BIGINT DEFAULT '0' NOT NULL,
  C8 BIGINT DEFAULT '0' NOT NULL
);
PARTITION TABLE noJoinedViewSrc1 ON COLUMN id;

CREATE TABLE noJoinedViewSrc2 (
  G0 BIGINT NOT NULL PRIMARY KEY
);

CREATE TABLE joinedViewSrc1 (
  id BIGINT NOT NULL PRIMARY KEY,
  G0 BIGINT DEFAULT '0' NOT NULL,
  G1 BIGINT DEFAULT '0' NOT NULL,
  C2 BIGINT DEFAULT '0' NOT NULL,
  C3 BIGINT DEFAULT '0' NOT NULL,
  C8 BIGINT DEFAULT '0' NOT NULL
);
PARTITION TABLE joinedViewSrc1 ON COLUMN id;
CREATE INDEX idxJoinedViewSrc1 ON joinedViewSrc1(G1, C3);

CREATE TABLE joinedViewSrc2 (
  G0 BIGINT NOT NULL PRIMARY KEY
);

CREATE PROCEDURE ids_insert
  PARTITION ON TABLE ids COLUMN id AS
  INSERT INTO ids VALUES (?,?,?);
CREATE PROCEDURE idsWithMatView_insert
  PARTITION ON TABLE idsWithMatView COLUMN id AS
  INSERT INTO idsWithMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatView_insert
  PARTITION ON TABLE idsWithMinMatView COLUMN id AS
  INSERT INTO idsWithMinMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatViewOpt_insert
  PARTITION ON TABLE idsWithMinMatViewOpt COLUMN id AS
  INSERT INTO idsWithMinMatViewOpt VALUES (?,?,?);
CREATE PROCEDURE idsWith4MinMatView_insert
  PARTITION ON TABLE idsWith4MinMatView COLUMN id AS
  INSERT INTO idsWith4MinMatView VALUES (?,?,?,?,?,?);
CREATE PROCEDURE idsWith4MinMatViewOpt_insert
  PARTITION ON TABLE idsWith4MinMatViewOpt COLUMN id AS
  INSERT INTO idsWith4MinMatViewOpt VALUES (?,?,?,?,?,?);
CREATE PROCEDURE idsWithMultiGroupsMinMatView_insert
  PARTITION ON TABLE idsWithMultiGroupsMinMatView COLUMN id AS
  INSERT INTO idsWithMultiGroupsMinMatView VALUES (?,?,?,?);
CREATE PROCEDURE idsWithMultiGroupsMinMatViewOpt_insert
  PARTITION ON TABLE idsWithMultiGroupsMinMatViewOpt COLUMN id AS
  INSERT INTO idsWithMultiGroupsMinMatViewOpt VALUES (?,?,?,?);
CREATE PROCEDURE idsWithMultiGroupsMinMatViewBestOpt_insert
  PARTITION ON TABLE idsWithMultiGroupsMinMatViewBestOpt COLUMN id AS
  INSERT INTO idsWithMultiGroupsMinMatViewBestOpt VALUES (?,?,?,?);
CREATE PROCEDURE noJoinedViewSrc1_insert
  PARTITION ON TABLE noJoinedViewSrc1 COLUMN id AS
  INSERT INTO noJoinedViewSrc1 VALUES (?,?,?,?,?,?);
CREATE PROCEDURE noJoinedViewSrc2_insert AS
  INSERT INTO noJoinedViewSrc2 VALUES (?);
CREATE PROCEDURE joinedViewSrc1_insert
  PARTITION ON TABLE joinedViewSrc1 COLUMN id AS
  INSERT INTO joinedViewSrc1 VALUES (?,?,?,?,?,?);
CREATE PROCEDURE joinedViewSrc2_insert AS
  INSERT INTO joinedViewSrc2 VALUES (?);

CREATE PROCEDURE ids_group_id_update
  PARTITION ON TABLE ids COLUMN id AS
  UPDATE ids SET group_id = ? WHERE id = ?;
CREATE PROCEDURE idsWithMatView_group_id_update
  PARTITION ON TABLE idsWithMatView COLUMN id AS
  UPDATE idsWithMatView SET group_id = ? WHERE id = ?;

CREATE PROCEDURE ids_value_update
  PARTITION ON TABLE ids COLUMN id AS
  UPDATE ids SET value = ? WHERE id = ?;
CREATE PROCEDURE idsWithMatView_value_update
  PARTITION ON TABLE idsWithMatView COLUMN id AS
  UPDATE idsWithMatView SET value = ? WHERE id = ?;

CREATE PROCEDURE ids_delete
  PARTITION ON TABLE ids COLUMN id AS
  DELETE FROM ids WHERE (id = ?);
CREATE PROCEDURE idsWithMatView_delete
  PARTITION ON TABLE idsWithMatView COLUMN id AS
  DELETE FROM idsWithMatView WHERE (id = ?);
CREATE PROCEDURE idsWithMinMatView_delete
  PARTITION ON TABLE idsWithMinMatView COLUMN id AS
  DELETE FROM idsWithMinMatView WHERE (id = ?);
CREATE PROCEDURE idsWithMinMatViewOpt_delete
  PARTITION ON TABLE idsWithMinMatViewOpt COLUMN id AS
  DELETE FROM idsWithMinMatViewOpt WHERE (id = ?);
CREATE PROCEDURE idsWith4MinMatView_delete
  PARTITION ON TABLE idsWith4MinMatView COLUMN id AS
  DELETE FROM idsWith4MinMatView WHERE (id = ?);
CREATE PROCEDURE idsWith4MinMatViewOpt_delete
  PARTITION ON TABLE idsWith4MinMatViewOpt COLUMN id AS
  DELETE FROM idsWith4MinMatViewOpt WHERE (id = ?);
CREATE PROCEDURE idsWithMultiGroupsMinMatView_delete
  PARTITION ON TABLE idsWithMultiGroupsMinMatView COLUMN id AS
  DELETE FROM idsWithMultiGroupsMinMatView WHERE (id = ?);
CREATE PROCEDURE idsWithMultiGroupsMinMatViewOpt_delete
  PARTITION ON TABLE idsWithMultiGroupsMinMatViewOpt COLUMN id AS
  DELETE FROM idsWithMultiGroupsMinMatViewOpt WHERE (id = ?);
CREATE PROCEDURE idsWithMultiGroupsMinMatViewBestOpt_delete
  PARTITION ON TABLE idsWithMultiGroupsMinMatViewBestOpt COLUMN id AS
  DELETE FROM idsWithMultiGroupsMinMatViewBestOpt WHERE (id = ?);
CREATE PROCEDURE noJoinedViewSrc1_delete
  PARTITION ON TABLE noJoinedViewSrc1 COLUMN id AS
  DELETE FROM noJoinedViewSrc1 WHERE (id = ?);
CREATE PROCEDURE noJoinedViewSrc2_delete AS
  DELETE FROM noJoinedViewSrc2 WHERE (G0 = ?);
CREATE PROCEDURE joinedViewSrc1_delete
  PARTITION ON TABLE joinedViewSrc1 COLUMN id AS
  DELETE FROM joinedViewSrc1 WHERE (id = ?);
CREATE PROCEDURE joinedViewSrc2_delete AS
  DELETE FROM joinedViewSrc2 WHERE (G0 = ?);

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
  sum_value,
  min_id
) AS SELECT 
  group_id,
  COUNT(*),
  SUM(value),
  MIN(id) 
FROM idsWithMinMatView GROUP BY group_id;
CREATE INDEX idWithMinMatView_idx ON idsWithMinMatView (group_id);

CREATE VIEW id_min_opt (
  group_id,
  total_id,
  sum_value,
  min_id
) AS SELECT 
  group_id,
  COUNT(*),
  SUM(value),
  MIN(id) 
FROM idsWithMinMatViewOpt GROUP BY group_id;
CREATE INDEX idWithMinMatViewOpt_idx ON idsWithMinMatViewOpt (group_id, id);

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
  MAX(v2),
  MIN(v3),
  MAX(v4)
FROM idsWith4MinMatView GROUP BY group_id;
CREATE INDEX idWith4MinMatView_idx0 ON idsWith4MinMatView (group_id);
CREATE INDEX idWith4MinMatView_idx1 ON idsWith4MinMatView (v1, group_id);
CREATE INDEX idWith4MinMatView_idx2 ON idsWith4MinMatView (v2, group_id);
CREATE INDEX idWith4MinMatView_idx3 ON idsWith4MinMatView (v3, group_id);
CREATE INDEX idWith4MinMatView_idx4 ON idsWith4MinMatView (v4, group_id);

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
  MAX(v2),
  MIN(v3),
  MAX(v4)
FROM idsWith4MinMatViewOpt GROUP BY group_id;
CREATE INDEX idWith4MinMatViewOpt_idx0 ON idsWith4MinMatViewOpt (group_id);
CREATE INDEX idWith4MinMatViewOpt_idx1 ON idsWith4MinMatViewOpt (group_id, v1);
CREATE INDEX idWith4MinMatViewOpt_idx2 ON idsWith4MinMatViewOpt (group_id, v2);
CREATE INDEX idWith4MinMatViewOpt_idx3 ON idsWith4MinMatViewOpt (group_id, v3);
CREATE INDEX idWith4MinMatViewOpt_idx4 ON idsWith4MinMatViewOpt (group_id, v4);


CREATE VIEW id_multi_group_min (
  group_id_1,
  group_id_2,
  total_id,
  sum_value,
  min_id
) AS SELECT
  group_id_1,
  group_id_2,
  COUNT(*),
  SUM(value),
  MIN(id)
FROM idsWithMultiGroupsMinMatView GROUP BY group_id_1, group_id_2;

CREATE VIEW id_multi_group_min_opt (
  group_id_1,
  group_id_2,
  total_id,
  sum_value,
  min_id
) AS SELECT
  group_id_1,
  group_id_2,
  COUNT(*),
  SUM(value),
  MIN(id)
FROM idsWithMultiGroupsMinMatViewOpt GROUP BY group_id_1, group_id_2;
CREATE INDEX idsWithMultiGroupsMinMatViewOpt_idx ON idsWithMultiGroupsMinMatViewOpt (group_id_1);

CREATE VIEW id_multi_group_min_best_opt (
  group_id_1,
  group_id_2,
  total_id,
  sum_value,
  min_id
) AS SELECT
  group_id_1,
  group_id_2,
  COUNT(*),
  SUM(value),
  MIN(id)
FROM idsWithMultiGroupsMinMatViewBestOpt GROUP BY group_id_1, group_id_2;
CREATE INDEX idsWithMultiGroupsMinMatViewBestOpt_idx ON idsWithMultiGroupsMinMatViewBestOpt (group_id_1, group_id_2, id, value);

CREATE VIEW joinedView (G1, CNT, C2, SUMC2, C3, CNTC8) AS
SELECT T1.G1, COUNT(*), MAX(T1.C2), SUM(T1.C2),
                        MIN(T1.C3), COUNT(T1.C8)
FROM joinedViewSrc1 T1 JOIN joinedViewSrc2 T2 ON T1.G0 = T2.G0
GROUP BY T1.G1;
