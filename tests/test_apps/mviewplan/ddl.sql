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

PARTITION TABLE idsWith4MinMatView ON COLUMN id;
PARTITION TABLE idsWith4MinMatViewOpt ON COLUMN id;

CREATE PROCEDURE idsWith4MinMatView_insert AS
  INSERT INTO idsWith4MinMatView VALUES (?,?,?,?,?,?);
CREATE PROCEDURE idsWith4MinMatViewOpt_insert AS
  INSERT INTO idsWith4MinMatViewOpt VALUES (?,?,?,?,?,?);

PARTITION PROCEDURE idsWith4MinMatView_insert ON TABLE idsWith4MinMatView COLUMN id;
PARTITION PROCEDURE idsWith4MinMatViewOpt_insert ON TABLE idsWith4MinMatViewOpt COLUMN id;


CREATE PROCEDURE idsWith4MinMatView_delete AS
  DELETE FROM idsWith4MinMatView WHERE (id = ?);
CREATE PROCEDURE idsWith4MinMatViewOpt_delete AS
  DELETE FROM idsWith4MinMatViewOpt WHERE (id = ?);

PARTITION PROCEDURE idsWith4MinMatView_delete ON TABLE idsWith4MinMatView COLUMN id;
PARTITION PROCEDURE idsWith4MinMatViewOpt_delete ON TABLE idsWith4MinMatViewOpt COLUMN id;

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
