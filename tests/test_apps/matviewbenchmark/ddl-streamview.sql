CREATE STREAM ids PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint
);

CREATE STREAM idsWithMatView PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint
);

CREATE STREAM idsWithMinMatView PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint
);

CREATE STREAM idsWithMinMatViewOpt PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id bigint,
  value bigint
);

CREATE STREAM idsWith4MinMatView PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id bigint,
  v1 bigint,
  v2 bigint,
  v3 bigint,
  v4 bigint
);

CREATE STREAM idsWith4MinMatViewOpt PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id bigint,
  v1 bigint,
  v2 bigint,
  v3 bigint,
  v4 bigint
);

CREATE STREAM idsWithMultiGroupsMinMatView PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id_1 bigint,
  group_id_2 bigint,
  value bigint
);

CREATE STREAM idsWithMultiGroupsMinMatViewOpt PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id_1 bigint,
  group_id_2 bigint,
  value bigint
);

CREATE STREAM idsWithMultiGroupsMinMatViewBestOpt PARTITION ON COLUMN id
(
  id bigint NOT NULL,
  group_id_1 bigint,
  group_id_2 bigint,
  value bigint
);

--
-- Procedures that insert rows into the streams
--
CREATE PROCEDURE ids_insert AS
  INSERT INTO ids VALUES (?,?,?);
CREATE PROCEDURE idsWithMatView_insert AS
  INSERT INTO idsWithMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatView_insert AS
  INSERT INTO idsWithMinMatView VALUES (?,?,?);
CREATE PROCEDURE idsWithMinMatViewOpt_insert AS
  INSERT INTO idsWithMinMatViewOpt VALUES (?,?,?);
CREATE PROCEDURE idsWith4MinMatView_insert AS
  INSERT INTO idsWith4MinMatView VALUES (?,?,?,?,?,?);
CREATE PROCEDURE idsWith4MinMatViewOpt_insert AS
  INSERT INTO idsWith4MinMatViewOpt VALUES (?,?,?,?,?,?);
CREATE PROCEDURE idsWithMultiGroupsMinMatView_insert AS
  INSERT INTO idsWithMultiGroupsMinMatView VALUES (?,?,?,?);
CREATE PROCEDURE idsWithMultiGroupsMinMatViewOpt_insert AS
  INSERT INTO idsWithMultiGroupsMinMatViewOpt VALUES (?,?,?,?);
CREATE PROCEDURE idsWithMultiGroupsMinMatViewBestOpt_insert AS
  INSERT INTO idsWithMultiGroupsMinMatViewBestOpt VALUES (?,?,?,?);

PARTITION PROCEDURE ids_insert ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_insert ON TABLE idsWithMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatView_insert ON TABLE idsWithMinMatView COLUMN id;
PARTITION PROCEDURE idsWithMinMatViewOpt_insert ON TABLE idsWithMinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWith4MinMatView_insert ON TABLE idsWith4MinMatView COLUMN id;
PARTITION PROCEDURE idsWith4MinMatViewOpt_insert ON TABLE idsWith4MinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWithMultiGroupsMinMatView_insert ON TABLE idsWithMultiGroupsMinMatView COLUMN id;
PARTITION PROCEDURE idsWithMultiGroupsMinMatViewOpt_insert ON TABLE idsWithMultiGroupsMinMatViewOpt COLUMN id;
PARTITION PROCEDURE idsWithMultiGroupsMinMatViewBestOpt_insert ON TABLE idsWithMultiGroupsMinMatViewBestOpt COLUMN id;

--
-- Views on streams
--
CREATE VIEW id_count (
  id,
  group_id,
  total_id,
  sum_value
) AS SELECT 
  id,
  group_id,
  COUNT(*),
  SUM(value) 
FROM idsWithMatView GROUP BY id, group_id;

CREATE VIEW id_min (
  id,
  group_id,
  total_id,
  sum_value,
  min_id
) AS SELECT 
  id,
  group_id,
  COUNT(*),
  SUM(value),
  MIN(id) 
FROM idsWithMinMatView GROUP BY id, group_id;

CREATE VIEW id_min_opt (
  id,
  group_id,
  total_id,
  sum_value,
  min_id
) AS SELECT 
  id,
  group_id,
  COUNT(*),
  SUM(value),
  MIN(id) 
FROM idsWithMinMatViewOpt GROUP BY id, group_id;

CREATE VIEW id_4min (
  id,
  group_id,
  total_id,
  min_v1,
  min_v2,
  min_v3,
  min_v4
) AS SELECT 
  id,
  group_id,
  COUNT(*),
  MIN(v1),
  MAX(v2),
  MIN(v3),
  MAX(v4)
FROM idsWith4MinMatView GROUP BY id, group_id;

CREATE VIEW id_4min_opt (
  id,
  group_id,
  total_id,
  min_v1,
  min_v2,
  min_v3,
  min_v4
) AS SELECT 
  id,
  group_id,
  COUNT(*),
  MIN(v1),
  MAX(v2),
  MIN(v3),
  MAX(v4)
FROM idsWith4MinMatViewOpt GROUP BY id, group_id;

CREATE VIEW id_multi_group_min (
  id,
  group_id_1,
  group_id_2,
  total_id,
  sum_value,
  min_id
) AS SELECT
  id,
  group_id_1,
  group_id_2,
  COUNT(*),
  SUM(value),
  MIN(id)
FROM idsWithMultiGroupsMinMatView GROUP BY id, group_id_1, group_id_2;

CREATE VIEW id_multi_group_min_opt (
  id,
  group_id_1,
  group_id_2,
  total_id,
  sum_value,
  min_id
) AS SELECT
  id,
  group_id_1,
  group_id_2,
  COUNT(*),
  SUM(value),
  MIN(id)
FROM idsWithMultiGroupsMinMatViewOpt GROUP BY id, group_id_1, group_id_2;

CREATE VIEW id_multi_group_min_best_opt (
  id,
  group_id_1,
  group_id_2,
  total_id,
  sum_value,
  min_id
) AS SELECT
  id,
  group_id_1,
  group_id_2,
  COUNT(*),
  SUM(value),
  MIN(id)
FROM idsWithMultiGroupsMinMatViewBestOpt GROUP BY id, group_id_1, group_id_2;
