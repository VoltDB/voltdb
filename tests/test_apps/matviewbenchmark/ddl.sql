CREATE TABLE ids
(
  id bigint NOT NULL,
  group_id bigint, 
  PRIMARY KEY (id)
);

CREATE TABLE idsWithMatView
(
  id bigint NOT NULL,
  group_id bigint,
  PRIMARY KEY (id)
);

PARTITION TABLE ids ON COLUMN id;
PARTITION TABLE idsWithMatView ON COLUMN id;

CREATE PROCEDURE ids_insert AS
  INSERT INTO ids VALUES (?,?);
CREATE PROCEDURE idsWithMatView_insert AS
  INSERT INTO idsWithMatView VALUES (?,?);
PARTITION PROCEDURE ids_insert ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_insert ON TABLE idsWithMatView COLUMN id;

CREATE PROCEDURE ids_delete AS
  DELETE FROM ids WHERE (id = ?);
CREATE PROCEDURE idsWithMatView_delete AS
  DELETE FROM idsWithMatView WHERE (id = ?);
PARTITION PROCEDURE ids_delete ON TABLE ids COLUMN id;
PARTITION PROCEDURE idsWithMatView_delete ON TABLE idsWithMatView COLUMN id;

CREATE VIEW id_count (
	group_id,
	total_id
) AS SELECT 
	group_id,
	COUNT(*) 
FROM idsWithMatView GROUP BY group_id;
