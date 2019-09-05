-- Partitioned Data Table
CREATE TABLE backed
(
   rowid_group               BIGINT         NOT NULL
,  rowid                     BIGINT         NOT NULL
,  atime                     BIGINT         NOT NULL
,  payload                   VARBINARY(512) NOT NULL
, PRIMARY KEY (rowid_group, rowid)
);

PARTITION TABLE backed ON COLUMN rowid_group;

create index atime_tree_idx on backed (atime ASC);

-- Grouping view over Partitioned Data Table
CREATE VIEW partitioned_table_group
(
  rowid_group
, record_count
)
AS
   SELECT rowid_group
        , COUNT(*)
     FROM backed
 GROUP BY rowid_group;
 
 CREATE PROCEDURE PARTITION ON TABLE backed COLUMN rowid_group PARAMETER 1 FROM CLASS eng1969.procedures.CreateKey;
 CREATE PROCEDURE PARTITION ON TABLE backed COLUMN rowid_group PARAMETER 1 FROM CLASS eng1969.procedures.UpdateKey;

