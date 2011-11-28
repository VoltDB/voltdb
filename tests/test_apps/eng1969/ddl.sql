-- Partitioned Data Table
CREATE TABLE backed
(
   rowid_group               BIGINT         NOT NULL
,  rowid                     BIGINT         NOT NULL
,  atime                     BIGINT         NOT NULL
,  payload                   VARBINARY(512) NOT NULL
, PRIMARY KEY (rowid_group, rowid)
);

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

