CREATE TABLE cells
(
  cell_id INTEGER NOT NULL,
  occupied TINYINT DEFAULT 0,
  PRIMARY KEY (cell_id)
);

CREATE TABLE metadata
(
  pk INTEGER NOT NULL,
  numrows INT DEFAULT 1,
  numcols INT DEFAULT 1,
  generation BIGINT DEFAULT 0,
  PRIMARY KEY (pk)
);
