-- partitioned in test on pkey
CREATE TABLE A (
 PKEY          INTEGER NOT NULL,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);

-- replicated in test
CREATE TABLE B (
 PKEY          INTEGER,
 I             INTEGER,
 PRIMARY KEY (PKEY)
);
