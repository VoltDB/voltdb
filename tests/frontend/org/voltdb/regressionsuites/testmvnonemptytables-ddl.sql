drop table alpha if exists;
drop table beta  if exists;
drop table empty if exists;
CREATE TABLE alpha (
    a      INTEGER NOT NULL,
    b      INTEGER NOT NULL
);
PARTITION TABLE alpha ON COLUMN a;

CREATE TABLE beta (
    a      INTEGER NOT NULL,
    b      INTEGER NOT NULL
);

CREATE TABLE empty (
    a       INTEGER NOT NULL,
    b       INTEGER NOT NULL
);

drop table safeTestFull  if exists;
drop table safeTestEmpty if exists;
CREATE TABLE safeTestFull (
    a       INTEGER      DEFAULT '100'  NOT NULL,
    astr    VARCHAR(10)  DEFAULT 'astr' NOT NULL,
    b       INTEGER      DEFAULT '101'  NOT NULL,
    bstr    VARCHAR(10)  DEFAULT 'bstr' NOT NULL
);

CREATE TABLE safeTestEmpty (
    a       INTEGER      DEFAULT '200'  NOT NULL,
    astr    VARCHAR(10)  DEFAULT 'astr' NOT NULL,
    b       INTEGER      DEFAULT '201'  NOT NULL,
    bstr    VARCHAR(10)  DEFAULT 'bstr' NOT NULL
);
