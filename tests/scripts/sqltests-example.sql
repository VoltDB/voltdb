--
-- This example demonstrates syntax understood by sqltests.py.
--

/*

Metadata may follow a sql statement. Metadata is written
"keyword: value". Parsing of keywords is pretty strict:

* Keywords are associated with the preceding sql statement.
* Keywords must not preceeded by whitespace (a parsing limitation).
* Keywords must exist inside comments, multiline and -- supported.
  A keyword value must not be split between two comments (multi-
  line comments must be used for tables, therefore).


Example of "desc" keyword
-------------------------
desc: A test demonstrating sql support for compound predicates.

A comment for the user's benefit, opaque to the harness. A good
description briefly describe a test's intent.  If a description
is not provided, a default will be generated.


Example of "rows" keyword
-------------------------
rows: 1

The rowcount a DML statement is expected to return.


Example of "table" keyword
--------------------------
table:
11,12,13
21,22,23
31,32,33

The literal table a query is expected to return. Support for
normalizers and references to tables (to allow re-use of a table) will
be added soon.

  NOTE:
  * columns are delimitted by comma (,).
  * rows are delimitted by newlines (\n).

*/

--
--  A valid sqltest.py input
--

-- CREATE statements will be identified as DDL and moved in to test.ddl.

CREATE TABLE T (
  T_ID INTEGER DEFAULT '0' NOT NULL,
  T_ITEM INTEGER,
  PRIMARY KEY (T_ID)
);

INSERT INTO T VALUES (1,2);
-- rows: 1

INSERT INTO T VALUES (1,2);
/*
desc: Insert a duplicate value.
rows: 0
*/

INSERT INTO T VALUES (3,4);
INSERT INTO T VALUES (5,6);
-- desc: A final boring insert
-- rows: 1

-- an intentional error in test metadata (rows should be 0)
INSERT INTO T VALUES (5,6);
-- desc: Show a failure
-- desc: Have two descriptions
-- rows: 1

select T_ID, T_ITEM from T where T_ID=5;
-- table:
-- 5,6

SELECT T_ID, T_ITEM from T where T_ID=1;
/*
table:
1,2
3,4
5,6
*/

select * from T;
/*
desc: Select *
*/
-- table:
-- 1,2
-- 3,4
-- 5,6

/*
table:
100
200 */

-- table:
--300
--400

/*
table:
500
600

table:
700
800
*/


--
-- The generated stored procedure produces a single summary
-- output table that is returned to the invoking client.
-- For this example, the table would contain the following
-- data.
--
/*
TEST				RESULT
=======================		==========
Statement1			PASS
Insert a duplicate value	PASS
Statement3	   		UNVERIFIED
A final boring insert		PASS
Show a failure 			FAIL
Statement6			PASS
Statement7			PASS
Select *			PASS
*/


