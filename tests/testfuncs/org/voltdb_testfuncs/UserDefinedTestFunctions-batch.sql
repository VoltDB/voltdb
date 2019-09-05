-- This file simply runs the UserDefinedTestFunctions-DDL.sql file in batch
-- mode, to make it faster when being run in sqlcmd, including when run by the
-- SQL-grammar-generator tests.  Batch statements cannot be used directly in
-- UserDefinedTestFunctions-DDL.sql, without breaking the
-- org.voltdb.regressionsuites.TestUserDefinedFunctions JUnit test, which also
-- uses that file.

file -batch UserDefinedTestFunctions-DDL.sql;
