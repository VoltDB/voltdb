-- Loads the test UDF's (user-defined functions) defined in
-- UserDefinedTestFunctions.java, which use various data types, and with
-- various numbers of arguments. The associated (DROP and) CREATE FUNCTION
-- statements are in a separate file, UserDefinedTestFunctions-DDL.sql,
-- which may be used by itself (as in JUnit tests) or with this one
-- (as in SQL-grammar-generator tests).

load classes testfuncs.jar;
