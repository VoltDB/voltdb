-- Loads the test UDF's (user-defined functions) defined in
-- UserDefinedTestFunctions.java, which use various data types, and with
-- various numbers of arguments. The associated (DROP and) CREATE FUNCTION
-- statements are in a separate file, UserDefinedTestFunctions-DDL.sql,
-- which may be used by itself (as in JUnit tests) or with this one
-- (as in SQL-grammar-generator tests).

-- First, remove the class containing all the test UDF's (user-defined
-- functions), in case it was loaded previously
remove classes org.voltdb_testfuncs.UserDefinedTestFunctions;

-- Then, load the .jar file containing the class containing those test UDF's
load classes testfuncs.jar;
