-- Loads and creates the test UDF's (user-defined functions) defined in
-- UserDefinedTestFunctions.java, using various data types, and with
-- various numbers of arguments.

-- First, drop all the test UDF's (user-defined functions), and remove the class
-- containing them, in case they were loaded and created previously

DROP FUNCTION add2Tinyint   IF EXISTS;
DROP FUNCTION add2Smallint  IF EXISTS;
DROP FUNCTION add2Integer   IF EXISTS;
DROP FUNCTION add2Bigint    IF EXISTS;
DROP FUNCTION add2Float     IF EXISTS;
DROP FUNCTION add2TinyintBoxed  IF EXISTS;
DROP FUNCTION add2SmallintBoxed IF EXISTS;
DROP FUNCTION add2IntegerBoxed  IF EXISTS;
DROP FUNCTION add2BigintBoxed   IF EXISTS;
DROP FUNCTION add2FloatBoxed    IF EXISTS;
DROP FUNCTION add2Decimal   IF EXISTS;
DROP FUNCTION add2Varchar   IF EXISTS;
DROP FUNCTION add2Varbinary IF EXISTS;
DROP FUNCTION add2VarbinaryBoxed  IF EXISTS;
DROP FUNCTION addYearsToTimestamp IF EXISTS;
DROP FUNCTION add2GeographyPoint  IF EXISTS;
DROP FUNCTION addGeographyPointToGeography IF EXISTS;

DROP FUNCTION pi_UDF        IF EXISTS;
DROP FUNCTION pi_UDF_Boxed  IF EXISTS;
DROP FUNCTION abs_TINYINT   IF EXISTS;
DROP FUNCTION abs_SMALLINT  IF EXISTS;
DROP FUNCTION abs_INTEGER   IF EXISTS;
DROP FUNCTION abs_BIGINT    IF EXISTS;
DROP FUNCTION abs_FLOAT     IF EXISTS;
DROP FUNCTION abs_TINYINT_Boxed  IF EXISTS;
DROP FUNCTION abs_SMALLINT_Boxed IF EXISTS;
DROP FUNCTION abs_INTEGER_Boxed  IF EXISTS;
DROP FUNCTION abs_BIGINT_Boxed   IF EXISTS;
DROP FUNCTION abs_FLOAT_Boxed    IF EXISTS;
DROP FUNCTION abs_DECIMAL   IF EXISTS;
DROP FUNCTION reverse       IF EXISTS;
DROP FUNCTION numRings      IF EXISTS;
DROP FUNCTION numPoints_UDF IF EXISTS;

DROP FUNCTION mod_TINYINT  IF EXISTS;
DROP FUNCTION mod_SMALLINT IF EXISTS;
DROP FUNCTION mod_INTEGER  IF EXISTS;
DROP FUNCTION mod_BIGINT   IF EXISTS;
DROP FUNCTION mod_FLOAT    IF EXISTS;
DROP FUNCTION mod_TINYINT_Boxed  IF EXISTS;
DROP FUNCTION mod_SMALLINT_Boxed IF EXISTS;
DROP FUNCTION mod_INTEGER_Boxed  IF EXISTS;
DROP FUNCTION mod_BIGINT_Boxed   IF EXISTS;
DROP FUNCTION mod_FLOAT_Boxed    IF EXISTS;
DROP FUNCTION mod_DECIMAL  IF EXISTS;
DROP FUNCTION btrim        IF EXISTS;
DROP FUNCTION btrim_Boxed    IF EXISTS;
DROP FUNCTION concat2Varchar IF EXISTS;
DROP FUNCTION concat3Varchar IF EXISTS;
DROP FUNCTION concat4Varchar IF EXISTS;

remove classes org.voltdb_testfuncs.UserDefinedTestFunctions;

-- This does not seem to work; I'm not sure whether it should:
--remove classes org.voltdb_testfuncs.UserDefinedTestException;

-- Load the class containing the test UDF's (user-defined functions)
load classes testfuncs.jar;

-- Create the 'add...' test UDF's, which throw all kinds of exceptions, and
-- return various flavors of VoltDB 'null' values, when given certain special
-- input values (generally from -100 to -120):

CREATE FUNCTION add2Tinyint   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Tinyint;
CREATE FUNCTION add2Smallint  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Smallint;
CREATE FUNCTION add2Integer   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Integer;
CREATE FUNCTION add2Bigint    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Bigint;
CREATE FUNCTION add2Float     FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Float;
CREATE FUNCTION add2TinyintBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2TinyintBoxed;
CREATE FUNCTION add2SmallintBoxed FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2SmallintBoxed;
CREATE FUNCTION add2IntegerBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2IntegerBoxed;
CREATE FUNCTION add2BigintBoxed   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2BigintBoxed;
CREATE FUNCTION add2FloatBoxed    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2FloatBoxed;
CREATE FUNCTION add2Decimal   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Decimal;
CREATE FUNCTION add2Varchar   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Varchar;
CREATE FUNCTION add2Varbinary FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Varbinary;
CREATE FUNCTION addYearsToTimestamp FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.addYearsToTimestamp;
-- TODO: uncomment these once UDF's using GEOGRAPHY_POINT and GEOGRAPHY work:
--CREATE FUNCTION add2GeographyPoint  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2GeographyPoint;
--CREATE FUNCTION addGeographyPointToGeography FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.addGeographyPointToGeography;

-- Create simple test UDF's with 0 or 1 arguments (these, and the ones below,
-- unlike the ones above, are 'compatible' with PostgreSQL, and do not go out
-- of their way to throw exceptions or return VoltDB null values, so they could
-- be used by SqlCoverage):

CREATE FUNCTION pi_UDF       FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.pi_UDF;
CREATE FUNCTION pi_UDF_Boxed       FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.pi_UDF_Boxed;
CREATE FUNCTION abs_TINYINT  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_TINYINT;
CREATE FUNCTION abs_SMALLINT FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_SMALLINT;
CREATE FUNCTION abs_INTEGER  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_INTEGER;
CREATE FUNCTION abs_BIGINT   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_BIGINT;
CREATE FUNCTION abs_FLOAT    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_FLOAT;
CREATE FUNCTION abs_TINYINT_Boxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_TINYINT_Boxed;
CREATE FUNCTION abs_SMALLINT_Boxed FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_SMALLINT_Boxed;
CREATE FUNCTION abs_INTEGER_Boxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_INTEGER_Boxed;
CREATE FUNCTION abs_BIGINT_Boxed   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_BIGINT_Boxed;
CREATE FUNCTION abs_FLOAT_Boxed    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_FLOAT_Boxed;
CREATE FUNCTION abs_DECIMAL  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.abs_DECIMAL;
CREATE FUNCTION reverse   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.reverse;
-- TODO: uncomment these once UDF's using GEOGRAPHY work:
--CREATE FUNCTION numRings  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.numRings;
--CREATE FUNCTION numPoints_UDF FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.numPoints;

-- Create simple test UDF's with 2 arguments

CREATE FUNCTION mod_TINYINT  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_TINYINT;
CREATE FUNCTION mod_SMALLINT FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_SMALLINT;
CREATE FUNCTION mod_INTEGER  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_INTEGER;
CREATE FUNCTION mod_BIGINT   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_BIGINT;
CREATE FUNCTION mod_FLOAT    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_FLOAT;
CREATE FUNCTION mod_TINYINT_Boxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_TINYINT_Boxed;
CREATE FUNCTION mod_SMALLINT_Boxed FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_SMALLINT_Boxed;
CREATE FUNCTION mod_INTEGER_Boxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_INTEGER_Boxed;
CREATE FUNCTION mod_BIGINT_Boxed   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_BIGINT_Boxed;
CREATE FUNCTION mod_FLOAT_Boxed    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_FLOAT_Boxed;
CREATE FUNCTION mod_DECIMAL  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.mod_DECIMAL;
CREATE FUNCTION btrim        FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.btrim;
CREATE FUNCTION concat2Varchar FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.concat2Varchar;

-- Create simple test UDF's with 3+ arguments

CREATE FUNCTION concat3Varchar FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.concat3Varchar;
CREATE FUNCTION concat4Varchar FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.concat4Varchar;


-- These just show that the functions have been loaded and created successfully:
show classes;
show functions;