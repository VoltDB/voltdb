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
DROP FUNCTION getByteArrayOfSize IF EXISTS;

DROP FUNCTION add2TinyintWithoutNullCheck  IF EXISTS;
DROP FUNCTION add2SmallintWithoutNullCheck IF EXISTS;
DROP FUNCTION add2IntegerWithoutNullCheck  IF EXISTS;
DROP FUNCTION add2BigintWithoutNullCheck   IF EXISTS;
DROP FUNCTION add2FloatWithoutNullCheck    IF EXISTS;
DROP FUNCTION add2TinyintBoxedWithoutNullCheck  IF EXISTS;
DROP FUNCTION add2SmallintBoxedWithoutNullCheck IF EXISTS;
DROP FUNCTION add2IntegerBoxedWithoutNullCheck  IF EXISTS;
DROP FUNCTION add2BigintBoxedWithoutNullCheck   IF EXISTS;
DROP FUNCTION add2FloatBoxedWithoutNullCheck    IF EXISTS;

DROP FUNCTION piUdf        IF EXISTS;
DROP FUNCTION piUdfBoxed   IF EXISTS;
DROP FUNCTION absTinyint   IF EXISTS;
DROP FUNCTION absSmallint  IF EXISTS;
DROP FUNCTION absInteger   IF EXISTS;
DROP FUNCTION absBigint    IF EXISTS;
DROP FUNCTION absFloat     IF EXISTS;
DROP FUNCTION absTinyintBoxed  IF EXISTS;
DROP FUNCTION absSmallintBoxed IF EXISTS;
DROP FUNCTION absIntegerBoxed  IF EXISTS;
DROP FUNCTION absBigintBoxed   IF EXISTS;
DROP FUNCTION absFloatBoxed    IF EXISTS;
DROP FUNCTION absDecimal   IF EXISTS;
DROP FUNCTION reverse      IF EXISTS;
DROP FUNCTION numRings     IF EXISTS;
DROP FUNCTION numPointsUdf IF EXISTS;

DROP FUNCTION modTinyint  IF EXISTS;
DROP FUNCTION modSmallint IF EXISTS;
DROP FUNCTION modInteger  IF EXISTS;
DROP FUNCTION modBigint   IF EXISTS;
DROP FUNCTION modFloat    IF EXISTS;
DROP FUNCTION modTinyintBoxed  IF EXISTS;
DROP FUNCTION modSmallintBoxed IF EXISTS;
DROP FUNCTION modIntegerBoxed  IF EXISTS;
DROP FUNCTION modBigintBoxed   IF EXISTS;
DROP FUNCTION modFloatBoxed    IF EXISTS;
DROP FUNCTION modDecimal  IF EXISTS;
DROP FUNCTION btrim       IF EXISTS;
DROP FUNCTION btrimBoxed     IF EXISTS;
DROP FUNCTION concat2Varchar IF EXISTS;
DROP FUNCTION concat3Varchar IF EXISTS;
DROP FUNCTION concat4Varchar IF EXISTS;

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
CREATE FUNCTION add2VarbinaryBoxed   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2VarbinaryBoxed;
CREATE FUNCTION addYearsToTimestamp  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.addYearsToTimestamp;
CREATE FUNCTION add2GeographyPoint   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2GeographyPoint;
CREATE FUNCTION addGeographyPointToGeography FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.addGeographyPointToGeography;
CREATE FUNCTION getByteArrayOfSize   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.getByteArrayOfSize;

-- Create the 'add...WithoutNullCheck' test UDF's, which are just like (some of)
-- the ones above (they throw all kinds of exceptions, and return various flavors
-- of VoltDB 'null' values, when given certain special input values, generally
-- from -100 to -120); but these functions do no null checking:
CREATE FUNCTION add2TinyintWithoutNullCheck  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2TinyintWithoutNullCheck;
CREATE FUNCTION add2SmallintWithoutNullCheck FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2SmallintWithoutNullCheck;
CREATE FUNCTION add2IntegerWithoutNullCheck  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2IntegerWithoutNullCheck;
CREATE FUNCTION add2BigintWithoutNullCheck   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2BigintWithoutNullCheck;
CREATE FUNCTION add2FloatWithoutNullCheck    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2FloatWithoutNullCheck;
CREATE FUNCTION add2TinyintBoxedWithoutNullCheck  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2TinyintBoxedWithoutNullCheck;
CREATE FUNCTION add2SmallintBoxedWithoutNullCheck FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2SmallintBoxedWithoutNullCheck;
CREATE FUNCTION add2IntegerBoxedWithoutNullCheck  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2IntegerBoxedWithoutNullCheck;
CREATE FUNCTION add2BigintBoxedWithoutNullCheck   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2BigintBoxedWithoutNullCheck;
CREATE FUNCTION add2FloatBoxedWithoutNullCheck    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2FloatBoxedWithoutNullCheck;


-- Create simple test UDF's with 0 or 1 arguments (these, and the ones below,
-- unlike the ones above, are 'compatible' with PostgreSQL, and do not go out
-- of their way to throw exceptions or return VoltDB null values, so they could
-- be used by SqlCoverage):

CREATE FUNCTION piUdf       FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.piUdf;
CREATE FUNCTION piUdfBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.piUdfBoxed;
CREATE FUNCTION absTinyint  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absTinyint;
CREATE FUNCTION absSmallint FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absSmallint;
CREATE FUNCTION absInteger  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absInteger;
CREATE FUNCTION absBigint   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absBigint;
CREATE FUNCTION absFloat    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absFloat;
CREATE FUNCTION absTinyintBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absTinyintBoxed;
CREATE FUNCTION absSmallintBoxed FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absSmallintBoxed;
CREATE FUNCTION absIntegerBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absIntegerBoxed;
CREATE FUNCTION absBigintBoxed   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absBigintBoxed;
CREATE FUNCTION absFloatBoxed    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absFloatBoxed;
CREATE FUNCTION absDecimal   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.absDecimal;
CREATE FUNCTION reverse      FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.reverse;
CREATE FUNCTION numRings     FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.numRings;
CREATE FUNCTION numPointsUdf FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.numPointsUdf;

-- Create simple test UDF's with 2 arguments

CREATE FUNCTION modTinyint  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modTinyint;
CREATE FUNCTION modSmallint FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modSmallint;
CREATE FUNCTION modInteger  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modInteger;
CREATE FUNCTION modBigint   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modBigint;
CREATE FUNCTION modFloat    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modFloat;
CREATE FUNCTION modTinyintBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modTinyintBoxed;
CREATE FUNCTION modSmallintBoxed FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modSmallintBoxed;
CREATE FUNCTION modIntegerBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modIntegerBoxed;
CREATE FUNCTION modBigintBoxed   FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modBigintBoxed;
CREATE FUNCTION modFloatBoxed    FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modFloatBoxed;
CREATE FUNCTION modDecimal  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.modDecimal;
CREATE FUNCTION btrim       FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.btrim;
CREATE FUNCTION btrimBoxed  FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.btrimBoxed;
CREATE FUNCTION concat2Varchar FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.concat2Varchar;

-- Create simple test UDF's with 3+ arguments

CREATE FUNCTION concat3Varchar FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.concat3Varchar;
CREATE FUNCTION concat4Varchar FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.concat4Varchar;
