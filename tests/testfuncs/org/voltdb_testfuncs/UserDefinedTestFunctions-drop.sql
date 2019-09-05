-- Drops all the test UDF's (user-defined functions), and removes the class
-- containing them, in case they were loaded and created previously

file -inlinebatch END_OF_BATCH_DROP_FUNC

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

END_OF_BATCH_DROP_FUNC

-- Finally, remove the class containing all the test UDF's (user-defined functions)
remove classes org.voltdb_testfuncs.UserDefinedTestFunctions;
