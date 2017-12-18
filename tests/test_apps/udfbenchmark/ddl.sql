LOAD CLASSES udfbenchmark.jar;

file -inlinebatch END_OF_BATCH

CREATE TABLE R1 (
    ID      INTEGER NOT NULL PRIMARY KEY,
    TINY    TINYINT,
    SMALL   SMALLINT,
    INT     INTEGER,
    BIG     BIGINT,
    NUM     FLOAT,
    DEC     DECIMAL,
    VCHAR_INLINE_MAX VARCHAR(63 BYTES),
    VCHAR            VARCHAR(64 BYTES),
    TIME    TIMESTAMP,
    VARBIN1 VARBINARY(100),
    VARBIN2 VARBINARY(100),
    POINT1  GEOGRAPHY_POINT,
    POINT2  GEOGRAPHY_POINT,
    POLYGON GEOGRAPHY
);

CREATE TABLE P1 (
    ID      INTEGER NOT NULL PRIMARY KEY,
    TINY    TINYINT,
    SMALL   SMALLINT,
    INT     INTEGER,
    BIG     BIGINT,
    NUM     FLOAT,
    DEC     DECIMAL,
    VCHAR_INLINE_MAX VARCHAR(63 BYTES),
    VCHAR            VARCHAR(64 BYTES),
    TIME    TIMESTAMP,
    VARBIN1 VARBINARY(100),
    VARBIN2 VARBINARY(100),
    POINT1  GEOGRAPHY_POINT,
    POINT2  GEOGRAPHY_POINT,
    POLYGON GEOGRAPHY
);
PARTITION TABLE P1 ON COLUMN ID;

-- Create the 'add...' test UDF's, which throw all kinds of exceptions, and
-- return various flavors of VoltDB 'null' values, when given certain special
-- input values (generally from -100 to -120):

CREATE FUNCTION add2Tinyint                  FROM METHOD udfbenchmark.UDFLib.add2Tinyint;
CREATE FUNCTION add2Smallint                 FROM METHOD udfbenchmark.UDFLib.add2Smallint;
CREATE FUNCTION add2Integer                  FROM METHOD udfbenchmark.UDFLib.add2Integer;
CREATE FUNCTION add2Bigint                   FROM METHOD udfbenchmark.UDFLib.add2Bigint;
CREATE FUNCTION add2Float                    FROM METHOD udfbenchmark.UDFLib.add2Float;
CREATE FUNCTION add2TinyintBoxed             FROM METHOD udfbenchmark.UDFLib.add2TinyintBoxed;
CREATE FUNCTION add2SmallintBoxed            FROM METHOD udfbenchmark.UDFLib.add2SmallintBoxed;
CREATE FUNCTION add2IntegerBoxed             FROM METHOD udfbenchmark.UDFLib.add2IntegerBoxed;
CREATE FUNCTION add2BigintBoxed              FROM METHOD udfbenchmark.UDFLib.add2BigintBoxed;
CREATE FUNCTION add2FloatBoxed               FROM METHOD udfbenchmark.UDFLib.add2FloatBoxed;
CREATE FUNCTION add2Decimal                  FROM METHOD udfbenchmark.UDFLib.add2Decimal;
CREATE FUNCTION add2Varchar                  FROM METHOD udfbenchmark.UDFLib.add2Varchar;
CREATE FUNCTION add2Varbinary                FROM METHOD udfbenchmark.UDFLib.add2Varbinary;
CREATE FUNCTION add2VarbinaryBoxed           FROM METHOD udfbenchmark.UDFLib.add2VarbinaryBoxed;
CREATE FUNCTION addYearsToTimestamp          FROM METHOD udfbenchmark.UDFLib.addYearsToTimestamp;
CREATE FUNCTION add2GeographyPoint           FROM METHOD udfbenchmark.UDFLib.add2GeographyPoint;
CREATE FUNCTION addGeographyPointToGeography FROM METHOD udfbenchmark.UDFLib.addGeographyPointToGeography;
CREATE FUNCTION getByteArrayOfSize           FROM METHOD udfbenchmark.UDFLib.getByteArrayOfSize;

-- Create the 'add...WithoutNullCheck' test UDF's, which are just like (some of)
-- the ones above (they throw all kinds of exceptions, and return various flavors
-- of VoltDB 'null' values, when given certain special input values, generally
-- from -100 to -120); but these functions do no null checking:

CREATE FUNCTION add2TinyintWithoutNullCheck       FROM METHOD udfbenchmark.UDFLib.add2TinyintWithoutNullCheck;
CREATE FUNCTION add2SmallintWithoutNullCheck      FROM METHOD udfbenchmark.UDFLib.add2SmallintWithoutNullCheck;
CREATE FUNCTION add2IntegerWithoutNullCheck       FROM METHOD udfbenchmark.UDFLib.add2IntegerWithoutNullCheck;
CREATE FUNCTION add2BigintWithoutNullCheck        FROM METHOD udfbenchmark.UDFLib.add2BigintWithoutNullCheck;
CREATE FUNCTION add2FloatWithoutNullCheck         FROM METHOD udfbenchmark.UDFLib.add2FloatWithoutNullCheck;
CREATE FUNCTION add2TinyintBoxedWithoutNullCheck  FROM METHOD udfbenchmark.UDFLib.add2TinyintBoxedWithoutNullCheck;
CREATE FUNCTION add2SmallintBoxedWithoutNullCheck FROM METHOD udfbenchmark.UDFLib.add2SmallintBoxedWithoutNullCheck;
CREATE FUNCTION add2IntegerBoxedWithoutNullCheck  FROM METHOD udfbenchmark.UDFLib.add2IntegerBoxedWithoutNullCheck;
CREATE FUNCTION add2BigintBoxedWithoutNullCheck   FROM METHOD udfbenchmark.UDFLib.add2BigintBoxedWithoutNullCheck;
CREATE FUNCTION add2FloatBoxedWithoutNullCheck    FROM METHOD udfbenchmark.UDFLib.add2FloatBoxedWithoutNullCheck;


-- Create simple test UDF's with 0 or 1 arguments (these, and the ones below,
-- unlike the ones above, are 'compatible' with PostgreSQL, and do not go out
-- of their way to throw exceptions or return VoltDB null values, so they could
-- be used by SqlCoverage):

CREATE FUNCTION piUdf            FROM METHOD udfbenchmark.UDFLib.piUdf;
CREATE FUNCTION piUdfBoxed       FROM METHOD udfbenchmark.UDFLib.piUdfBoxed;
CREATE FUNCTION absTinyint       FROM METHOD udfbenchmark.UDFLib.absTinyint;
CREATE FUNCTION absSmallint      FROM METHOD udfbenchmark.UDFLib.absSmallint;
CREATE FUNCTION absInteger       FROM METHOD udfbenchmark.UDFLib.absInteger;
CREATE FUNCTION absBigint        FROM METHOD udfbenchmark.UDFLib.absBigint;
CREATE FUNCTION absFloat         FROM METHOD udfbenchmark.UDFLib.absFloat;
CREATE FUNCTION absTinyintBoxed  FROM METHOD udfbenchmark.UDFLib.absTinyintBoxed;
CREATE FUNCTION absSmallintBoxed FROM METHOD udfbenchmark.UDFLib.absSmallintBoxed;
CREATE FUNCTION absIntegerBoxed  FROM METHOD udfbenchmark.UDFLib.absIntegerBoxed;
CREATE FUNCTION absBigintBoxed   FROM METHOD udfbenchmark.UDFLib.absBigintBoxed;
CREATE FUNCTION absFloatBoxed    FROM METHOD udfbenchmark.UDFLib.absFloatBoxed;
CREATE FUNCTION absDecimal       FROM METHOD udfbenchmark.UDFLib.absDecimal;
CREATE FUNCTION reverse          FROM METHOD udfbenchmark.UDFLib.reverse;
CREATE FUNCTION numRings         FROM METHOD udfbenchmark.UDFLib.numRings;
CREATE FUNCTION numPointsUdf     FROM METHOD udfbenchmark.UDFLib.numPointsUdf;

-- Create simple test UDF's with 2 arguments

CREATE FUNCTION modTinyint       FROM METHOD udfbenchmark.UDFLib.modTinyint;
CREATE FUNCTION modSmallint      FROM METHOD udfbenchmark.UDFLib.modSmallint;
CREATE FUNCTION modInteger       FROM METHOD udfbenchmark.UDFLib.modInteger;
CREATE FUNCTION modBigint        FROM METHOD udfbenchmark.UDFLib.modBigint;
CREATE FUNCTION modFloat         FROM METHOD udfbenchmark.UDFLib.modFloat;
CREATE FUNCTION modTinyintBoxed  FROM METHOD udfbenchmark.UDFLib.modTinyintBoxed;
CREATE FUNCTION modSmallintBoxed FROM METHOD udfbenchmark.UDFLib.modSmallintBoxed;
CREATE FUNCTION modIntegerBoxed  FROM METHOD udfbenchmark.UDFLib.modIntegerBoxed;
CREATE FUNCTION modBigintBoxed   FROM METHOD udfbenchmark.UDFLib.modBigintBoxed;
CREATE FUNCTION modFloatBoxed    FROM METHOD udfbenchmark.UDFLib.modFloatBoxed;
CREATE FUNCTION modDecimal       FROM METHOD udfbenchmark.UDFLib.modDecimal;
CREATE FUNCTION btrim            FROM METHOD udfbenchmark.UDFLib.btrim;
CREATE FUNCTION btrimBoxed       FROM METHOD udfbenchmark.UDFLib.btrimBoxed;
CREATE FUNCTION concat2Varchar   FROM METHOD udfbenchmark.UDFLib.concat2Varchar;

-- Create simple test UDF's with 3+ arguments

CREATE FUNCTION concat3Varchar FROM METHOD udfbenchmark.UDFLib.concat3Varchar;
CREATE FUNCTION concat4Varchar FROM METHOD udfbenchmark.UDFLib.concat4Varchar;

CREATE PROCEDURE R1Tester AS SELECT
    piUdfBoxed(),
    absTinyint(TINY),
    modSmallint(SMALL, 10),
    add2BigintBoxed(INT, BIG),
    add2FloatBoxed(DEC, NUM),
    addYearsToTimestamp(TIME, modIntegerBoxed(INT, 100)),
    add2Varchar(VCHAR_INLINE_MAX, VCHAR),
    add2Varbinary(VARBIN1, VARBIN2),
    add2GeographyPoint(POINT1, POINT2),
    addGeographyPointToGeography(POLYGON, POINT1)
FROM R1 WHERE ID = ?;

CREATE PROCEDURE P1Tester PARTITION ON TABLE P1 COLUMN ID AS SELECT
    piUdfBoxed(),
    absTinyint(TINY),
    modSmallint(SMALL, 10),
    add2BigintBoxed(INT, BIG),
    add2FloatBoxed(DEC, NUM),
    addYearsToTimestamp(TIME, modIntegerBoxed(INT, 100)),
    add2Varchar(VCHAR_INLINE_MAX, VCHAR),
    add2Varbinary(VARBIN1, VARBIN2),
    add2GeographyPoint(POINT1, POINT2),
    addGeographyPointToGeography(POLYGON, POINT1)
FROM P1 WHERE ID = ?;

END_OF_BATCH
