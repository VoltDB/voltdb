-- Loads and creates the test UDAF's (user-defined aggregate functions) defined in
-- in voltdb_testfuncs folder, using various data types, and with
-- various numbers of arguments.

-- First, drop all the test UDAF's (user-defined aggregate functions), and remove the class
-- containing them, in case they were loaded and created previously

DROP AGGREGATE FUNCTION uavg IF EXISTS;
DROP AGGREGATE FUNCTION ucount IF EXISTS;
DROP AGGREGATE FUNCTION umax IF EXISTS;
DROP AGGREGATE FUNCTION umedian IF EXISTS;
DROP AGGREGATE FUNCTION umedianEndOverload IF EXISTS;
DROP AGGREGATE FUNCTION umedianEndTwo IF EXISTS;
DROP AGGREGATE FUNCTION umin IF EXISTS;
DROP AGGREGATE FUNCTION uminStartOverload IF EXISTS;
DROP AGGREGATE FUNCTION uminStartTwo IF EXISTS;
DROP AGGREGATE FUNCTION umode IF EXISTS;
DROP AGGREGATE FUNCTION umodeAssembleTwo IF EXISTS;
DROP AGGREGATE FUNCTION uprimesum IF EXISTS;
DROP AGGREGATE FUNCTION uprimesumCombineTwo IF EXISTS;
DROP AGGREGATE FUNCTION usum IF EXISTS;

DROP FUNCTION add2Integer IF EXISTS;
DROP FUNCTION add2Float   IF EXISTS;

CREATE AGGREGATE FUNCTION uavg FROM CLASS org.voltdb_testfuncs.Uavg;
CREATE AGGREGATE FUNCTION ucount FROM CLASS org.voltdb_testfuncs.Ucount;
CREATE AGGREGATE FUNCTION umax FROM CLASS org.voltdb_testfuncs.Umax;
CREATE AGGREGATE FUNCTION umedian FROM CLASS org.voltdb_testfuncs.Umedian;
CREATE AGGREGATE FUNCTION umedianEndTwo FROM CLASS org.voltdb_testfuncs.UmedianEndTwo;
CREATE AGGREGATE FUNCTION umedianEndOverload FROM CLASS org.voltdb_testfuncs.UmedianEndOverload;
CREATE AGGREGATE FUNCTION umin FROM CLASS org.voltdb_testfuncs.Umin;
CREATE AGGREGATE FUNCTION uminStartTwo FROM CLASS org.voltdb_testfuncs.UminStartTwo;
CREATE AGGREGATE FUNCTION uminStartOverload FROM CLASS org.voltdb_testfuncs.UminStartOverload;
CREATE AGGREGATE FUNCTION umode FROM CLASS org.voltdb_testfuncs.Umode;
CREATE AGGREGATE FUNCTION umodeAssembleTwo FROM CLASS org.voltdb_testfuncs.UmodeAssembleTwo;
CREATE AGGREGATE FUNCTION uprimesum FROM CLASS org.voltdb_testfuncs.Uprimesum;
CREATE AGGREGATE FUNCTION uprimesumCombineTwo FROM CLASS org.voltdb_testfuncs.UprimesumCombineTwo;
CREATE AGGREGATE FUNCTION usum FROM CLASS org.voltdb_testfuncs.Usum;

CREATE FUNCTION add2Integer FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Integer;
CREATE FUNCTION add2Float     FROM METHOD org.voltdb_testfuncs.UserDefinedTestFunctions.add2Float;