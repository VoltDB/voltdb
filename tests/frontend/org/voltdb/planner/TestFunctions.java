/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.planner;


public class TestFunctions extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestFunctions.class.getResource("testplans-functions-ddl.sql"), "testfunctions",
                                                    planForSinglePartitionFalse);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Before fixing ENG-913, the SQL would compile, even though 'user'
     * was not a column name and is a reserved word.
     */
    public void testENG913_userfunction() {
        failToCompile("update ENG913 set name = 'tim' where user = ?;", "'user'", "not supported");
    }

    public void testBitwise() {
        String errorMsg = "incompatible data type in conversion";
        failToCompile("select bitand(tinyint_type, 3) from bit;", errorMsg);
        failToCompile("select bitand(INTEGER_TYPE, 3) from bit;", errorMsg);
        failToCompile("select bitand(FLOAT_TYPE, 3) from bit;", errorMsg);
        failToCompile("select bitand(VARCHAR_TYPE, 3) from bit;", errorMsg);


        failToCompile("select bitor(tinyint_type, 3) from bit;", errorMsg);
        failToCompile("select bitor(INTEGER_TYPE, 3) from bit;", errorMsg);
        failToCompile("select bitor(FLOAT_TYPE, 3) from bit;", errorMsg);
        failToCompile("select bitor(VARCHAR_TYPE, 3) from bit;", errorMsg);


        failToCompile("select bitxor(tinyint_type, 3) from bit;", errorMsg);
        failToCompile("select bitxor(INTEGER_TYPE, 3) from bit;", errorMsg);
        failToCompile("select bitxor(FLOAT_TYPE, 3) from bit;", errorMsg);
        failToCompile("select bitxor(VARCHAR_TYPE, 3) from bit;", errorMsg);


        failToCompile("select bitnot(tinyint_type) from bit;", errorMsg);
        failToCompile("select bitnot(INTEGER_TYPE) from bit;", errorMsg);
        failToCompile("select bitnot(FLOAT_TYPE) from bit;", errorMsg);
        failToCompile("select bitnot(VARCHAR_TYPE) from bit;", errorMsg);

        // bit shift
        failToCompile("select BIT_SHIFT_LEFT(FLOAT_TYPE, 3), BIT_SHIFT_RIGHT(FLOAT_TYPE, 3) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_LEFT(VARCHAR_TYPE, 3), BIT_SHIFT_RIGHT(VARCHAR_TYPE, 3) from bit", errorMsg);

        failToCompile("select BIT_SHIFT_LEFT(tinyint_type, 3)  from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(tinyint_type, 3) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_LEFT(3.356, tinyint_type)from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(3.356, tinyint_type)from bit", errorMsg);

        failToCompile("select BIT_SHIFT_LEFT(INTEGER_TYPE, 3)  from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(INTEGER_TYPE, 3) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_LEFT(3.356, INTEGER_TYPE) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(3.356, INTEGER_TYPE) from bit", errorMsg);

        failToCompile("select BIT_SHIFT_LEFT(BIGINT_TYPE, 0.5) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(BIGINT_TYPE, 0.5) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_LEFT(BIGINT_TYPE, FLOAT_TYPE) from bit", errorMsg);
        failToCompile("select BIT_SHIFT_LEFT(BIGINT_TYPE, FLOAT_TYPE) from bit", errorMsg);

        failToCompile("select hex(FLOAT_TYPE) from bit;", errorMsg);
        failToCompile("select hex(INTEGER_TYPE) from bit;", errorMsg);

        failToCompile("select bin(FLOAT_TYPE) from bit;", errorMsg);
        failToCompile("select bin(INTEGER_TYPE) from bit;", errorMsg);

        // compile on constants
        compile("select BIT_SHIFT_LEFT(3, tinyint_type), BIT_SHIFT_RIGHT(3, tinyint_type) from bit");
        compile("select BIT_SHIFT_LEFT(3, INTEGER_TYPE), BIT_SHIFT_RIGHT(3, INTEGER_TYPE) from bit");

        // out of range exception
        errorMsg = "numeric value out of range";
        failToCompile("select bitand(bigint_type, 9223372036854775809) from bit;", errorMsg);
        failToCompile("select bitand(bigint_type, -9223372036854775809) from bit;", errorMsg);

        failToCompile("select BIT_SHIFT_LEFT(9223372036854775809, tinyint_type)from bit", errorMsg);
        failToCompile("select BIT_SHIFT_LEFT(-9223372036854775809, tinyint_type)from bit", errorMsg);

        failToCompile("select BIT_SHIFT_RIGHT(9223372036854775809, tinyint_type)from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(-9223372036854775809, tinyint_type)from bit", errorMsg);

        failToCompile("select hex(9223372036854775809) from bit;", errorMsg);

        failToCompile("select bin(9223372036854775809) from bit;", errorMsg);

        // invalid syntax
        errorMsg = "object not found";
        failToCompile("select BIT_SHIFT_LEFT(BIGINT_TYPE)from bit", errorMsg);
        failToCompile("select BIT_SHIFT_RIGHT(BIGINT_TYPE)from bit", errorMsg);

    }

    public void testUnsupportedFuncs() {

        failToCompile("SELECT  ACOS(FLOAT_TYPE) FROM NUMERICFUN",
                "Function 'acos' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  ASIN(FLOAT_TYPE) FROM NUMERICFUN",
                "Function 'asin' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  ATAN(FLOAT_TYPE) FROM NUMERICFUN",
                "Function 'atan' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  SIGN(FLOAT_TYPE) FROM NUMERICFUN",
                "Function 'sign' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  ROUNDMAGIC(FLOAT_TYPE) FROM NUMERICFUN",
                "Function 'roundmagic' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  SOUNDEX(VARCHAR_TYPE) FROM NUMERICFUN",
                "Function 'soundex' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  ASCII(VARCHAR_TYPE) FROM NUMERICFUN",
                "Function 'ascii' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  RAWTOHEX(VARBINARY_TYPE) FROM NUMERICFUN",
                "Function 'rawtohex' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  HEXTORAW(VARCHAR_TYPE) FROM NUMERICFUN",
                "Function 'hextoraw' is not supported in VoltDB: Custom Function");
        failToCompile("SELECT  ATAN2(FLOAT_TYPE,FLOAT_TYPE) FROM NUMERICFUN",
                "object not found: ATAN2");

    }

    public void testLikeNoopt() {
        compile("select case when varchar_type like 'M%' then 1 end as m_state from bit;");
        compile("select case when varchar_type like '_%' then 1 end as m_state from bit;");
    }

    public void testSerializeFunctionTimestampArgumentInPlan() {
        // This test comes from ENG-10749 and ENG-10750
        compile("SELECT SINCE_EPOCH(MICROSECOND, '1812-10-28 07:30:43') FROM ENG10749 WHERE '2080-05-24 11:18:38' <> TIME OR '4253-02-25 00:20:34' IS NULL;");
    }

    public void testLongConstants() {
        failToCompile("SELECT  LN(MOD(17004989843871566923468, MOD(10, -73))) FROM ENG12089",
                      "Numeric conversion error to type BIGINT for input string: \"17004989843871566923468\"");
    }
}
