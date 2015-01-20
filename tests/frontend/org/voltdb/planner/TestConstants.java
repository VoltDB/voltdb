/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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


public class TestConstants extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestFunctions.class.getResource("testplans-const-ddl.sql"),
                    "testconstants", planForSinglePartitionFalse);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Before fixing ENG-257, the SQL wouldn't  compile.
     */
    public void testSelectConstTable() {
        compile("select INT1, 5, 'test' from T1 where T1_PK=1");
    }

    /**
     * Testing select constant with aggregate function.
     */
    public void testSelectConstAggrTable() {
        compile("select max(INT1), 5, 'test' from T1");
    }

    /**
     * Testing select constant from view.
     */
    public void testSelectConstView() {
        compile("select V1, 5, 'test' from V_T1");
    }

    /**
     * Testing select constant from join.
     */
    public void testSelectConstJoin() {
        compile("select a.CHAR1, 5, 'test' from T2 a, V_T1 b where a.T2_PK = b.V1 group by a.CHAR1 order by a.CHAR1");
    }

}
