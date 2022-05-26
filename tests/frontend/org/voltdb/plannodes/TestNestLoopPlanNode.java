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
package org.voltdb.plannodes;

import junit.framework.TestCase;

public class TestNestLoopPlanNode extends TestCase
{
    static final String TABLE1 = "table1";
    static final String[] T1COLS = { "t1col0", "t1col1", "t1col2", "t1col3",
                                     "t1col4" };

    static final String TABLE2 = "table2";
    static final String[] T2COLS = { "t2col0", "t2col1", "t2col2", "t2col3" };

    public void testStuff()
    {
        NestLoopPlanNode dut = new NestLoopPlanNode();

        MockPlanNode outer_child = new MockPlanNode(TABLE1, T1COLS);
        MockPlanNode inner_child = new MockPlanNode(TABLE2, T2COLS);

        dut.addAndLinkChild(inner_child);
        dut.addAndLinkChild(outer_child);

        dut.generateOutputSchema(null);
        dut.resolveColumnIndexes();
        dut.resolveSortDirection();
        System.out.println(dut.getOutputSchema().toString());
    }
}
