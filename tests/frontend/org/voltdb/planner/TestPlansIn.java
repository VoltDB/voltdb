/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;


public class TestPlansIn extends PlannerTestCase {

    public void testPresentlyUnsupportedIn() {

        failToCompile("select * from new_order where no_w_id in (5,7);",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from new_order where no_w_id in (?);",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from new_order where no_w_id in (?,5,3,?);",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from new_order where no_w_id not in (?,5,3,?);",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from warehouse where w_name not in (?, 'foo');",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from new_order where no_w_id in (no_d_id, no_o_id, ?, 7);",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from new_order where no_w_id in (abs(-1), ?, 17761776);",
                "VoltDB does not support SQL IN expressions.");
        failToCompile("select * from new_order where no_w_id in (abs(17761776), ?, 17761776) and no_d_id in (abs(-1), ?, 17761776);",
                "VoltDB does not support SQL IN expressions.");
    }

    public void testNonSupportedIn() {
        failToCompile("select * from new_order where no_w_id in (select w_id from warehouse);",
                "VoltDB does not support subqueries");
        failToCompile("select * from new_order where no_w_id <> (5, 7, 8);",
                "row column count mismatch");
        failToCompile("select * from new_order where exists (select w_id from warehouse);",
                "VoltDB does not support subqueries");
        failToCompile("select * from new_order where not exists (select w_id from warehouse);",
                "VoltDB does not support subqueries");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TPCCProjectBuilder.class.getResource("tpcc-ddl.sql"), "testplansin", false);
    }

}
