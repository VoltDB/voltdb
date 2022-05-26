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

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;



public class TestPlansIn extends PlannerTestCase {

    public void testIn()
    {
        compile("select * from new_order where no_w_id in ?;");
        compile("select * from new_order where no_w_id in (5,7);");
        compile("select * from new_order where no_w_id in (?);");
        compile("select * from new_order where no_w_id in (?,5,3,?);");
        compile("select * from new_order where no_w_id not in (?,5,3,?);");
        compile("select * from warehouse where w_name not in (?, 'foo');");
        compile("select * from new_order where no_w_id in (no_d_id, no_o_id, ?, 7);");
        compile("select * from new_order where no_w_id in (abs(-1), ?, 17761776);");
        compile("select * from new_order where no_w_id in (abs(17761776), ?, 17761776) and no_d_id in (abs(-1), ?, 17761776);");
        // IN LISTs once interacted badly with joins and indexes, especially with compound indexes,
        // so exercise the planner with the possible combinations of using potentially indexable components
        // in IN LISTs and join clauses. See ENG-9626.
        // Note: the index is on NEW_ORDERS (NO_D_ID, NO_W_ID, NO_O_ID).
        compile("select warehouse.w_id from warehouse, new_order where no_d_id in (5, 7) and no_w_id = w_id;");
        compile("select district.d_id from district, new_order where no_d_id = d_id and no_w_id in (5, 7);");
        compile("select order_line.ol_o_id from order_line, new_order where no_d_id in (5, 7) and no_o_id = ol_o_id;");
        compile("select district.d_id from district, new_order where no_d_id = d_id and no_o_id in (5, 7);");
        // These two are not actually indexable cases, but they're easy enough to test.
        compile("select warehouse.w_id from warehouse, new_order where no_w_id = w_id and no_o_id in (5, 7);");
        compile("select order_line.ol_o_id from order_line, new_order where no_o_id = ol_o_id and no_w_id in (5, 7);");
    }

    public void testNonSupportedIn() {
        // Empty in list cases should give roughly the same error message regardless
        // of other valid where clauses or use of ";"
        failToCompile("select * from new_order where no_w_id in ( )",
                " unexpected ");
        failToCompile("select * from new_order where no_w_id in ( ) and no_o_id > 1",
                " unexpected ");
        failToCompile("select * from new_order where no_w_id in ( );",
                " unexpected ");
        failToCompile("select * from new_order where no_w_id in ( ) and no_o_id > 1;",
                " unexpected ");

        failToCompile("select * from new_order where no_w_id <> (5, 7, 8);",
                "row column count mismatch");
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TPCCProjectBuilder.class.getResource("tpcc-ddl.sql"), "testplansin", false);
    }

}
