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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.AbstractPlanNode;

public class TestGetTablesAndIndexes extends PlannerTestCase {

    @Override
    public void setUp() throws Exception {
        setupSchema(TPCCProjectBuilder.class.getResource("tpcc-ddl.sql"), "testGetTablesAndIndexes", false);
    }

    private void assertTablesAndIndexes(String stmt,
            String expectedPlanNode,
            Collection<String> expectedTables,
            Collection<String> expectedIndexes) {
        List<AbstractPlanNode> fragments = compileToFragments(stmt);

        Map<String, StmtTargetTableScan> actualTables = new HashMap<>();
        Set<String> actualIndexes = new HashSet<>();
        boolean foundExpectedPlanNode = false;
        for (AbstractPlanNode frag : fragments) {
            frag.getTablesAndIndexes(actualTables, actualIndexes);
            String explainPlan = frag.toExplainPlanString();
            if (explainPlan.contains(expectedPlanNode)) {
                foundExpectedPlanNode = true;
            }
        }

        assertTrue(foundExpectedPlanNode);

        for (String expectedTable : expectedTables) {
            assertTrue(actualTables.containsKey(expectedTable));
        }
        assertEquals(expectedTables.size(), actualTables.size());

        if (expectedIndexes != null) {
            for (String expectedIndex : expectedIndexes) {
                assertTrue(actualIndexes.contains(expectedIndex));
            }
            assertEquals(expectedIndexes.size(), actualIndexes.size());
        }
        else {
            assertTrue(actualIndexes.isEmpty());
        }
    }

    public void testSelectStatements() {
        // Make sure that all the leaf nodes report the tables they access
        assertTablesAndIndexes("select * from history", "SEQUENTIAL SCAN",
                Arrays.asList("HISTORY"), null);
        assertTablesAndIndexes("select count(*) from history", "TABLE COUNT",
                Arrays.asList("HISTORY"), null);

        assertTablesAndIndexes("select * from warehouse", "INDEX SCAN",
                Arrays.asList("WAREHOUSE"),
                Arrays.asList("VOLTDB_AUTOGEN_CONSTRAINT_IDX_W_PK_TREE"));
        assertTablesAndIndexes("select COUNT(*) from warehouse where w_id > 100", "INDEX COUNT",
                Arrays.asList("WAREHOUSE"),
                Arrays.asList("VOLTDB_AUTOGEN_CONSTRAINT_IDX_W_PK_TREE"));

        assertTablesAndIndexes(
                "select * "
                + "from orders as o inner join order_line as ol "
                + "on o.o_id = ol.ol_o_id and o_w_id = ol_w_id "
                + "where o_d_id = 32",
                "INDEX SCAN",
                Arrays.asList("ORDERS", "ORDER_LINE"),
                Arrays.asList("VOLTDB_AUTOGEN_CONSTRAINT_IDX_O_PK_HASH"));

        // Test subqueries!
        assertTablesAndIndexes("select * from (select * from customer where c_w_id > 50) as c;",
                "INDEX SCAN",
                Arrays.asList("CUSTOMER"),
                Arrays.asList("IDX_CUSTOMER"));
        assertTablesAndIndexes("select (select max(i_price) from item) as maxprice from customer",
                "INDEX SCAN",
                Arrays.asList("CUSTOMER", "ITEM"),
                Arrays.asList("VOLTDB_AUTOGEN_IDX_CT_CUSTOMER_C_W_ID_C_D_ID_C_LAST_C_FIRST"));
        // Following query tests the TUPLE_SCAN plan node (TupleScanPlanNode)
        assertTablesAndIndexes("select * "
                + "from customer "
                + "where (c_id,c_w_id) > (select ol_o_id, ol_d_id from order_line where ol_w_id = 0)",
                "INDEX SCAN",
                Arrays.asList("CUSTOMER", "ORDER_LINE"),
                Arrays.asList("VOLTDB_AUTOGEN_IDX_CT_CUSTOMER_C_W_ID_C_D_ID_C_LAST_C_FIRST",
                        "IDX_ORDER_LINE_TREE"));
    }

    private void assertUpdatedTable(String stmt, String expectedTable) {
        List<AbstractPlanNode> fragments = compileToFragments(stmt);

        String actualTable = null;
        for (AbstractPlanNode fragment : fragments) {
            String tbl = fragment.getUpdatedTable();
            if (tbl != null) {
                assertNull(actualTable);
                actualTable = tbl;
            }
        }

        assertNotNull(actualTable);
        assertEquals(expectedTable, actualTable);
    }

    public void testDmlStatements() {
        assertUpdatedTable("insert into new_order values (0, 0, 0)", "NEW_ORDER");
        assertUpdatedTable("upsert into new_order values (0, 0, 0)", "NEW_ORDER");
        assertUpdatedTable("delete from new_order", "NEW_ORDER");
        assertUpdatedTable("update new_order set no_w_id = 3 where no_d_id = 10", "NEW_ORDER");
        assertUpdatedTable("truncate table new_order", "NEW_ORDER");
    }

}
