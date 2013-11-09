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

import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;

public class TestSubQueries   extends PlannerTestCase {

    public void testSubQuery() {
        {
            AbstractPlanNode pn = compile("select A, C FROM (SELECT A, C FROM R1) TEMP WHERE TEMP.A > 0");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            AbstractExpression p = ((SeqScanPlanNode) pn).getPredicate();
            assertTrue(p != null);
            assertTrue(p instanceof ComparisonExpression);
            p = p.getLeft();
            assertTrue(p instanceof TupleValueExpression);
            assertEquals("TEMP", ((TupleValueExpression) p).getTableAlias());
            assertTrue(pn.getChildCount() == 1);
            assertTrue(pn.getChild(0) instanceof SeqScanPlanNode);
            NodeSchema ns = pn.getOutputSchema();
            String columns[] = {"A", "C"};
            List<SchemaColumn> scs = ns.getColumns();
            for (int i = 0; i < scs.size(); ++i) {
                SchemaColumn col = scs.get(i);
                assertEquals(columns[i], col.getColumnName());
                assertEquals(4, col.getSize());
                assertEquals(VoltType.INTEGER, col.getType());
                assertTrue(col.getExpression() instanceof TupleValueExpression);
                assertTrue(((TupleValueExpression)col.getExpression()).getColumnIndex() != -1);
            }
        }

        {
            AbstractPlanNode pn = compile("select A1, C1 FROM (SELECT A A1, C C1 FROM R1) TEMP WHERE TEMP.A1 > 0");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            NodeSchema ns = pn.getOutputSchema();
            String columns[] = {"A1", "C1"};
            List<SchemaColumn> scs = ns.getColumns();
            for (int i = 0; i < scs.size(); ++i) {
                SchemaColumn col = scs.get(i);
                assertEquals(columns[i], col.getColumnName());
                assertEquals(4, col.getSize());
                assertEquals(VoltType.INTEGER, col.getType());
                assertTrue(col.getExpression() instanceof TupleValueExpression);
                assertTrue(((TupleValueExpression)col.getExpression()).getColumnIndex() != -1);
            }
        }

        {
            AbstractPlanNode pn = compile("select A2 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) TEMP1 WHERE TEMP1.A1 > 0) TEMP2 WHERE TEMP2.A2 = 3");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            NodeSchema ns = pn.getOutputSchema();
            String columns[] = {"A2"};
            List<SchemaColumn> scs = ns.getColumns();
            for (int i = 0; i < scs.size(); ++i) {
                SchemaColumn col = scs.get(i);
                assertEquals(columns[i], col.getColumnName());
                assertEquals(4, col.getSize());
                assertEquals(VoltType.INTEGER, col.getType());
                assertTrue(col.getExpression() instanceof TupleValueExpression);
                assertTrue(((TupleValueExpression)col.getExpression()).getColumnIndex() != -1);
            }
        }

    }

//    failToCompile("select * from new_order where no_w_id in (select w_id from warehouse);",
//            "VoltDB does not support subqueries");

// params
    // update
    // delete

//    adHocQuery = "  UPDATE STAFF \n" +
//            "          SET GRADE=10*STAFF.GRADE \n" +
//            "          WHERE STAFF.EMPNUM NOT IN \n" +
//            "                (SELECT WORKS.EMPNUM \n" +
//            "                      FROM WORKS \n" +
//            "                      WHERE STAFF.EMPNUM = WORKS.EMPNUM);";
//    adHocQuery = "     SELECT 'ZZ', EMPNUM, EMPNAME, -99 \n" +
//            "           FROM STAFF \n" +
//            "           WHERE NOT EXISTS (SELECT * FROM WORKS \n" +
//            "                WHERE WORKS.EMPNUM = STAFF.EMPNUM) \n" +
//            "                ORDER BY EMPNUM;";
//    adHocQuery = "   SELECT STAFF.EMPNAME \n" +
//            "          FROM STAFF \n" +
//            "          WHERE STAFF.EMPNUM IN \n" +
//            "                  (SELECT WORKS.EMPNUM \n" +
//            "                        FROM WORKS \n" +
//            "                        WHERE WORKS.PNUM IN \n" +
//            "                              (SELECT PROJ.PNUM \n" +
//            "                                    FROM PROJ \n" +
//            "                                    WHERE PROJ.CITY='Tampa')); \n" +
//            "";


    // need the case with two  sub-queries on the same lavel (table names will be identical)

    //failToCompile("select * from new_order where no_w_id in (select w_id from warehouse);",
    //        "VoltDB does not support subqueries");

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testsub-queries-ddl.sql"), "testsubqueries", false);
    }

}
