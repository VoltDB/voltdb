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
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
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
            verifyOutputSchema(pn, "A", "C");
        }

        {
            AbstractPlanNode pn = compile("select A1, C1 FROM (SELECT A A1, C C1 FROM R1) TEMP WHERE TEMP.A1 > 0");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            verifyOutputSchema(pn, "A1", "C1");
        }

        {
            AbstractPlanNode pn = compile("select A2 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) TEMP1 WHERE TEMP1.A1 > 0) TEMP2 WHERE TEMP2.A2 = 3");
            pn = pn.getChild(0);
            assertTrue(pn instanceof SeqScanPlanNode);
            verifyOutputSchema(pn, "A2");
        }

        {
            AbstractPlanNode pn = compile("select A, C FROM (SELECT A FROM R1) TEMP1, (SELECT C FROM R2) TEMP2 WHERE A = C");
            pn = pn.getChild(0).getChild(0);
            assertTrue(pn instanceof NestLoopPlanNode);
            verifyOutputSchema(pn, "A", "C");
        }
    }

    public void testDistributedSubQuery() {
        {
            // Partitioned sub-query
            List<AbstractPlanNode> lpn = compileToFragments("select A, C FROM (SELECT A, C FROM P1) TEMP ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(0).getChild(0);
            assertTrue(n instanceof SeqScanPlanNode);
            assertEquals("SYSTEM_SUBQUERY", ((SeqScanPlanNode) n).getTargetTableName());
            n = lpn.get(1).getChild(0);
            assertTrue(n instanceof IndexScanPlanNode);
        }

        {
            // Two sub-queries. One is partitioned and the other one is replicated
            List<AbstractPlanNode> lpn = compileToFragments("select A, C FROM (SELECT A FROM R1) TEMP1, (SELECT C FROM P2) TEMP2 WHERE TEMP1.A = TEMP2.C ");
            assertTrue(lpn.size() == 2);
            AbstractPlanNode n = lpn.get(0).getChild(0).getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            AbstractPlanNode c = n.getChild(0);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP1", ((SeqScanPlanNode) c).getTargetTableAlias());
            c = n.getChild(1);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP2", ((SeqScanPlanNode) c).getTargetTableAlias());
            n = lpn.get(1).getChild(0);
            assertTrue(n instanceof IndexScanPlanNode);
        }

        {
            // Join of two multi-partitioned sub-queries on non-partition column. Should fail
            failToCompile("select A, C FROM (SELECT A FROM P1) TEMP1, (SELECT C FROM P2) TEMP2 WHERE TEMP1.A = TEMP2.C ",
                    "Statements are too complex in set operation or sub-query using multiple partitioned");
        }
        {
            // Join of two single partitioned sub-queries. Should compile
            List<AbstractPlanNode> lpn = compileToFragments("select D1, D2 FROM (SELECT D D1 FROM P1 WHERE A=1) TEMP1, (SELECT D D2 FROM P2 WHERE A=1) TEMP2 WHERE TEMP1.D1 = TEMP2.D2 ");
            assertTrue(lpn.size() == 1);
            AbstractPlanNode n = lpn.get(0).getChild(0).getChild(0);
            assertTrue(n instanceof NestLoopPlanNode);
            AbstractPlanNode c = n.getChild(0);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP1", ((SeqScanPlanNode) c).getTargetTableAlias());
            c = n.getChild(1);
            assertTrue(c instanceof SeqScanPlanNode);
            assertEquals("TEMP2", ((SeqScanPlanNode) c).getTargetTableAlias());
        }
        {
            // Join of two multi-partitioned sub-queries on the partition column. Should compile ?
            List<AbstractPlanNode> lpn = compileToFragments("select A1, A2 FROM (SELECT A A1 FROM P1) TEMP1, (SELECT A A2 FROM P2) TEMP2 WHERE TEMP1.A1 = TEMP2.A2 ");
            assertTrue(lpn.size() == 2);
        }
    }

    private void verifyOutputSchema(AbstractPlanNode pn, String... columns) {
        NodeSchema ns = pn.getOutputSchema();
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


    //failToCompile("select * from new_order where no_w_id in (select w_id from warehouse);",
    //        "VoltDB does not support subqueries");

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testsub-queries-ddl.sql"), "testsubqueries", false);
    }

}
