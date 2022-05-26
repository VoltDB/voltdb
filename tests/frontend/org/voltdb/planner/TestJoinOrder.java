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

import java.util.HashSet;
import java.util.List;

import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestJoinOrder extends PlannerTestCase {
    public void testBasicJoinOrder() {
        AbstractPlanNode pn = compileSPWithJoinOrder("select * FROM T1, T2, T3, T4, T5, T6, T7", "T7,T6,T5,T4,T3,T2,T1");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        for (int ii = 1; ii <= 7; ii++) {
            if (ii == 6) {
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii + 1)));
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii + 1)));
                break;
            } else {
                NestLoopPlanNode node = (NestLoopPlanNode)n;
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii)));
                n = node.getChild(0);
            }
        }

        pn = compileSPWithJoinOrder("select * FROM T1, T2, T3, T4, T5, T6, T7", "T1,T2,T3,T4,T5,T6,T7");
        n = pn.getChild(0);
        for (int ii = 7; ii > 0; ii--) {
            if (ii == 2) {
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii - 1)));
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii - 1)));
                break;
            } else {
                NestLoopPlanNode node = (NestLoopPlanNode)n;
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii)));
                n = node.getChild(0);
            }
        }

        pn = compileSPWithJoinOrder("select * from T1, T2 where A=B", "  T1  ,  T2  ");
        //* enable to debug */ System.out.println(pn.toExplainPlanString());
        n = pn.getChild(0);
        assertEquals("T1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("T2", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pn = compileSPWithJoinOrder("select * from T1, T2 where A=B", "  T2,T1  ");
        n = pn.getChild(0).getChild(0);
        assertEquals("T2", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("T1", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        // Don't mind a trailing comma -- even when followed by space.
        pn = compileSPWithJoinOrder("select * from T1, T2 where A=B", "T2,T1,  ");
        n = pn.getChild(0).getChild(0);
        assertEquals("T2", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("T1", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pn = compileSPWithJoinOrder("select * from T1, T2 where A=B", "T1,T2,");
        n = pn.getChild(0);
        assertEquals("T1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("T2", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        try {
            // Wants alias "T2", not bogus "Z"
            compileWithInvalidJoinOrder("select * from T1, T2 where A=B", "T1,Z");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" doesn't exist ") != -1);
        }

        try {
            // Wants comma, not semicolon -- does not parse the correct number of symbols.
            // The parser may give a smarter message in the future.
            compileWithInvalidJoinOrder("select * from T1, T2 where A=B", "T1;T2");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" does not contain the correct number of elements") != -1);
        }

        try {
            // Wants comma, not double comma -- does not parse the correct number of symbols.
            // The parser may give a smarter message in the future.
            compileWithInvalidJoinOrder("select * from T1, T2 where A=B", "T1,,T2");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" does not contain the correct number of elements") != -1);
        }

        try {
            // Does not want leading comma -- does not parse the correct number of symbols.
            // The parser may give a smarter message in the future.
            compileWithInvalidJoinOrder("select * from T1, T2 where A=B", ",T1,T2");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" does not contain the correct number of elements") != -1);
        }

        try {
            // Does not want leading comma after whitespace
            // -- does not parse the correct number of symbols.
            // The parser may give a smarter message in the future.
            compileWithInvalidJoinOrder("select * from T1, T2 where A=B", " ,T1,T2");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" does not contain the correct number of elements") != -1);
        }
    }

    public void testAliasJoinOrder() {
        List<AbstractPlanNode> pns;
        AbstractPlanNode n;

        pns = compileWithJoinOrderToFragments("select * from P1 X, P2 Y where A=B", "X,Y");
        n = pns.get(1).getChild(0);
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P2", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pns = compileWithJoinOrderToFragments("select * from P1, P2 Y where A=B", "P1,Y");
        n = pns.get(1).getChild(0);
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P2", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pns = compileWithJoinOrderToFragments("select * from P1 , P1 Y where P1.A=Y.A", "P1,Y");
        n = pns.get(1).getChild(0);
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        // Test case insensitivity in table and alias names

        pns = compileWithJoinOrderToFragments("select * from P1 x, P2 Y where A=B", "X,y");
        n = pns.get(1).getChild(0);
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P2", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pns = compileWithJoinOrderToFragments("select * from P1 x, P2 Y where A=B", "y,X");
        n = pns.get(1).getChild(0);
        assertEquals("P2", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pns = compileWithJoinOrderToFragments("select * from P1 , P1 YY where P1.A=YY.A", "P1,yY");
        n = pns.get(1).getChild(0);
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        pns = compileWithJoinOrderToFragments("select * from P1 , P1 Yy where P1.A=Yy.A", "p1,YY");
        n = pns.get(1).getChild(0);
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(0)).getTargetTableName());
        assertEquals("P1", ((SeqScanPlanNode)n.getChild(1)).getTargetTableName());

        try {
            // Wants alias "Y", not bogus "Z"
            compileWithInvalidJoinOrder("select * from P1 x, P2 Y where A=B", "x,Z");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" doesn't exist ") != -1);
        }

        try {
            // Wants alias "Y", not raw table name
            // -- in the future, this may get a specialized message -- or may just be allowed
            compileWithInvalidJoinOrder("select * from P1 x, P2 Y where A=B", "x,P2");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" doesn't exist ") != -1);
        }

        try {
            // Wants alias "Y", not raw table name (which is arguably ambiguous in this case).
            // -- in the future, this may get a specialized message -- or may just be allowed
            compileWithInvalidJoinOrder("select * from P1 x, P1 Y where x.A=Y.A", "x,P1");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" doesn't exist ") != -1);
        }

        try {
            // Wants aliases "x,Y", not raw table names (which are definitely ambiguous in this case).
            // -- in the future, this may get a specialized message -- or may just be allowed
            compileWithInvalidJoinOrder("select * from P1 x, P1 Y where x.A=Y.A", "P1,P1");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" contains a duplicate element ") != -1);
        }

    }

    public void testOuterJoinOrder() {
        AbstractPlanNode pn = compileSPWithJoinOrder("select * FROM T1 LEFT JOIN T2 ON T1.A = T2.B", "T1, T2");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().equals("T1"));
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("T2"));

        try {
            compileWithInvalidJoinOrder("select * FROM T1 LEFT JOIN T2 ON T1.A = T2.B", "T2, T1");
            fail();
        } catch (Exception ex) {
            assertTrue("The specified join order is invalid for the given query".equals(ex.getMessage()));
        }
    }

    public void testFullJoinOrder() {
        AbstractPlanNode pn = compileSPWithJoinOrder("select * FROM T1 JOIN T2 ON T1.A = T2.B FULL JOIN T3 ON T1.A = T3.C", "T1, T2, T3");
        AbstractPlanNode n = pn.getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, n.getPlanNodeType());
        assertEquals(JoinType.FULL, ((NestLoopPlanNode) n).getJoinType());
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("T3"));
        n = n.getChild(0);
        assertEquals(PlanNodeType.NESTLOOP, n.getPlanNodeType());
        assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().equals("T1"));
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("T2"));

        try {
            compileWithInvalidJoinOrder("select * FROM T1 JOIN T2 ON T1.A = T2.B FULL JOIN T3 ON T1.A = T3.C", "T3, T1, T2");
            fail();
        } catch (Exception ex) {
            assertTrue("The specified join order is invalid for the given query".equals(ex.getMessage()));
        }
    }

    public void testMicroOptimizationJoinOrder() {
        // Microoptimization can be used for determinism only when working with replicated tables or
        // single-partition queries.
        List<AbstractPlanNode> pns;
        AbstractPlanNode n;

        pns = compileWithJoinOrderToFragments("select * from J1, P2 where A=B and A=1", "J1, P2");
        n = pns.get(0).getChild(0);
        assertTrue(((IndexScanPlanNode)n.getChild(0)).getTargetTableName().equals("J1"));
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("P2"));

        pns = compileWithJoinOrderToFragments("select * from I1, T2 where A=B", "I1, T2");
        //* enable to debug */ System.out.println(pns.get(0).toExplainPlanString());
        n = pns.get(0).getChild(0);
        assertTrue(((IndexScanPlanNode)n.getChild(0)).getTargetTableName().equals("I1"));
        assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().equals("T2"));

    }

    public void testInnerOuterJoinOrder() {
        AbstractPlanNode pn;
        pn = compileSPWithJoinOrder(
                "select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7",
                "T2, T1, T3, T4, T5, T7, T6");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        String joinOrder[] = {"T2", "T1", "T3", "T4", "T5", "T7", "T6"};
        for (int i = 6; i > 0; i--) {
            assertTrue(n instanceof NestLoopPlanNode);
            assertTrue(n.getChild(1) instanceof SeqScanPlanNode);
            SeqScanPlanNode s = (SeqScanPlanNode) n.getChild(1);
            if (i == 1) {
                assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
                assertTrue(joinOrder[i-1].equals(((SeqScanPlanNode) n.getChild(0)).getTargetTableName()));
            } else {
                assertTrue(n.getChild(0) instanceof NestLoopPlanNode);
                n = n.getChild(0);
            }
            assertTrue(joinOrder[i].equals(s.getTargetTableName()));
        }

        try {
            compileWithInvalidJoinOrder("select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7",
                    "T2, T6, T3, T4, T5, T7, T1");
            fail();
        } catch (Exception ex) {
            assertTrue("The specified join order is invalid for the given query".equals(ex.getMessage()));
        }

        try {
            compileWithInvalidJoinOrder("select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7",
                    "T1, T2, T4, T3, T5, T7, T6");
            fail();
        } catch (Exception ex) {
            assertTrue("The specified join order is invalid for the given query".equals(ex.getMessage()));
        }

        try {
            compileWithInvalidJoinOrder("select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7",
                    "T1, T2, T3, T4, T5, T7");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" does not contain the correct number of elements") != -1);
        }

        try {
            compileWithInvalidJoinOrder("select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7",
                    "T1, T2, T3, T4, T5, T7, T6, T8");
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().indexOf(" does not contain the correct number of elements") != -1);
        }
    }

    private void checkJoinOrder(String sql, int... exceptions) {
        AbstractPlanNode pn, n;
        pn = compile(sql);
        // We want a send first.
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        n = pn.getChild(0);
        // We may see a projection node here.  It's for the
        // select list.  It may be optimized away, so don't
        // worry all that much.
        if (PlanNodeType.PROJECTION == n.getPlanNodeType()) {
            n = n.getChild(0);
        }
        //* enable to debug */ System.out.println(pn.toExplainPlanString());
        // starts from T7

        HashSet<Integer> mySets = new HashSet<>();
        for (int i : exceptions) {
            mySets.add(Integer.valueOf(i));
        }

        for (int ii = 7; ii > 0; ii--) {
            if (ii == 2) {
                assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii - 1)));
                assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii))
                        || ((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii - 1)));
                break;
            } else {
                NestLoopPlanNode node = (NestLoopPlanNode)n;
                if (mySets.contains(Integer.valueOf(ii))) {
                    assertTrue(((SeqScanPlanNode)n.getChild(0)).getTargetTableName().endsWith(Integer.toString(ii)));
                    n = node.getChild(1);
                } else {
                    // starts from T6 on child 1 because of the invalid join
                    assertTrue(((SeqScanPlanNode)n.getChild(1)).getTargetTableName().endsWith(Integer.toString(ii)));
                    n = node.getChild(0);
                }
            }
        }
    }

    private void validJoinOrder(String explainPlan, String... joinOrder) {
        int start = -1;
        for (String tb: joinOrder) {
            int idx = explainPlan.indexOf("\"" + tb + "\"");
            assertTrue(idx > start);
            start = idx;
        }
    }

    public void testMoreThan5TablesJoin() {
        String sql;

        sql = "select * FROM T1, T2, T3, T4, T5, T6, T7";
        checkJoinOrder(sql, -1);

        // Try the left outer join
        sql = "select * FROM T1, T2, T3, T4, T5, T6 left outer join T7 on T6.F = T7.G";
        checkJoinOrder(sql, -1);

        sql = "select * FROM T1, T2, T3 LEFT JOIN T4 ON T3.C = T4.D LEFT JOIN T5 ON T3.C = T5.E, T6,T7";
        checkJoinOrder(sql, -1);

        // Try the right outer join
        sql = "select * FROM T1, T2, T3, T4, T5, T6 right outer join T7 on T6.F = T7.G";
        checkJoinOrder(sql, 7);

        sql = "select * FROM T1, T2, T3, T4, T5 right outer join T6 on T6.F = T5.E, T7";
        checkJoinOrder(sql, 6);

        sql = "select * FROM T1, T2, T3, T4 right outer join T5 on T5.E = T4.D right outer join T6 on T6.F = T5.E, T7";
        checkJoinOrder(sql,5, 6);


        // Sub-queries is an interesting question to test
        AbstractPlanNode pn;
        sql = "select * FROM T1, T2, (select T4.D from T3 right outer join T4 on T4.D = T3.C) TM1 LEFT OUTER JOIN T5 on T5.E = TM1.D, T6, T7";
        pn = compile(sql);
        validJoinOrder(pn.toExplainPlanString(), "T1", "T2",  "T4", "T3", "T5", "T6", "T7");
        /*
        NEST LOOP INNER JOIN
         NEST LOOP INNER JOIN
          NEST LOOP LEFT JOIN
           filter by (T5.E = TM1.D)
           NEST LOOP INNER JOIN
            NEST LOOP INNER JOIN
             SEQUENTIAL SCAN of "T1"
             SEQUENTIAL SCAN of "T2"
            SEQUENTIAL SCAN of "TM1"
             NEST LOOP LEFT JOIN
              filter by (T4.D = T3.C)
              SEQUENTIAL SCAN of "T4"
              SEQUENTIAL SCAN of "T3"
           SEQUENTIAL SCAN of "T5"
          SEQUENTIAL SCAN of "T6"
         SEQUENTIAL SCAN of "T7"
       */
        pn = compileSPWithJoinOrder(sql, "T1,T2,TM1,T5,T6,T7");
        validJoinOrder(pn.toExplainPlanString(), "T1", "T2",  "T4", "T3", "T5", "T6", "T7");
        //
        // Join order not the input table order
        //

        // Do we have a case the join order as the input table order that is invalid ?!


    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(true, TestJoinOrder.class.getResource("testplans-joinorder-ddl.sql"), "testjoinorder");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }
}
