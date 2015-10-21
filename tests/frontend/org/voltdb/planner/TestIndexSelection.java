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

import java.util.List;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

public class TestIndexSelection extends PlannerTestCase {

    @Override
    protected void setUp() throws Exception {
        final boolean planForSinglePartitionFalse = false;
        setupSchema(TestIndexSelection.class.getResource("testplans-indexselection-ddl.sql"),
                    "testindexselectionplans",
                    planForSinglePartitionFalse);
    }

    // This tests recognition of a complex expression value
    // -- an addition -- used as an indexable join key's search key value.
    // Some time ago, this would throw a casting error in the planner.
    public void testEng3850ComplexIndexablePlan()
    {
        AbstractPlanNode pn = compile("select id from a, t where a.id < (t.a + ?);");
        pn = pn.getChild(0);
        pn = pn.getChild(0);
//        System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertEquals(IndexLookupType.LT, indexScan.getLookupType());
        assertTrue(indexScan.toJSONString().contains("\"TARGET_INDEX_NAME\":\"" +
                HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "ID"));
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
//        System.out.println("DEBUG: " + pn.toJSONString());
        assertTrue(pn.toJSONString().contains("\"TARGET_TABLE_NAME\":\"T\""));
    }

    // This tests recognition of covering parameters to prefer a hash index that would use a
    // greater number of key components than a competing tree index.
    // Not sure how this relates to ENG-931?
    public void testEng931Plan()
    {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ? and c = ? and d = ? " +
                                      "and e >= ? and e <= ?;");
        pn = pn.getChild(0);
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_1_HASH\""));
    }

    // This tests recognition of prefix parameters and constants to prefer an index that
    // would use a greater number of key components AND would give the desired ordering.
    public void testEng2541Plan() throws JSONException
    {
        AbstractPlanNode pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;");
        pn = pn.getChild(0);
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"IDX_B\""));
    }

    // This tests recognition of a prefix parameter and upper bound to prefer an index that would
    // use a greater number of key components even though another index would give the desired ordering.
    public void testEng4792PlanWithCompoundEQLTEOrderedByPK() throws JSONException
    {
        AbstractPlanNode pn = compile("select id from a where deleted=? and updated_date <= ? order by id limit ?;");
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        pn = pn.getChild(0);
        // ENG-5066: now Limit is pushed under Projection
        assertTrue(pn instanceof ProjectionPlanNode);
        pn = pn.getChild(0);
        // inline limit with order by
        assertTrue(pn instanceof OrderByPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.LIMIT));

        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        assertTrue(pn.toJSONString().contains("\"TARGET_INDEX_NAME\":\"DELETED_SINCE_IDX\""));
    }

    public void testFixedPlanWithExpressionIndexAndAlias()
    {
        AbstractPlanNode pn;
        IndexScanPlanNode ispn;
        String json;
        pn = compile(
                "select * from l aliased where  b = ? and DECODE(a, null, 0, a) = 0 and id = ?;");
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l aliased, l where l.b = ? and DECODE(l.a, null, 0, l.a) = 0 and l.id = ? and l.lname = aliased.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0).getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0).getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"" +
                HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "PK_LOG"));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(l.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname;");
        System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and l.id = ? and l.lname = x.lname;");
        System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*/ to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"IDX_A\""));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());

        pn = compile(
                "select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname;");
        System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send and Projection plan nodes.
        pn = pn.getChild(0).getChild(0);
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = (IndexScanPlanNode)pn.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        json = ispn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"" +
                HSQLInterface.AUTO_GEN_CONSTRAINT_WRAPPER_PREFIX + "PK_LOG"));
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        json = pn.toJSONString();
        assertTrue(json.contains("\"TARGET_INDEX_NAME\":\"DECODE_IDX3\""));
        ispn = (IndexScanPlanNode)pn;
        assertEquals(1, ispn.getSearchKeyExpressions().size());
    }

    public void testCaseWhenIndex()
    {
        AbstractPlanNode pn = compile("select * from l where CASE WHEN a > b THEN a ELSE b END > 8;");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("CASEWHEN_IDX1"));


        pn = compile("select * from l WHERE CASE WHEN a < 10 THEN a*5 ELSE a + 5 END > 2");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("CASEWHEN_IDX2"));

        // Negative case
        pn = compile("select * from l WHERE CASE WHEN a < 10 THEN a*2 ELSE a + 5 END > 2");
        pn = pn.getChild(0);
        System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("using its primary key index (for deterministic order only)"));
    }

    public void testPartialIndexNULLPredicate()
    {
        {
            AbstractPlanNode pn = compile("select * from c where a > 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
        {
            // CREATE INDEX partial_idx_null_e ON c (a) where e is null;
            AbstractPlanNode pn = compile("select * from c where a > 0 and e is NULL;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_NULL_E\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
            AbstractPlanNode pn = compile("select * from c where a > 0 and e is not NULL;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"A_PARTIAL_IDX_NOT_NULL_E\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
            // range-scan covering from (A > 0) to end Z_FULL_IDX_A has higher cost
            AbstractPlanNode pn = compile("select * from c where a > 0 and e = 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"A_PARTIAL_IDX_NOT_NULL_E\"");
            String[] columns = {"E"};
            checkIndexPredicateContains(pn, columns);
        }
        {
            // CREATE INDEX a_partial_idx_not_null_d_e ON c (a+b) where (d + e) is not null;
            AbstractPlanNode pn = compile("select * from c where a + b > 0 and 0 = abs(e + d);");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"A_PARTIAL_IDX_NOT_NULL_D_E\"");
            String[] columns = {"E", "D"};
            checkIndexPredicateContains(pn, columns);
        }
        {
            // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
            AbstractPlanNode pn = compile("select * from c where a > 0 and 0 = abs(e + b);");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"A_PARTIAL_IDX_NOT_NULL_E\"");
            String[] columns = {"E", "B"};
            checkIndexPredicateContains(pn, columns);
        }
        {
            // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
            // uniquely match (A = 0) Z_FULL_IDX_A has the lowest cost
            AbstractPlanNode pn = compile("select * from c where a = 0 and e = 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
            String[] columns = {"E"};
            checkIndexPredicateContains(pn, columns);
        }
    }

    public void testPartialIndexArbitraryPredicate()
    {
        {
            // CREATE INDEX partial_idx_or_expr ON c (f) where e > 0 or d < 5;
            AbstractPlanNode pn = compile("select * from c where f > 0 and (e > 0 or d < 5);");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_OR_EXPR\"");
            checkIndexPredicateIsNull(pn);
        }
    }

    public void testPartialIndexComparisonPredicateExactMatch()
    {
        {
            // CREATE INDEX partial_idx_or_expr ON c (a) where e > 0 or d < 5; -- expression trees differ
            AbstractPlanNode pn = compile("select * from c where a > 0 and e > 0 or d < 5;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
        {
            // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1;
            AbstractPlanNode pn = compile("select * from c where abs(b) = 1 and abs(e) > 1;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_1\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1;
            AbstractPlanNode pn = compile("select * from c where abs(b) > 1 and 1 < abs(e);");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_1\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5;
            // CREATE INDEX partial_idx_3 ON c (b) where d > 0; is also a match
            // but has higher cost because of the extra post filter
            AbstractPlanNode pn = compile("select * from c where b > 0 and d > 0 and d < 5;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_2\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5;
            AbstractPlanNode pn = compile("select * from c where b > 0 and d < 5 and 0 < d;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_2\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f;
            AbstractPlanNode pn = compile("select * from c where a > 0 and b > 0 and f > 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_4\"");
            String[] columns = {"F"};
            checkIndexPredicateDoesNotHave(pn, columns);
        }
        {
            // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f;
            AbstractPlanNode pn = compile("select * from c where a > 0 and b > 0 and 0 < f;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_4\"");
            String[] columns = {"F"};
            checkIndexPredicateDoesNotHave(pn, columns);
        }
        {
            // CREATE INDEX partial_idx_5 ON c (b) where d > f;
            AbstractPlanNode pn = compile("select * from c where b > 0 and d > f;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_5\"");
            checkIndexPredicateIsNull(pn);
        }
        {
            // CREATE INDEX partial_idx_5 ON c (b) where d > f;
            AbstractPlanNode pn = compile("select * from c where b > 0 and f < d;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_5\"");
            checkIndexPredicateIsNull(pn);
        }
    }

    public void testPartialIndexComparisonPredicateNonExactMatch()
    {
        // At the moment an index filter must exactly match a query filter expression (or sub-expression)
        // to be selected
        {
            // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; Not exact match
            AbstractPlanNode pn = compile("select * from c where abs(b) > 1 and 2 <  abs(e);");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
        {
            // CREATE INDEX partial_idx_3 ON c (b) where d > 0; Not exact match
            AbstractPlanNode pn = compile("select * from c where b > 0 and d > 3;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
        {
            // CREATE INDEX partial_idx_5 ON c (b) where d > f;  Not exact match
            AbstractPlanNode pn = compile("select * from c where b > 0 and f + 1 < d;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
    }

    public void testPartialIndexPredicateOnly()
    {
        // Partial index can be used solely to eliminate a post-filter
        // even when the indexed columns are irrelevant
        {
            // CREATE INDEX partial_idx_3 ON c (b)  where d > 0;
            AbstractPlanNode pn = compile("select * from c where d > 0");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_3\"");
        }
        {
            // CREATE UNIQUE INDEX z_full_idx_a ON c (a); takes precedence over the partial_idx_3
            // because indexed column (A) is part of the WHERE expressions
            AbstractPlanNode pn = compile("select * from c where d > 0 and a < 0");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
        {
            // CREATE INDEX partial_idx_3 ON c (b)  where d > 0;
            AbstractPlanNode pn = compile("select c.d from a join c on a.id = c.e and d > 0");
            assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getChild(0).getChild(0).getPlanNodeType());
            checkIndexName(pn.getChild(0).getChild(0), PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_3\"");
        }
    }

    public void testParameterizedQueryPartialIndex()
    {
        {
            // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
            // range-scan covering from (A > 0) to end Z_FULL_IDX_A has higher cost
            AbstractPlanNode pn = compile("select * from c where a > 0 and e = ?;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"A_PARTIAL_IDX_NOT_NULL_E\"");
            String[] columns = {"E"};
            checkIndexPredicateContains(pn, columns);
        }
        {
            // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f; - not selected because of the parameter
            AbstractPlanNode pn = compile("select * from c where a > 0 and b > 0 and ? < f;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
        {
            // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; not selected because of the parameter
            AbstractPlanNode pn = compile("select * from c where abs(b) = 1 and abs(e) > ?;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
        }
    }

    public void testSkipNullPartialIndex() {
        {
            //CREATE INDEX partial_idx_7 ON c (g) where g is not null;
            // skipNull predicate is redundant and eliminated
            AbstractPlanNode pn = compile("select count(*) from c where g > 0;");
            checkIndexName(pn, PlanNodeType.INDEXCOUNT, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_7\"");
            checkIndexSkipNullPredicateIsNull(pn, false);
        }
        {
            //CREATE INDEX partial_idx_7 ON c (g) where g is not null;
            // skipNull predicate is redundant and eliminated
            AbstractPlanNode pn = compile("select e from c where g > 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_7\"");
            checkIndexSkipNullPredicateIsNull(pn, false);
        }
        {
            //CREATE INDEX partial_idx_6 ON c (g) where g < 0;
            // skipNull predicate is redundant and eliminated
            AbstractPlanNode pn = compile("select count(*) from c where g < 0;");
            checkIndexName(pn, PlanNodeType.INDEXCOUNT, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_6\"");
            checkIndexSkipNullPredicateIsNull(pn, false);
        }
        {
            //CREATE INDEX partial_idx_6 ON c (g) where g < 0;
            // skipNull predicate is redundant and eliminated
            AbstractPlanNode pn = compile("select g from c where g < 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_6\"");
            checkIndexSkipNullPredicateIsNull(pn, false);
        }
        {
            // CREATE UNIQUE INDEX z_full_idx_a ON c (a);
            // skipNull is required - full index
            AbstractPlanNode pn = compile("select count(*) from c where a > 0;");
            checkIndexName(pn, PlanNodeType.INDEXCOUNT, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
            checkIndexSkipNullPredicateIsNull(pn, true);
        }
        {
            // CREATE UNIQUE INDEX z_full_idx_a ON c (a);
            // skipNull is required - full index
            AbstractPlanNode pn = compile("select e from c where a > 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"Z_FULL_IDX_A\"");
            checkIndexSkipNullPredicateIsNull(pn, true);
        }
        {
            // CREATE INDEX partial_idx_3 ON c (b) where d > 0;
            // skipNull is required - index predicate is not NULL-rejecting for column B
            AbstractPlanNode pn = compile("select count(*) from c where b > 0 and d > 0;");
            checkIndexName(pn, PlanNodeType.INDEXCOUNT, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_3\"");
            checkIndexSkipNullPredicateIsNull(pn, true);
        }
        {
            // CREATE INDEX partial_idx_3 ON c (b) where d > 0;
            // skipNull is required - index predicate is not NULL-rejecting for column B
            AbstractPlanNode pn = compile("select b from c where b > 0 and d > 0;");
            checkIndexName(pn, PlanNodeType.INDEXSCAN, "\"TARGET_INDEX_NAME\":\"PARTIAL_IDX_3\"");
            checkIndexSkipNullPredicateIsNull(pn, true);
        }
    }

    private void checkIndexName(AbstractPlanNode pn, PlanNodeType targetPlanNodeType, String targetIndexName)
    {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        String json = pn.toJSONString();
        assertEquals(targetPlanNodeType, pn.getPlanNodeType());
        assertTrue(json.contains(targetIndexName));
    }

    private void checkIndexPredicateIsNull(AbstractPlanNode pn)
    {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
        AbstractExpression pred = ipn.getPredicate();
        assertTrue(pred == null);
    }

    private void checkIndexPredicateContains(AbstractPlanNode pn, String[] columns)
    {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
        AbstractExpression pred = ipn.getPredicate();
        assertTrue(pred != null);
        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(pred);
        for (TupleValueExpression tve : tves) {
            boolean match = false;
            for (String column: columns) {
                if (match = tve.getColumnName().equals(column)) {
                    break;
                }
            }
            assertEquals(true, match);
        }
    }

    private void checkIndexPredicateDoesNotHave(AbstractPlanNode pn, String[] columns)
    {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
        AbstractExpression pred = ipn.getPredicate();
        assertTrue(pred != null);
        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(pred);
        for (TupleValueExpression tve : tves) {
            for (String column: columns) {
                assertTrue(!tve.getColumnName().equals(column));
            }
        }
    }

    private void checkIndexSkipNullPredicateIsNull(AbstractPlanNode pn, boolean hasSkipNullPredicate) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        String json = pn.toJSONString();
        if (pn instanceof IndexCountPlanNode) {
            assertEquals(hasSkipNullPredicate, json.contains("SKIP_NULL_PREDICATE"));
        } else {
            // index scan
            AbstractExpression skipNull = ((IndexScanPlanNode) pn).getSkipNullPredicate();
            assertEquals(hasSkipNullPredicate, skipNull != null);
        }
    }
}
