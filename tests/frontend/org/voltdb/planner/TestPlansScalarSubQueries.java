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

import java.util.List;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ScalarValueExpression;
import org.voltdb.expressions.SelectSubqueryExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UnionPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.QuantifierType;

public class TestPlansScalarSubQueries extends PlannerTestCase {

    public void testSelectScalar() {
        AbstractPlanNode pn = compile("select r2.c, (select d from r1) scalar from r2");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractPlanNode proj = pn.getInlinePlanNode(PlanNodeType.PROJECTION);
        NodeSchema schema = proj.getOutputSchema();
        assertEquals(2, schema.size());
        SchemaColumn col = schema.getColumn(1);
        assertTrue(col != null);
        assertEquals("SCALAR", col.getColumnName());
    }

    public void testSelectCorrelatedScalar() {
        AbstractPlanNode pn = compile("select r2.c, (select d from r1 where r1.c = r2.c ) scalar from r2");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractPlanNode proj = pn.getInlinePlanNode(PlanNodeType.PROJECTION);
        NodeSchema schema = proj.getOutputSchema();
        assertEquals(2, schema.size());
        SchemaColumn col = schema.getColumn(1);
        assertTrue(col != null);
        assertEquals("SCALAR", col.getColumnName());
        AbstractExpression colExpr = col.getExpression();
        assertEquals(ExpressionType.VALUE_SCALAR, colExpr.getExpressionType());
        assertTrue(colExpr.getLeft() instanceof AbstractSubqueryExpression);
        AbstractSubqueryExpression subqueryExpr = (AbstractSubqueryExpression) colExpr.getLeft();
        List<Integer> params = subqueryExpr.getParameterIdxList();
        assertEquals(1, params.size());
        assertEquals(new Integer(0), params.get(0));
    }

    public void testSelectCorrelatedScalarWithGroupby() {
        String sql = "select franchise_id, count(*) as stores_in_category_AdHoc, "
                + " (select category from store_types where type_id = stores.type_id) as store_category "
                + "from stores group by franchise_id, type_id;";

        AbstractPlanNode pn = compile(sql);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        NodeSchema schema = pn.getOutputSchema();
        assertEquals(3, schema.size());
        SchemaColumn col = schema.getColumn(2);
        assertTrue(col != null);
        assertEquals("STORE_CATEGORY", col.getColumnName());
        assertTrue(col.getExpression() instanceof ScalarValueExpression);
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
    }

    public void testSelectCorrelatedScalarInGroupbyClause() {
        String sql = "select franchise_id, count(*) as stores_in_category_AdHoc "
                + " from stores group by franchise_id, (select category from store_types where type_id = stores.type_id);";
        AbstractPlanNode pn = compile(sql);
        pn = pn.getChild(0);
        assertTrue(pn instanceof ProjectionPlanNode);
        NodeSchema schema = pn.getOutputSchema();
        assertEquals(2, schema.size());
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        assertNotNull(pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE));
        HashAggregatePlanNode aggNode = (HashAggregatePlanNode) pn.getInlinePlanNode(PlanNodeType.HASHAGGREGATE);
        assertEquals(2, aggNode.getGroupByExpressionsSize());
        AbstractExpression tveExpr = aggNode.getGroupByExpressions().get(0);
        assertTrue(tveExpr instanceof TupleValueExpression);
        AbstractExpression gbExpr = aggNode.getGroupByExpressions().get(1);
        assertTrue(gbExpr instanceof ScalarValueExpression);
        assertTrue(gbExpr.getLeft() instanceof SelectSubqueryExpression);
    }

    // negative tests not to support subquery inside of aggregate function
    public void testSelectScalarInAggregation() {
        String sql, errorMsg = "SQL Aggregate function calls with subquery expression arguments are not allowed.";

        // non-correlated
        sql = "select franchise_id, sum((select count(category) from store_types where type_id = 3)) as stores_in_category_AdHoc "
                + " from stores group by franchise_id;";
        failToCompile(sql, errorMsg);

        // expression with subquery
        sql = "select franchise_id, sum(1 + (select count(category) from store_types where type_id = 3)) as stores_in_category_AdHoc "
                + " from stores group by franchise_id;";
        failToCompile(sql, errorMsg);

        // correlated
        sql = "select franchise_id, sum((select count(category) from store_types where type_id = stores.franchise_id)) as stores_in_category_AdHoc "
                + " from stores group by franchise_id;";
        failToCompile(sql, "object not found: STORES.FRANCHISE_ID");
    }

    public void testSelectParameterScalar() {
        AbstractPlanNode pn = compile("select r2.c, (select d from r1 where r1.c = ? ) scalar from r2");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractPlanNode proj = pn.getInlinePlanNode(PlanNodeType.PROJECTION);
        NodeSchema schema = proj.getOutputSchema();
        assertEquals(2, schema.size());
        SchemaColumn col = schema.getColumn(1);
        assertTrue(col != null);
        assertEquals("SCALAR", col.getColumnName());
        AbstractExpression colExpr = col.getExpression();
        assertEquals(ExpressionType.VALUE_SCALAR, colExpr.getExpressionType());
        assertTrue(colExpr.getLeft() instanceof AbstractSubqueryExpression);
        AbstractSubqueryExpression subqueryExpr = (AbstractSubqueryExpression) colExpr.getLeft();
        AbstractPlanNode subquery = subqueryExpr.getSubqueryNode();
        assertEquals(PlanNodeType.SEQSCAN, subquery.getPlanNodeType());
        AbstractExpression pred = ((SeqScanPlanNode) subquery).getPredicate();
        assertEquals(ExpressionType.VALUE_PARAMETER, pred.getRight().getExpressionType());
    }

    public void testMultiColumnSelect() {
        failToCompile("select r2.c, (select d, c from r1) from r2",
                "Scalar subquery can have only one output column");
    }

    public void testWhereEqualScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where (select r1.a from r1) = r2.c;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
    }

    public void testWhereGreaterScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where (select r1.a from r1) > r2.c;");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_LESSTHAN, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
    }

    public void testWhereParamScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c = (select r1.a from r1 where r1.a = r2.c);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        assertEquals(1, pred.getRight().getArgs().size());
    }

    public void testWhereUserParamScalar() {
        AbstractPlanNode pn = compile("select r2.c from r2 where r2.c = (select r1.a from r1 where r1.a = ?);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.VALUE_TUPLE, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        assertEquals(0, pred.getRight().getArgs().size());
    }

    /**
     * ENG-8203: Index usage for scalar value expression is disabled.
     * All the where clause are used as post predicate in the index scan, other than search key or end key.
     */
    public void testWhereIndexScalar() {
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where r5.a = (select r1.a from r1 where r1.a = ?);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            assertNull(((IndexScanPlanNode) pn).getEndExpression());
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where (select r1.a from r1 where r1.a = ?) < r5.a;");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            List<AbstractExpression> exprs = ((IndexScanPlanNode) pn).getSearchKeyExpressions();
            assertEquals(0, exprs.size());
            assertNotNull(((IndexScanPlanNode) pn).getPredicate());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where r5.a IN (select r1.a from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getLeft().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where  r5.a = ANY (select r1.a from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression expr = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, expr.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, expr.getLeft().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where  r5.a = ANY (select r1.a from r1 limit 3 offset 4);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression expr = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, expr.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, expr.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select r5.c from r5 where  r5.a > ALL (select r1.a from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression expr = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.SELECT_SUBQUERY, expr.getRight().getExpressionType());
        }
    }

    public void testWhereGreaterRow() {
        AbstractPlanNode pn = compile("select r5.c from r5 where (a,c) > (select r1.a, r1.c from r1);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_GREATERTHAN, pred.getExpressionType());
        assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
    }

    public void testWhereEqualRow() {
        AbstractPlanNode pn = compile("select r2.c from r2 where (a,c) = (select r1.a, r1.c from r1 where r1.c = r2.c);");
        pn = pn.getChild(0);
        assertTrue(pn instanceof AbstractScanPlanNode);
        AbstractExpression pred = ((AbstractScanPlanNode) pn).getPredicate();
        assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
        assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        assertEquals(1, pred.getRight().getArgs().size());
    }

    public void testWhereRowMismatch() {
        failToCompile("select r2.c from r2 where (a,c) = (select a, a , 5 from r1);",
                "row column count mismatch");
    }

    /**
     * Uncomment these tests when ENG-8306 is finished
     */
    public void testHavingScalar() {
        failToCompile("select max(r2.c) from r2 group by r2.c having count(c) = (select a from r1);",
                TestPlansInExistsSubQueries.HavingErrorMsg);

//        AbstractPlanNode pn = compile("select max(r2.c) from r2 group by r2.c having count(*) = (select a from r1);");
//        pn = pn.getChild(0).getChild(0);
//        assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
//        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
//        AbstractExpression aggExpr = aggNode.getPostPredicate();
//        assertEquals(ExpressionType.SELECT_SUBQUERY, aggExpr.getRight().getExpressionType());
    }

    /**
     * Uncomment these tests when ENG-8306 is finished
     */
    public void testHavingRow() {
        failToCompile("select max(r2.c) from r2 group by r2.c having (count(c), max(r2.c)) = (select a,c from r1);",
                TestPlansInExistsSubQueries.HavingErrorMsg);

//        AbstractPlanNode pn = compile("select max(r2.c) from r2 group by r2.c having (count(*), max(r2.c)) = (select a,c from r1);");
//        pn = pn.getChild(0).getChild(0);
//        assertEquals(PlanNodeType.SEQSCAN, pn.getPlanNodeType());
//        AggregatePlanNode aggNode = AggregatePlanNode.getInlineAggregationNode(pn);
//        AbstractExpression aggExpr = aggNode.getPostPredicate();
//        assertEquals(ExpressionType.SELECT_SUBQUERY, aggExpr.getRight().getExpressionType());
    }

    public void testHavingRowMismatch() {
        failToCompile("select max(r2.c) from r2 group by r2.c having (count(*), max(r2.c)) = (select a,c, 5 from r1);",
                "row column count mismatch");
    }

    public void testWhereIndexRow() {
        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) = (select a, c from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select * from r5 where (select a, c from r1) >= (a,c);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_GREATERTHANOREQUALTO, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getLeft().getExpressionType());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getRight().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) IN (select a, c from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.OPERATOR_EXISTS, pred.getExpressionType());
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getLeft().getExpressionType());
        }
        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) IN (select a, c from r1 limit 1 offset 4);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            assertEquals(QuantifierType.ANY, ((ComparisonExpression) pred).getQuantifier());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        }

        {
            AbstractPlanNode pn = compile("select * from r5 where (a,c) > ALL (select a, c from r1);");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertEquals(ExpressionType.COMPARE_GREATERTHAN, pred.getExpressionType());
            assertEquals(QuantifierType.ALL, ((ComparisonExpression) pred).getQuantifier());
            assertEquals(ExpressionType.ROW_SUBQUERY, pred.getLeft().getExpressionType());
        }
    }

    public void testUnion() {
        {
            AbstractPlanNode pn = compile(
                    "select * "
                            + "from r4 "
                            + "where (a, c) = all ("
                            + "    select * from R3 "
                            + "  union "
                            + "    select * from R5)");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertNotNull(pred);
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            pred = pred.getRight();
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getExpressionType());

            SelectSubqueryExpression selSubq = (SelectSubqueryExpression)pred;
            assertEquals(0, selSubq.getArgs().size()); // no correlation params
            AbstractPlanNode subqPlanNode = selSubq.getSubqueryNode();
            assertNotNull(subqPlanNode);
            assertTrue(subqPlanNode instanceof UnionPlanNode);
        }
        {
            // Correlation parameter on the LHS of the union
            AbstractPlanNode pn = compile(
                    "select * "
                            + "from r4 as outer_tbl "
                            + "where (a, c) = all ("
                            + "    select * from R3 "
                            + "    where outer_tbl.a = c "
                            + "  union "
                            + "    select * from R5)");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertNotNull(pred);
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            pred = pred.getRight();
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getExpressionType());

            SelectSubqueryExpression selSubq = (SelectSubqueryExpression)pred;
            List<AbstractExpression> args = selSubq.getArgs();
            assertEquals(1, args.size()); // one correlation param
            assertEquals(ExpressionType.VALUE_TUPLE, args.get(0).getExpressionType());

            TupleValueExpression tve = (TupleValueExpression)args.get(0);
            assertEquals("OUTER_TBL", tve.getTableAlias());
            assertEquals("A", tve.getColumnName());

            AbstractPlanNode subqPlanNode = selSubq.getSubqueryNode();
            assertNotNull(subqPlanNode);
            assertTrue(subqPlanNode instanceof UnionPlanNode);
        }
        {
            // Correlation parameter on the RHS of the union
            AbstractPlanNode pn = compile(
                    "select * "
                            + "from r4 as outer_tbl "
                            + "where (a, c) = all ("
                            + "    select * from R3 "
                            + "  union "
                            + "    select * from R5"
                            + "    where outer_tbl.a = c)");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertNotNull(pred);
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            pred = pred.getRight();
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getExpressionType());

            SelectSubqueryExpression selSubq = (SelectSubqueryExpression)pred;
            List<AbstractExpression> args = selSubq.getArgs();
            assertEquals(1, args.size()); // one correlation param
            assertEquals(ExpressionType.VALUE_TUPLE, args.get(0).getExpressionType());

            TupleValueExpression tve = (TupleValueExpression)args.get(0);
            assertEquals("OUTER_TBL", tve.getTableAlias());
            assertEquals("A", tve.getColumnName());

            AbstractPlanNode subqPlanNode = selSubq.getSubqueryNode();
            assertNotNull(subqPlanNode);
            assertTrue(subqPlanNode instanceof UnionPlanNode);
        }
        {
            // Correlation parameter in an intersect under a union
            AbstractPlanNode pn = compile(
                    "select * "
                            + "from r4 as outer_tbl "
                            + "where (a, c) = all ("
                            + "    select * from R3 "
                            + "  union "
                            + "      (select * from R4 "
                            + "    intersect"
                            + "      select * from R5"
                            + "      where outer_tbl.a = c))");
            pn = pn.getChild(0);
            assertTrue(pn instanceof IndexScanPlanNode);
            AbstractExpression pred = ((IndexScanPlanNode) pn).getPredicate();
            assertNotNull(pred);
            assertEquals(ExpressionType.COMPARE_EQUAL, pred.getExpressionType());
            pred = pred.getRight();
            assertEquals(ExpressionType.SELECT_SUBQUERY, pred.getExpressionType());

            SelectSubqueryExpression selSubq = (SelectSubqueryExpression)pred;
            List<AbstractExpression> args = selSubq.getArgs();
            assertEquals(1, args.size()); // one correlation param
            assertEquals(ExpressionType.VALUE_TUPLE, args.get(0).getExpressionType());

            TupleValueExpression tve = (TupleValueExpression)args.get(0);
            assertEquals("OUTER_TBL", tve.getTableAlias());
            assertEquals("A", tve.getColumnName());

            AbstractPlanNode subqPlanNode = selSubq.getSubqueryNode();
            assertNotNull(subqPlanNode);
            assertTrue(subqPlanNode instanceof UnionPlanNode);

        }
    }

    public void testScalarGuard() {
        String errorMessage = PlanAssembler.IN_EXISTS_SCALAR_ERROR_MESSAGE;

        failToCompile("select * from r5 where (a,c) > ALL (select a, c from p1);", errorMessage);
        failToCompile("select r2.c from r2 where r2.c = (select p1.a from p1 where p1.a = r2.c);", errorMessage);

        // partition table in the UNION clause
        // 2 partition tables
        failToCompile("select * from r2 where r2.c > "
                + " (select p1.a from p1 where p1.a = r2.a UNION select p2.a from p2 where p2.a = r2.a);", errorMessage);
        // 1 partition table and 1 replicated table
        failToCompile("select * from r2 where r2.c > "
                + " (select r4.a from r4 where r4.a = r2.a UNION select p2.a from p2 where p2.a = r2.a);", errorMessage);
        // 2 tables with the same table alias
        failToCompile("select * from r2 where r2.c > "
                + " (select p2.a from r4 as p2 where p2.a = r2.a UNION select p2.a from p2 where p2.a = r2.a);", errorMessage);
        // swap the UNION order for the previous case
        failToCompile("select * from r2 where r2.c > "
                + " (select p2.a from p2 where p2.a = r2.a UNION select p2.a from r4 as p2 where p2.a = r2.a);", errorMessage);
        // Join in the right clause of UNION
        failToCompile("select * from r2 where r2.c > "
                + " (select r4.a from r4 where r4.a = r2.a UNION select p2.a from p2, r3 where p2.a = r2.a and p2.a = r3.a);", errorMessage);
        // partition sub-query in the UNION
        failToCompile("select * from r2 where r2.c > "
                + " (select r4.a from r4 where r4.a = r2.a UNION "
                + "  select tb.c from (select a, c from p2 where a = 3 and d > 2) tb, r3 where tb.a = r2.a and tb.a = r3.a);", errorMessage);
    }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestPlansSubQueries.class.getResource("testplans-subqueries-ddl.sql"), "ddl", false);
        //        AbstractPlanNode.enableVerboseExplainForDebugging();
        //        AbstractExpression.enableVerboseExplainForDebugging();
    }

}
