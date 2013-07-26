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

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.JoinType;
import org.voltdb.types.PlanNodeType;

public class TestPlansJoin extends PlannerTestCase {

    public void testBasicInnerJoin() {
        // select * with ON clause should return all columns from all tables
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        //assertTrue(nlj.getJoinType() == JoinType.INNER);
        for (int ii = 0; ii < 2; ii++) {
            assertTrue(n.getChild(ii) instanceof SeqScanPlanNode);
        }
        assertTrue(pn.getOutputSchema().getColumns().size() == 5);

        // select * with USING clause should contain only one column for each column from the USING expression
        pn = compile("select * FROM R1 JOIN R2 USING(C)");
        assertTrue(pn.getChild(0).getChild(0) instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 4);

        pn = compile("select A,C,D FROM R1 JOIN R2 ON R1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 3);

        pn = compile("select A,C,D FROM R1 JOIN R2 USING(C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 3);

        pn = compile("select R1.A, R2.C, R1.D FROM R1 JOIN R2 ON R1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 3);
        assertTrue("R1".equalsIgnoreCase(pn.getOutputSchema().getColumns().get(0).getTableName()));
        assertTrue("R2".equalsIgnoreCase(pn.getOutputSchema().getColumns().get(1).getTableName()));

        // The output table for C canbe either R1 or R2 because it's an INNER join
        pn = compile("select R1.A, C, R1.D FROM R1 JOIN R2 USING(C)");
        n = pn.getChild(0).getChild(0);
        String table = pn.getOutputSchema().getColumns().get(1).getTableName();
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 3);
        assertTrue(pn.getOutputSchema().getColumns().get(0).getTableName().equalsIgnoreCase("R1"));
        assertTrue("R2".equalsIgnoreCase(table) || "R1".equalsIgnoreCase(table));

        // Column from USING expression can not have qualifier in the SELECT clause
        failToCompile("select R1.C FROM R1 JOIN R2 USING(C)",
                      "user lacks privilege or object not found: R1.C");
        failToCompile("select R2.C FROM R1 JOIN R2 USING(C)",
                      "user lacks privilege or object not found: R2.C");
        failToCompile("select R2.C FROM R1 JOIN R2 USING(X)",
                      "user lacks privilege or object not found: X");
        failToCompile("select R2.C FROM R1 JOIN R2 ON R1.X = R2.X",
                      "user lacks privilege or object not found: R1.X");
    }

    public void testBasicThreeTableInnerJoin() {
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON R1.C = R2.C JOIN R3 ON R3.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 7);

        pn = compile("select R1.C, R2.C R3.C FROM R1 INNER JOIN R2 ON R1.C = R2.C INNER JOIN R3 ON R3.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof NestLoopPlanNode);

        pn = compile("select C FROM R1 INNER JOIN R2 USING (C) INNER JOIN R3 USING(C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof NestLoopPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 1);

        pn = compile("select C FROM R1 INNER JOIN R2 USING (C), R3 WHERE R1.A = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof NestLoopIndexPlanNode);
        assertTrue(pn.getOutputSchema().getColumns().size() == 1);

        failToCompile("select C, R3.C FROM R1 INNER JOIN R2 USING (C) INNER JOIN R3 ON R1.C = R3.C",
                      "user lacks privilege or object not found: R1.C");
    }

    public void testScanJoinConditions() {
        AbstractPlanNode pn = compile("select * FROM R1 WHERE R1.C = 0");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        AbstractScanPlanNode scan = (AbstractScanPlanNode) n;
        AbstractExpression p = scan.getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);

        pn = compile("select * FROM R1, R2 WHERE R1.A = R2.A AND R1.C > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue(((AbstractScanPlanNode) n).getTargetTableName().equalsIgnoreCase("R1"));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN);

        pn = compile("select * FROM R1, R2 WHERE R1.A = R2.A AND R1.C > R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.CONJUNCTION_AND);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.COMPARE_LESSTHAN);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(((AbstractScanPlanNode)n.getChild(0)).getPredicate() == null);
        assertTrue(((AbstractScanPlanNode)n.getChild(1)).getPredicate() == null);

        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN);

        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.A WHERE R1.C > R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.CONJUNCTION_AND);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.COMPARE_LESSTHAN);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(((AbstractScanPlanNode)n.getChild(0)).getPredicate() == null);
        assertTrue(((AbstractScanPlanNode)n.getChild(1)).getPredicate() == null);

        pn = compile("select * FROM R1, R2, R3 WHERE R1.A = R2.A AND R1.C = R3.C AND R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.CONJUNCTION_AND);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN);

        pn = compile("select * FROM R1 JOIN R2 on R1.A = R2.A AND R1.C = R2.C where R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.CONJUNCTION_AND);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((AbstractScanPlanNode) n).getTargetTableName()));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN);

        pn = compile("select A,C FROM R1 JOIN R2 USING (A, C)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.CONJUNCTION_AND);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.COMPARE_EQUAL);

        pn = compile("select * FROM R1 JOIN R2 ON R1.A = R2.A JOIN R3 ON R1.C = R3.C WHERE R1.A > 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.CONJUNCTION_AND);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        n = n.getChild(0);
        assertTrue(n instanceof AbstractScanPlanNode);
        assertTrue(((AbstractScanPlanNode) n).getTargetTableName().equalsIgnoreCase("R1"));
        p = ((AbstractScanPlanNode) n).getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_GREATERTHAN);
}

    public void testTransitiveValueEquivalenceConditions() {
        // R1.A = R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A AND R2.A = 1 ");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        AbstractJoinPlanNode jn = (AbstractJoinPlanNode) n;
        assertTrue(jn.getJoinPredicate() == null);
        AbstractExpression p = jn.getPreJoinPredicate();
        assertTrue(p != null);
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(p.getExpressionType()));
        assertTrue(ExpressionType.VALUE_CONSTANT.equals(p.getLeft().getExpressionType()));
        assertTrue(ExpressionType.VALUE_TUPLE.equals(p.getRight().getExpressionType()));
        assertTrue(jn.getChild(1) instanceof SeqScanPlanNode);
        SeqScanPlanNode ssn = (SeqScanPlanNode)jn.getChild(1);
        assertTrue(ssn.getPredicate() != null);
        p = ssn.getPredicate();
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(p.getExpressionType()));
        assertTrue(ExpressionType.VALUE_TUPLE.equals(p.getLeft().getExpressionType()));
        assertTrue(ExpressionType.VALUE_CONSTANT.equals(p.getRight().getExpressionType()));

        // Same test but now R2 is outer table R1.A = R2.A AND R2.A = 1 => R1.A = 1 AND R2.A = 1
        pn = compile("select * FROM R2 LEFT JOIN R1 ON R1.A = R2.A AND R2.A = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        jn = (AbstractJoinPlanNode) n;
        assertTrue(jn.getJoinPredicate() == null);
        p = jn.getPreJoinPredicate();
        assertTrue(p != null);
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(p.getExpressionType()));
        assertTrue(ExpressionType.VALUE_TUPLE.equals(p.getLeft().getExpressionType()));
        assertTrue(ExpressionType.VALUE_CONSTANT.equals(p.getRight().getExpressionType()));
        assertTrue(jn.getChild(1) instanceof SeqScanPlanNode);
        ssn = (SeqScanPlanNode)jn.getChild(1);
        assertTrue(ssn.getPredicate() != null);
        p = ssn.getPredicate();
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(p.getExpressionType()));
        assertTrue(ExpressionType.VALUE_TUPLE.equals(p.getLeft().getExpressionType()));
        assertTrue(ExpressionType.VALUE_CONSTANT.equals(p.getRight().getExpressionType()));

        // R1.A = R2.A AND R2.C = 1 => R1.A = R2.A AND R2.C = 1
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A AND R2.C = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assert(p != null &&  p.getExpressionType().equals(ExpressionType.COMPARE_EQUAL));
        AbstractExpression l = p.getLeft();
        AbstractExpression r = p.getRight();
        assert(ExpressionType.VALUE_TUPLE.equals(l.getExpressionType()));
        assert(ExpressionType.VALUE_TUPLE.equals(r.getExpressionType()));

        // R1.A = R2.A AND ABS(R2.C) = 1 => R1.A = R2.A AND ABS(R2.C) = 1
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.A = R2.A AND ABS(R2.C) = 1 ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assert(p != null &&  p.getExpressionType().equals(ExpressionType.COMPARE_EQUAL));
        l = p.getLeft();
        r = p.getRight();
        assert(ExpressionType.VALUE_TUPLE.equals(l.getExpressionType()));
        assert(ExpressionType.VALUE_TUPLE.equals(r.getExpressionType()));

        // R1.A = R3.A - NLIJ
        pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.A = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);

        // R1.A = R3.A and R1.A = 4 =>  R3.A = 4 and R1.A = 4  -- NLJ/IndexScan
        pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.A = R3.A and R1.A = 4");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        // R1.A = R3.A and R3.A = 4 =>  R3.A = 4 and R1.A = 4  -- NLJ/IndexScan
        pn = compile("select * FROM R1 LEFT JOIN R3 ON R1.A = R3.A and R3.A = 4");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();


    }

    public void testFunctionJoinConditions() {
        AbstractPlanNode pn = compile("select * FROM R1 JOIN R2 ON ABS(R1.A) = ABS(R2.A) ");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        AbstractExpression p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.FUNCTION);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.FUNCTION);

        pn = compile("select * FROM R1 ,R2 WHERE ABS(R1.A) = ABS(R2.A) ");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.FUNCTION);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.FUNCTION);

        pn = compile("select * FROM R1 ,R2");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof AbstractJoinPlanNode);
        p = ((AbstractJoinPlanNode) n).getJoinPredicate();
        assertTrue(p == null);

        // USING expression can have only comma separated list of column names
        failToCompile("select * FROM R1 JOIN R2 USING (ABS(A))",
                      "user lacks privilege or object not found: ABS");
    }

    public void testIndexJoinConditions() {
        AbstractPlanNode pn = compile("select * FROM R3 WHERE R3.A = 0");
        AbstractPlanNode n = pn.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        assertTrue(((IndexScanPlanNode) n).getPredicate() == null);

        pn = compile("select * FROM R3 WHERE R3.A > 0 and R3.A < 5 and R3.C = 4");
        n = pn.getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode) n;
        AbstractExpression p = indexScan.getPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);
        p = indexScan.getEndExpression();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_LESSTHAN);
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.GT));

        pn = compile("select * FROM R3, R2 WHERE R3.A = R2.A AND R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(((NestLoopIndexPlanNode) n).getJoinPredicate() == null);
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.EQ));
        assertTrue(indexScan.getEndExpression().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        assertTrue(indexScan.getPredicate().getExpressionType() == ExpressionType.COMPARE_GREATERTHAN);
        AbstractPlanNode seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertTrue(ExpressionType.COMPARE_GREATERTHANOREQUALTO.equals(((SeqScanPlanNode)seqScan).getPredicate().getExpressionType()));

        pn = compile("select * FROM R3 JOIN R2 ON R3.A = R2.A WHERE R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(((NestLoopIndexPlanNode) n).getJoinPredicate() == null);
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.EQ));
        assertTrue(indexScan.getEndExpression().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertTrue(ExpressionType.COMPARE_GREATERTHANOREQUALTO.equals(((SeqScanPlanNode)seqScan).getPredicate().getExpressionType()));

        pn = compile("select * FROM R3 JOIN R2 USING(A) WHERE R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(((NestLoopIndexPlanNode) n).getJoinPredicate() == null);
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.EQ));
        assertTrue(indexScan.getEndExpression().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertTrue(ExpressionType.COMPARE_GREATERTHANOREQUALTO.equals(((SeqScanPlanNode)seqScan).getPredicate().getExpressionType()));

        pn = compile("select * FROM R3 JOIN R2 ON R3.A = R2.A JOIN R1 ON R2.A = R1.A WHERE R3.C > 0 and R2.C >= 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        p = ((NestLoopPlanNode) n).getJoinPredicate();
        assertTrue(p.getExpressionType() == ExpressionType.COMPARE_EQUAL);
        ExpressionType t = p.getLeft().getExpressionType();
        assertTrue(p.getLeft().getExpressionType() == ExpressionType.VALUE_TUPLE);
        assertTrue(p.getRight().getExpressionType() == ExpressionType.VALUE_TUPLE);
        seqScan = n.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        n = n.getChild(1);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        indexScan = (IndexScanPlanNode)nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.EQ));
        assertTrue(indexScan.getEndExpression().getExpressionType() == ExpressionType.COMPARE_EQUAL);
        seqScan = nlij.getChild(0);
        assertTrue(seqScan instanceof SeqScanPlanNode);
        assertTrue(ExpressionType.COMPARE_GREATERTHANOREQUALTO.equals(((SeqScanPlanNode)seqScan).getPredicate().getExpressionType()));

    }

    public void testIndexInnerJoin() {
        AbstractPlanNode pn = compile("select * FROM R3 JOIN R1 ON R1.C = R3.A");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getInlinePlanNode(PlanNodeType.INDEXSCAN) != null);
    }

   public void testMultiColumnJoin() {
       // Test multi column condition on non index columns
       AbstractPlanNode pn = compile("select A, C FROM R2 JOIN R1 USING(A, C)");
       AbstractPlanNode n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       NestLoopPlanNode nlj = (NestLoopPlanNode) n;
       AbstractExpression pred = nlj.getJoinPredicate();
       assertTrue(pred != null);
       assertTrue(pred.getExpressionType() == ExpressionType.CONJUNCTION_AND);

       pn = compile("select R1.A, R2.A FROM R2 JOIN R1 on R1.A = R2.A and R1.C = R2.C");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       nlj = (NestLoopPlanNode) n;
       pred = nlj.getJoinPredicate();
       assertTrue(pred != null);
       assertTrue(pred.getExpressionType() == ExpressionType.CONJUNCTION_AND);

      // Test multi column condition on index columns
       pn = compile("select A FROM R2 JOIN R3 USING(A)");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
       ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getLookupType().equals(IndexLookupType.GT);

       pn = compile("select R3.A, R2.A FROM R2 JOIN R3 ON R3.A = R2.A");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       nlij = (NestLoopIndexPlanNode) n;
       pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
       ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getLookupType().equals(IndexLookupType.GT);

       pn = compile("select A, C FROM R3 JOIN R2 USING(A, C)");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       nlij = (NestLoopIndexPlanNode) n;
       pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
       assertTrue(pred != null);
       assertTrue(pred.getExpressionType() == ExpressionType.COMPARE_EQUAL);

       pn = compile("select R3.A, R2.A FROM R3 JOIN R2 ON R3.A = R2.A AND R3.C = R2.C");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopIndexPlanNode);
       nlij = (NestLoopIndexPlanNode) n;
       pred = ((IndexScanPlanNode) nlij.getInlinePlanNode(PlanNodeType.INDEXSCAN)).getPredicate();
       assertTrue(pred != null);
       assertTrue(pred.getExpressionType() == ExpressionType.COMPARE_EQUAL);
       }

   public void testDistributedInnerJoin() {
       // JOIN replicated and one distributed table
       AbstractPlanNode pn = compile("select * FROM R1 JOIN P2 ON R1.C = P2.A");
       AbstractPlanNode n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof ReceivePlanNode);

       // Join multiple distributed tables on the partitioned column
       pn = compile("select * FROM P1 JOIN P2 USING(A)");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof ReceivePlanNode);

       // Two Distributed tables join on non-partitioned column
       failToCompile("select * FROM P1 JOIN P2 ON P1.C = P2.E",
                     "Join or union of multiple partitioned tables has insufficient join criteria.");
   }

    public void testBasicOuterJoin() {
        // select * with ON clause should return all columns from all tables
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType().equals(JoinType.LEFT));
        assertTrue(nl.getChildCount() == 2);
        AbstractPlanNode c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        AbstractPlanNode c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c1).getTargetTableName()));

        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C AND R1.A = 5");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType().equals(JoinType.LEFT));
        assertTrue(nl.getChildCount() == 2);
        c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c1).getTargetTableName()));
      }

    public void testRightOuterJoin() {
        // select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C => select * FROM R2 LEFT JOIN R1 ON R1.C = R2.C
        AbstractPlanNode pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType().equals(JoinType.LEFT));
        assertTrue(nl.getChildCount() == 2);
        AbstractPlanNode c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        AbstractPlanNode c1 = nl.getChild(1);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertTrue("R1".equalsIgnoreCase(((SeqScanPlanNode) c1).getTargetTableName()));

        // Same but with distributed table
        pn = compile("select * FROM P1 RIGHT JOIN R2 ON P1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType().equals(JoinType.LEFT));
        assertTrue(nl.getChildCount() == 2);
        c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue("R2".equalsIgnoreCase(((SeqScanPlanNode) c0).getTargetTableName()));
        c1 = nl.getChild(1);
        assertTrue(c1 instanceof ReceivePlanNode);

    }

    public void testSeqScanOuterJoinCondition() {
        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(nl.getJoinPredicate().getExpressionType()));
        assertTrue(nl.getWherePredicate() == null);
        assertTrue(nl.getChildCount() == 2);
        SeqScanPlanNode c0 = (SeqScanPlanNode) nl.getChild(0);
        assertTrue(c0.getPredicate() == null);
        SeqScanPlanNode c1 = (SeqScanPlanNode) nl.getChild(1);
        assertTrue(c1.getPredicate() == null);

        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Join Expr stays at the the NLJ as pre-join predicate
        // R2.A < 0 Inner Join Expr is pushed down to the inner SeqScan node
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C AND R1.A > 0 AND R2.A < 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(nl.getPreJoinPredicate() != null);
        AbstractExpression p = nl.getPreJoinPredicate();
        assertTrue(ExpressionType.COMPARE_GREATERTHAN.equals(p.getExpressionType()));
        assertTrue(nl.getJoinPredicate() != null);
        p = nl.getJoinPredicate();
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(p.getExpressionType()));
        assertTrue(nl.getWherePredicate() == null);
        assertTrue(nl.getChildCount() == 2);
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertTrue(c0.getPredicate() == null);
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertTrue(c1.getPredicate() != null);
        p = c1.getPredicate();
        assertTrue(ExpressionType.COMPARE_LESSTHAN.equals(p.getExpressionType()));

        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // (R1.A > 0 OR R2.A < 0) Inner-Outer join Expr stays at the NLJ as Join predicate
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C AND (R1.A > 0 OR R2.A < 0)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        p = nl.getJoinPredicate();
        assertTrue(ExpressionType.CONJUNCTION_AND.equals(p.getExpressionType()));
        assertTrue(ExpressionType.CONJUNCTION_OR.equals(p.getLeft().getExpressionType()));
        assertTrue(nl.getWherePredicate() == null);
        assertTrue(nl.getChildCount() == 2);
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertTrue(c0.getPredicate() == null);
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertTrue(c1.getPredicate() == null);

        // R1.C = R2.C Inner-Outer join Expr stays at the NLJ as Join predicate
        // R1.A > 0 Outer Where Expr is pushed down to the outer SeqScan node
        // R2.A IS NULL Inner Where Expr stays at the the NLJ as post join (where) predicate
        // (R1.C > R2.C OR R2.C IS NULL) Inner-Outer Where stays at the the NLJ as post join (where) predicate
        pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE R1.A > 0 AND R2.A IS NULL AND (R1.C > R2.C OR R2.C IS NULL)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nl.getJoinType());
        assertTrue(nl.getJoinPredicate() != null);
        p = nl.getJoinPredicate();
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(p.getExpressionType()));
        AbstractExpression w = nl.getWherePredicate();
        assertTrue(w != null);
        assertTrue(ExpressionType.CONJUNCTION_AND.equals(w.getExpressionType()));
        assertTrue(ExpressionType.OPERATOR_IS_NULL.equals(w.getRight().getExpressionType()));
        assertTrue(ExpressionType.CONJUNCTION_OR.equals(w.getLeft().getExpressionType()));
        assertTrue(nl.getChildCount() == 2);
        c0 = (SeqScanPlanNode) nl.getChild(0);
        assertTrue(ExpressionType.COMPARE_GREATERTHAN.equals(c0.getPredicate().getExpressionType()));
        c1 = (SeqScanPlanNode) nl.getChild(1);
        assertTrue(c1.getPredicate() == null);

        // R3.A = R2.A Inner-Outer index join Expr. NLJ predicate.
        // R3.A > 3 Index Outer where expr pushed down to IndexScanPlanNode
        // R3.C < 0 non-index Outer where expr pushed down to IndexScanPlanNode as a predicate
        pn = compile("select * FROM R3 LEFT JOIN R2 ON R3.A = R2.A WHERE R3.A > 3 AND R3.C < 0");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType() == JoinType.LEFT);
        AbstractPlanNode outerScan = n.getChild(0);
        assertTrue(outerScan instanceof IndexScanPlanNode);
        IndexScanPlanNode indexScan = (IndexScanPlanNode) outerScan;
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.GT));
        assertTrue(indexScan.getPredicate() != null);
        assertTrue(indexScan.getPredicate().getExpressionType() == ExpressionType.COMPARE_LESSTHAN);
   }

    public void testDistributedSeqScanOuterJoinCondition() {
        // Distributed Outer table
        AbstractPlanNode pn = compile("select * FROM P1 LEFT JOIN R2 ON P1.C = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertTrue(nl.getChildCount() == 2);
        assertTrue(nl.getChild(0) instanceof ReceivePlanNode);
        assertTrue(nl.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner table
        pn = compile("select * FROM R2 LEFT JOIN P1 ON P1.C = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(nl.getChildCount() == 2);
        assertTrue(nl.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(nl.getChild(1) instanceof ReceivePlanNode);

        // Distributed Inner and Outer table joined on the partition column
        List<AbstractPlanNode> lpn = compileToFragments("select * FROM P1 LEFT JOIN P4 ON P1.A = P4.A");
        assertTrue(lpn.size() == 2);
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(n.getChildCount() == 2);
        assertTrue(n.getChild(0) instanceof SeqScanPlanNode);
        assertTrue(n.getChild(1) instanceof SeqScanPlanNode);

        // Distributed Inner and Outer table joined on the non-partition column
        failToCompile("select * FROM P1 LEFT JOIN P4 ON P1.A = P4.E",
                "Join or union of multiple partitioned tables has insufficient join criteria");
    }

    public void testBasicIndexOuterJoin() {
        // R3 is indexed but it's the outer table and the join expression must stay at the NLJ
        // so index can't be used
        AbstractPlanNode pn = compile("select * FROM R3 LEFT JOIN R2 ON R3.A = R2.C");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType().equals(JoinType.LEFT));
        assertTrue(nl.getChildCount() == 2);
        AbstractPlanNode c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c0).getTargetTableName().equalsIgnoreCase("R3"));
        AbstractPlanNode c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c1).getTargetTableName().equalsIgnoreCase("R2"));

        // R3 is indexed but it's the outer table so index can't be used
        pn = compile("select * FROM R2 RIGHT JOIN R3 ON R3.A = R2.C");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        nl = (NestLoopPlanNode) n;
        assertTrue(nl.getJoinType().equals(JoinType.LEFT));
        assertTrue(nl.getChildCount() == 2);
        c0 = nl.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c0).getTargetTableName().equalsIgnoreCase("R3"));
        c1 = nl.getChild(1);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c1).getTargetTableName().equalsIgnoreCase("R2"));

        pn = compile("select * FROM R2 LEFT JOIN R3 ON R2.C = R3.A");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nli = (NestLoopIndexPlanNode) n;
        assertTrue(nli.getJoinType().equals(JoinType.LEFT));
        assertTrue(nli.getChildCount() == 1);
        c0 = nli.getChild(0);
        assertTrue(c0 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode) c0).getTargetTableName().equalsIgnoreCase("R2"));
        c1 = nli.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(c1 != null);
        assertTrue(((IndexScanPlanNode) c1).getTargetTableName().equalsIgnoreCase("R3"));
      }

    public void testIndexOuterJoinConditions() {
        // R1.C = R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.C > 0 Inner Join Expr is pushed down to the inlined IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLIJ
        AbstractPlanNode pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A AND R3.C > 0 AND R2.A < 6");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nlij = (NestLoopIndexPlanNode) n;
        assertTrue(JoinType.LEFT == nlij.getJoinType());
        assertTrue(nlij.getPreJoinPredicate() != null);
        AbstractExpression p = nlij.getPreJoinPredicate();
        assertTrue(ExpressionType.COMPARE_LESSTHAN.equals(p.getExpressionType()));
        assertTrue(nlij.getJoinPredicate() == null);
        assertTrue(nlij.getWherePredicate() == null);
        IndexScanPlanNode indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(IndexLookupType.EQ == indexScan.getLookupType());
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(indexScan.getEndExpression().getExpressionType()));
        assertTrue(ExpressionType.COMPARE_GREATERTHAN.equals(indexScan.getPredicate().getExpressionType()));
        AbstractPlanNode c1 = n.getChild(0);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode)c1).getPredicate() == null);

        // R1.C = R3.A Inner-Outer non-index join Expr. NLJ/IndexScan
        // R3.A > 0 Inner index Join Expr is pushed down to the inner IndexScan node as an index
        // R3.C != 0 Non-index Inner Join Expression is pushed down to the inner IndexScan node as a predicate
        // R2.A < 6 Outer Join Expr is a pre-join predicate for NLJ
        pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.C = R2.A AND R3.A > 0 AND R3.C != 0 AND R2.A < 6");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        NestLoopPlanNode nlj = (NestLoopPlanNode) n;
        assertTrue(JoinType.LEFT == nlj.getJoinType());
        assertTrue(nlj.getPreJoinPredicate() != null);
        p = nlj.getPreJoinPredicate();
        assertTrue(ExpressionType.COMPARE_LESSTHAN.equals(p.getExpressionType()));
        assertTrue(nlj.getJoinPredicate() != null);
        assertTrue(ExpressionType.COMPARE_EQUAL.equals(nlj.getJoinPredicate().getExpressionType()));
        assertTrue(nlj.getWherePredicate() == null);
        c1 = n.getChild(0);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertTrue(((SeqScanPlanNode)c1).getPredicate() == null);
        AbstractPlanNode c2 = n.getChild(1);
        assertTrue(c2 instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) c2;
        assertTrue(IndexLookupType.GT == indexScan.getLookupType());
        assertTrue(indexScan.getPredicate() != null);
        assertTrue(ExpressionType.COMPARE_NOTEQUAL.equals(indexScan.getPredicate().getExpressionType()));

        // R2.A = R3.A Inner-Outer index join Expr. NLIJ/Inlined IndexScan
        // R3.A IS NULL Inner where expr - part of the NLIJ where predicate
        // R2.A < 6 OR R3.C IS NULL Inner-Outer where expr - part of the NLIJ where predicate
        // R2.A > 3 Outer where expr - pushed down to the outer node
        pn = compile("select * FROM R2 LEFT JOIN R3 ON R3.A = R2.A WHERE R3.A IS NULL AND R2.A > 3 AND (R2.A < 6 OR R3.C IS NULL)");
        n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(((NestLoopIndexPlanNode) n).getJoinType() == JoinType.LEFT);
        assertTrue(((NestLoopIndexPlanNode) n).getPreJoinPredicate() == null);
        assertTrue(((NestLoopIndexPlanNode) n).getJoinPredicate() == null);
        assertTrue(((NestLoopIndexPlanNode) n).getWherePredicate() != null);
        AbstractExpression w = ((NestLoopIndexPlanNode) n).getWherePredicate();
        assertTrue(ExpressionType.CONJUNCTION_AND == w.getExpressionType());
        assertTrue(ExpressionType.OPERATOR_IS_NULL == w.getRight().getExpressionType());
        assertTrue(ExpressionType.CONJUNCTION_OR == w.getLeft().getExpressionType());
        indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(IndexLookupType.EQ == indexScan.getLookupType());
        assertTrue(ExpressionType.COMPARE_EQUAL == indexScan.getEndExpression().getExpressionType());
        c1 = n.getChild(0);
        assertTrue(c1 instanceof SeqScanPlanNode);
        assertTrue(ExpressionType.COMPARE_GREATERTHAN == ((SeqScanPlanNode)c1).getPredicate().getExpressionType());

    }

    public void testDistributedIndexJoinConditions() {
        // Distributed outer table, replicated inner -NLIJ/inlined IndexScan
        AbstractPlanNode pn = compile("select * FROM P1 LEFT JOIN R3 ON P1.C = R3.A");
        AbstractPlanNode n = pn.getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        NestLoopIndexPlanNode nl = (NestLoopIndexPlanNode) n;
        assertTrue(nl.getChildCount() == 1);
        assertTrue(nl.getChild(0) instanceof ReceivePlanNode);

        // Distributed inner  and replicated outer tables -NLJ/IndexScan
        List<AbstractPlanNode> lpn = compileToFragments("select *  FROM R3 LEFT JOIN P2 ON R3.A = P2.A AND P2.A < 0 AND P2.E > 3 WHERE P2.A IS NULL");
        assertTrue(lpn.size() == 2);
        n = lpn.get(0).getChild(0).getChild(0);
        assertTrue(n instanceof NestLoopPlanNode);
        assertTrue(JoinType.LEFT == ((NestLoopPlanNode) n).getJoinType());
        assertTrue(((NestLoopPlanNode) n).getJoinPredicate() != null);
        assertTrue(((NestLoopPlanNode) n).getWherePredicate() != null);
        AbstractPlanNode c = n.getChild(0);
        assertTrue(c instanceof SeqScanPlanNode);
        c = n.getChild(1);
        assertTrue(c instanceof ReceivePlanNode);
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof IndexScanPlanNode);
        IndexScanPlanNode in = (IndexScanPlanNode) n;
        assertTrue(in.getPredicate() != null);
        assertTrue(ExpressionType.CONJUNCTION_AND == in.getPredicate().getExpressionType());
        assertTrue(IndexLookupType.LT == in.getLookupType());

        // Distributed inner  and outer tables -NLIJ/inlined IndexScan
        lpn = compileToFragments("select *  FROM P2 RIGHT JOIN P3 ON P3.A = P2.A AND P2.A < 0 WHERE P2.A IS NULL");
        assertTrue(lpn.size() == 2);
        n = lpn.get(1).getChild(0);
        assertTrue(n instanceof NestLoopIndexPlanNode);
        assertTrue(JoinType.LEFT == ((NestLoopIndexPlanNode) n).getJoinType());
        assertTrue(((NestLoopIndexPlanNode) n).getJoinPredicate() == null);
        assertTrue(((NestLoopIndexPlanNode) n).getWherePredicate() != null);
        AbstractExpression w = ((NestLoopIndexPlanNode) n).getWherePredicate();
        assertTrue(ExpressionType.OPERATOR_IS_NULL == w.getExpressionType());
        IndexScanPlanNode indexScan = (IndexScanPlanNode)n.getInlinePlanNode(PlanNodeType.INDEXSCAN);
        assertTrue(indexScan.getLookupType().equals(IndexLookupType.EQ));
        assertTrue(ExpressionType.COMPARE_EQUAL == indexScan.getEndExpression().getExpressionType());
        w = indexScan.getPredicate();
        assertTrue(w != null);
        assertTrue(w.getExpressionType() == ExpressionType.COMPARE_LESSTHAN);
    }


   public void testNonSupportedJoin() {
       // JOIN with parentheses (HSQL limitation)
       failToCompile("select R2.C FROM (R1 JOIN R2 ON R1.C = R2.C) JOIN R3 ON R1.C = R3.C",
                     "user lacks privilege or object not found: R1.C");
       // JOIN with join hierarchy (HSQL limitation)
       failToCompile("select * FROM R1 JOIN R2 JOIN R3 ON R1.C = R2.C ON R1.C = R3.C",
                     "unexpected token");
       // FUUL JOIN. Temporary restriction
       failToCompile("select R1.C FROM R1 FULL JOIN R2 ON R1.C = R2.C",
                     "VoltDB does not support full outer joins");
       failToCompile("select R1.C FROM R1 FULL OUTER JOIN R2 ON R1.C = R2.C",
                     "VoltDB does not support full outer joins");
       // OUTER JOIN with >5 tables.
       // Temporary commented out
       //failToCompile("select R1.C FROM R3,R2, P1, P2, P3 LEFT OUTER JOIN R1 ON R1.C = R2.C WHERE R3.A = R2.A and R2.A = P1.A and P1.A = P2.A and P3.A = P2.A",
       //              "join of > 5 tables was requested without specifying a join order");
       // INNER JOIN with >5 tables.
       failToCompile("select R1.C FROM R3,R2, P1, P2, P3, R1 WHERE R3.A = R2.A and R2.A = P1.A and P1.A = P2.A and P3.A = P2.A and R1.C = R2.C",
                     "join of > 5 tables was requested without specifying a join order");
       // Self JOIN . Temporary restriction
       failToCompile("select R1.C FROM R1 LEFT OUTER JOIN R2 ON R1.C = R2.C RIGHT JOIN R2 ON R2.C = R1.C",
                     "VoltDB does not support self joins, consider using views instead");
       // OUTER JOIN with more then two tables. Temporary restriction
       failToCompile("select R1.C FROM R1 LEFT OUTER JOIN R2 ON R1.C = R2.C RIGHT JOIN R3 ON R3.C = R1.C",
                     "VoltDB does not support outer joins with more than two tables involved");
       failToCompile("select R1.C FROM R1 LEFT JOIN R2 ON R1.C = R2.C, R3 WHERE R3.C = R1.C",
                     "VoltDB does not support outer joins with more than two tables involved");
   }


   public void testOuterJoinSimplification() {
       AbstractPlanNode pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE R2.C IS NOT NULL");
       AbstractPlanNode n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE R2.C > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C WHERE R1.C > 0");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.LEFT);

       pn = compile("select * FROM R1 RIGHT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.LEFT);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10 AND R1.C = 3");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R2.C) <  10 OR R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.INNER);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10 AND R1.C > 3");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.LEFT);

       pn = compile("select * FROM R1 LEFT JOIN R2 ON R1.C = R2.C WHERE ABS(R1.C) <  10 OR R2.C IS NOT NULL");
       n = pn.getChild(0).getChild(0);
       assertTrue(n instanceof NestLoopPlanNode);
       assertTrue(((NestLoopPlanNode) n).getJoinType() == JoinType.LEFT);
   }

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestJoinOrder.class.getResource("testplans-join-ddl.sql"), "testplansjoin", false);
    }

}
