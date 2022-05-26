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

import org.hsqldb_voltpatches.HSQLInterface;
import org.voltdb.expressions.AbstractExpression;
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

    public void testGeoIndex() {
        AbstractPlanNode pn;
        IndexScanPlanNode indexScan;
        String jsonicIdxScan;

        pn = compile(
                "select polys.point " +
                "from polypoints polys " +
                "where contains(polys.poly, ?);");
        pn = pn.getChild(0);
        /* enable to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) pn;
        assertEquals(IndexLookupType.GEO_CONTAINS, indexScan.getLookupType());
        jsonicIdxScan = indexScan.toJSONString();
        /* enable to debug */ System.out.println("DEBUG: " + jsonicIdxScan);
        assertEquals("POLYPOINTSPOLY", indexScan.getTargetIndexName());
        // Expecting one index search key expression
        // that is a parameter (31) of type GEOGRAPHY_POINT (26).
        assertEquals(1, indexScan.getSearchKeyExpressions().size());
        assertTrue(jsonicIdxScan.contains(
                "\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":31,\"VALUE_TYPE\":26"));

        pn = compile(
                "select polys.poly, points.point " +
                "from polypoints polys, polypoints points " +
                "where contains(polys.poly, points.point);");
        pn = pn.getChild(0);
        pn = pn.getChild(0);
        /* enable to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        indexScan = ((NestLoopIndexPlanNode) pn).getInlineIndexScan();
        assertEquals(IndexLookupType.GEO_CONTAINS, indexScan.getLookupType());
        jsonicIdxScan = indexScan.toJSONString();
        assertEquals("POLYPOINTSPOLY", indexScan.getTargetIndexName());
        // Expecting one index search key expression
        // that is a TVE (32) of type GEOGRAPHY_POINT (26).
        assertEquals(1, indexScan.getSearchKeyExpressions().size());
        assertTrue(jsonicIdxScan.contains(
                "\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":32,\"VALUE_TYPE\":26"));
        pn = pn.getChild(0);
        // A non-geography index scan over a unique key for the
        // outer scan of "points" gets injected strictly for determinism.
        assertTrue(pn instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) pn;
        assertEquals(IndexLookupType.GTE, indexScan.getLookupType());

        pn = compile(
                "select polys.point " +
                "from polypoints polys " +
                "where contains(polys.poly, ?);");
        pn = pn.getChild(0);
        //* enable to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        indexScan = (IndexScanPlanNode) pn;
        assertEquals(IndexLookupType.GEO_CONTAINS, indexScan.getLookupType());
        jsonicIdxScan = indexScan.toJSONString();
        //* enable to debug */ System.out.println("DEBUG: " + jsonicIdxScan);
        assertEquals("POLYPOINTSPOLY", indexScan.getTargetIndexName());
        // Expecting one index search key expression
        // that is a parameter (31) of type GEOGRAPHY_POINT (26).
        assertEquals(1, indexScan.getSearchKeyExpressions().size());
        assertTrue(jsonicIdxScan.contains(
                "\"SEARCHKEY_EXPRESSIONS\":[{\"TYPE\":31,\"VALUE_TYPE\":26"));
    }

    public void testHeadToHeadFilters() {
        // Each pair of strings contains an indexable query filter and a pattern that
        // its index optimization's plan will contain in its "explain" output.
        // The pairs are sorted in preference order, so we can generally expect to find
        // the pattern after planning a query with the filter by itself or in combination
        // with any filter listed later. There are currently exceptions, which are
        // counted below as "surprises", which we hope will not regress (increase)
        // as we evolve the cost calculations. The current costing does some particularly
        // surprising things with the relative costing of plans for predicates that
        // generally combine use of single and/or double-ended range filters
        // with unique and/or non-unique index filters, especially for compound indexes.
        String head_to_head_filters[][] = {
                {"uniquehashable = ?", "HASHUNIQUEHASH" },
                {"component1 = ? and component2unique = ?", "COMPOUNDUNIQUE"},
                {"primarykey = ?", "its primary" },
                // These commented out entries tend to cause too many non-deterministic
                // plan choices based on the order of planning same-cost plans.
                //TODO: In some or all of these cases, we may want to think about adding
                // tie-breaker criteria to the cost calculations, BUT this COULD cause
                // backward incompatible plan choices that, in actual effect,
                // could result in accidental performance regressions for existing
                // applications that were already (empirically) tuned to rely on lucky
                // breaks rather than deterministic costing.
                // Long term, the particular "tie" between unique key filtering and primary key
                // filtering MAY OR MAY NOT be something we want to change:
                // e.g. should "primary key" be favored over a "unique key" for exact
                // equality filters, but "unique key" preferred for non-equality
                // filters? Should this preference be reversed when the unique key index
                // is a hash index which must have been defined especially for equality
                // filtering?
                //
                //{"uniquekey = ?", "POLYPOINTS_UNIQUEKEY" },
                {"contains(poly, ?)", "POINTSPOLY"},
                {"component1 = ? and component2non = ?","COMPOUNDNON" },
                {"nonuniquekey = ?", "NONUNIQUE" },
                {"component1 = ? and component2unique between ? and ?", "COMPOUNDUNIQUE" },
                //{"primarykey between ? and ?", "its primary" },
                {"uniquekey between ? and ?", "POLYPOINTS_UNIQUEKEY" },
                //{"component1 = ? and component2non between ? and ?", "" },
                {"nonuniquekey between ? and ?", "" },
                {"component1 = ? and component2unique > ?", "" },
                {"component1 = ? and component2non > ?", "COMPOUNDNON" },
                {"primarykey > ?", "" },
                {"uniquekey > ?", "UNIQUEKEY" },
                {"nonuniquekey > ?", "" },
        };
        // Some number of "surprises" are deemed acceptable at least for now.
        // The number was determined empirically as of V6.1.
        final int ACCEPTABLE_SURPRISES = 5;
        int surprises = 0;
        StringBuffer surpriseDetails = new StringBuffer();
        for (int ii = 0; ii < head_to_head_filters.length; ++ii) {
            String[] filter1 = head_to_head_filters[ii];
            for (int jj = ii+1; jj < head_to_head_filters.length; ++jj) {
                String[] filter2 = head_to_head_filters[jj];

                AbstractPlanNode pn = compile(
                        "select polys.point from polypoints polys " +
                        "where " + filter1[0] + " and " + filter2[0] + " ;");
                if (pn.toExplainPlanString().contains(filter1[1])) {
                    /* enable to debug */System.out.println();
                }
                else {
                    String detail = "The query filtered by " + filter1[0] +
                            " AND " + filter2[0] + " is not using " +
                            filter1[1] + " index.";
                    surpriseDetails.append(detail).append("\n");
                    /* enable to debug */ System.out.println("WARNING: " + ii + " vs. " + jj + " " + detail);
                    //* enable to debug */ System.out.println("DEBUG: " + ii + " vs. " + jj + " " + pn.toExplainPlanString());
                    ++surprises;
                }
            }
        }
        if (surprises != ACCEPTABLE_SURPRISES) {
            // Only report all of the surprise details when the number of surprises changes.
            System.out.println("DEBUG: total plan surprises: " + surprises + " out of "
                    + (head_to_head_filters.length * (head_to_head_filters.length-1)/2) + ".");
            System.out.println(surpriseDetails);
            if (surprises < ACCEPTABLE_SURPRISES) {
                System.out.println("DEBUG: consider further constraining the baseline number to:");
                System.out.println("        final int ACCEPTABLE_SURPRISES = " + surprises + ";");
            }
            // Only fail the test when the number of surprises goes up.
            assertTrue(surprises < ACCEPTABLE_SURPRISES);
        }

    }

    // This tests recognition of a complex expression value
    // -- an addition -- used as an indexable join key's search key value.
    // Some time ago, this would throw a casting error in the planner.
    public void testEng3850ComplexIndexablePlan() {
        AbstractPlanNode pn = compile("select id from a, t where a.id < (t.a + ?);");
        pn = pn.getChild(0);
        pn = pn.getChild(0);
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        IndexScanPlanNode indexScan =
                ((NestLoopIndexPlanNode) pn).getInlineIndexScan();
        assertEquals(IndexLookupType.LT, indexScan.getLookupType());
        assertEquals(
                HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX + "ID",
                indexScan.getTargetIndexName());
        pn = pn.getChild(0);
        assertTrue(pn instanceof SeqScanPlanNode);
        SeqScanPlanNode sspn = (SeqScanPlanNode) pn;
        //*enable to debug*/System.out.println("DEBUG: " + pn.toJSONString());
        assertEquals("T", sspn.getTargetTableName());
    }

    // This tests recognition of covering parameters to prefer a hash index that would use a
    // greater number of key components than a competing tree index.
    // Not sure how this relates to ENG-931?
    public void testEng931Plan() {
        AbstractPlanNode pn = compile("select a from t where a = ? and b = ? and c = ? and d = ? " +
                                      "and e >= ? and e <= ?;");
        pn = pn.getChild(0);
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode) pn;
        assertEquals("IDX_1_HASH", ispn.getTargetIndexName());
    }

    // This tests recognition of prefix parameters and constants to prefer an index that
    // would use a greater number of key components AND would give the desired ordering.
    public void testEng2541Plan() {
        AbstractPlanNode pn = compile("select * from l where lname=? and b=0 order by id asc limit ?;");
        pn = pn.getChild(0);
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode) pn;
        assertEquals("IDX_B", ispn.getTargetIndexName());
    }

    // This tests recognition of a prefix parameter and upper bound to prefer an index that would
    // use a greater number of key components even though another index would give the desired ordering.
    public void testEng4792PlanWithCompoundEQLTEOrderedByPK() {
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
        IndexScanPlanNode ispn = (IndexScanPlanNode) pn;
        assertEquals("DELETED_SINCE_IDX", ispn.getTargetIndexName());
    }

    public void testFixedPlanWithExpressionIndexAndAlias() {
        AbstractPlanNode pn;
        IndexScanPlanNode ispn;
        String leftIndexName;

        pn = compile("select * from l aliased where b = ? and DECODE(a, null, 0, a) = 0 and id = ?;");
        // System.out.println("DEBUG: " + pn.toExplainPlanString());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        ispn = (IndexScanPlanNode) pn;
        assertEquals("DECODE_IDX3", ispn.getTargetIndexName());
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile("select * from l aliased, l where l.b = ? and DECODE(l.a, null, 0, l.a) = 0 and l.id = ? and l.lname = aliased.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 3);

        pn = compile("select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //* to debug */ System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes.
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        if (PlanNodeType.PROJECTION == pn.getPlanNodeType()) {
            pn = pn.getChild(0);
        }
        assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getPlanNodeType());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        ispn = (IndexScanPlanNode) pn;
        assertEquals("DECODE_IDX3", ispn.getTargetIndexName());
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile("select * from l x, l where x.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        // Skip the Send, Projection, and NestLoop plan nodes
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        if (PlanNodeType.PROJECTION == pn.getPlanNodeType()) {
            pn = pn.getChild(0);
        }
        assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getPlanNodeType());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        ispn = (IndexScanPlanNode) pn;
        assertEquals("DECODE_IDX3", ispn.getTargetIndexName());
        assertEquals(3, ispn.getSearchKeyExpressions().size());

        pn = compile("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        leftIndexName = HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX + "PK_LOG";
        checkDualIndexedJoin(pn, leftIndexName, "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where x.b = ? and DECODE(l.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, x.a) = 0 and l.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and l.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where x.b = ? and DECODE(l.a, null, 0, x.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        checkDualIndexedJoin(pn, "IDX_A", "DECODE_IDX3", 1);

        pn = compile("select * from l x, l where l.b = ? and DECODE(x.a, null, 0, l.a) = 0 and x.id = ? and l.lname = x.lname;");
        //*enable to debug*/System.out.println("DEBUG: " + pn.toExplainPlanString());
        leftIndexName = HSQLInterface.AUTO_GEN_NAMED_CONSTRAINT_IDX + "PK_LOG";
        checkDualIndexedJoin(pn, leftIndexName, "DECODE_IDX3", 1);
    }

    public void testCaseWhenIndex() {
        AbstractPlanNode pn;

        pn = compile("select * from l where CASE WHEN a > b THEN a ELSE b END > 8;");
        pn = pn.getChild(0);
        //*enable to debug*/System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("CASEWHEN_IDX1"));

        pn = compile("select * from l WHERE CASE WHEN a < 10 THEN a*5 ELSE a + 5 END > 2");
        pn = pn.getChild(0);
        //*enable to debug*/System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("CASEWHEN_IDX2"));

        // Negative case
        pn = compile("select * from l WHERE CASE WHEN a < 10 THEN a*2 ELSE a + 5 END > 2");
        pn = pn.getChild(0);
        /*enable to debug*/System.out.println(pn.toExplainPlanString());
        assertTrue(pn.toExplainPlanString().contains("using its primary key index (for deterministic order only)"));
    }

    public void testPartialIndexNULLPredicate() {
        AbstractPlanNode pn;

        pn = compile("select * from c where a > 0;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");

        // CREATE INDEX partial_idx_null_e ON c (a) where e is null;
        pn = compile("select * from c where a > 0 and e is NULL;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_NULL_E");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
        pn = compile("select * from c where a > 0 and e is not NULL;");
        checkScanUsesIndex(pn, "A_PARTIAL_IDX_NOT_NULL_E");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
        // range-scan covering from (A > 0) to end Z_FULL_IDX_A has higher cost
        pn = compile("select * from c where a > 0 and e = 0;");
        checkScanUsesIndex(pn, "A_PARTIAL_IDX_NOT_NULL_E");
        checkIndexPredicateContains(pn, "E");

        // CREATE INDEX a_partial_idx_not_null_d_e ON c (a+b) where (d + e) is not null;
        pn = compile("select * from c where a + b > 0 and 0 = abs(e + d);");
        checkScanUsesIndex(pn, "A_PARTIAL_IDX_NOT_NULL_D_E");
        checkIndexPredicateContains(pn, "E", "D");

        // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
        pn = compile("select * from c where a > 0 and 0 = abs(e + b);");
        checkScanUsesIndex(pn, "A_PARTIAL_IDX_NOT_NULL_E");
        checkIndexPredicateContains(pn, "E", "B");

        // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
        // uniquely match (A = 0) Z_FULL_IDX_A has the lowest cost
        pn = compile("select * from c where a = 0 and e = 0;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");
        checkIndexPredicateContains(pn, "E");

    }

    public void testPartialIndexArbitraryPredicate() {
        AbstractPlanNode pn;

        // CREATE INDEX partial_idx_or_expr ON c (f) where e > 0 or d < 5;
        pn = compile("select * from c where f > 0 and (e > 0 or d < 5);");
        checkScanUsesIndex(pn, "PARTIAL_IDX_OR_EXPR");
        checkIndexPredicateIsNull(pn);

        // ENG-15719
        // CREATE INDEX partial_idx_8 ON c (b) WHERE abs(a) > 0;
        pn = compile("SELECT COUNT(b) FROM c WHERE abs(a) > 0;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_8");
        checkIndexPredicateContains(pn, "A");
    }

    public void testPartialIndexComparisonPredicateExactMatch() {
        AbstractPlanNode pn;

        // CREATE INDEX partial_idx_or_expr ON c (a) where e > 0 or d < 5; -- expression trees differ
        pn = compile("select * from c where a > 0 and e > 0 or d < 5;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");

        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1;
        pn = compile("select * from c where abs(b) = 1 and abs(e) > 1;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_1");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1;
        pn = compile("select * from c where abs(b) > 1 and 1 < abs(e);");
        checkScanUsesIndex(pn, "PARTIAL_IDX_1");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5;
        // CREATE INDEX partial_idx_3 ON c (b) where d > 0; is also a match
        // but has higher cost because of the extra post filter
        pn = compile("select * from c where b > 0 and d > 0 and d < 5;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_2");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX partial_idx_2 ON c (b) where d > 0 and d < 5;
        pn = compile("select * from c where b > 0 and d < 5 and 0 < d;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_2");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f;
        pn = compile("select * from c where a > 0 and b > 0 and f > 0;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_4");
        checkIndexPredicateDoesNotHave(pn, "F");

        // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f;
        pn = compile("select * from c where a > 0 and b > 0 and 0 < f;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_4");
        String[] columns = {"F"};
        checkIndexPredicateDoesNotHave(pn, columns);

        // CREATE INDEX partial_idx_5 ON c (b) where d > f;
        pn = compile("select * from c where b > 0 and d > f;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_5");
        checkIndexPredicateIsNull(pn);

        // CREATE INDEX partial_idx_5 ON c (b) where d > f;
        pn = compile("select * from c where b > 0 and f < d;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_5");
        checkIndexPredicateIsNull(pn);
    }

    public void testPartialIndexComparisonPredicateNonExactMatch() {
        AbstractPlanNode pn;

        // At the moment an index filter must exactly match a
        // query filter expression (or sub-expression) to be selected

        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; Not exact match
        pn = compile("select * from c where abs(b) > 1 and 2 <  abs(e);");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");

        // CREATE INDEX partial_idx_3 ON c (b) where d > 0; Not exact match
        pn = compile("select * from c where b > 0 and d > 3;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");

        // CREATE INDEX partial_idx_5 ON c (b) where d > f;  Not exact match
        pn = compile("select * from c where b > 0 and f + 1 < d;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");
    }

    public void testPartialIndexPredicateOnly() {
        AbstractPlanNode pn;

        // Partial index can be used solely to eliminate a post-filter
        // even when the indexed columns are irrelevant

        // CREATE INDEX partial_idx_3 ON c (b)  where d > 0;
        pn = compile("select * from c where d > 0");
        checkScanUsesIndex(pn, "PARTIAL_IDX_3");

        // CREATE UNIQUE INDEX z_full_idx_a ON c (a); takes precedence over the partial_idx_3
        // because indexed column (A) is part of the WHERE expressions
        pn = compile("select * from c where d > 0 and a < 0");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");

        // CREATE INDEX partial_idx_3 ON c (b)  where d > 0;
        pn = compile("select c.d from a join c on a.id = c.e and d > 0");
        pn = pn.getChild(0).getChild(0);
        assertEquals(PlanNodeType.NESTLOOPINDEX, pn.getPlanNodeType());
        checkScanUsesIndex(pn, "PARTIAL_IDX_3");
    }

    public void testParameterizedQueryPartialIndex() {
        AbstractPlanNode pn;

        // CREATE INDEX a_partial_idx_not_null_e ON c (a) where e is not null;
        // range-scan covering from (A > 0) to end Z_FULL_IDX_A has higher cost
        pn = compile("select * from c where a > 0 and e = ?;");
        checkScanUsesIndex(pn, "A_PARTIAL_IDX_NOT_NULL_E");
        checkIndexPredicateContains(pn, "E");

        // CREATE INDEX partial_idx_4 ON c (a, b) where 0 < f; - not selected because of the parameter
        pn = compile("select * from c where a > 0 and b > 0 and ? < f;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");

        // CREATE INDEX partial_idx_1 ON c (abs(b)) where abs(e) > 1; not selected because of the parameter
        pn = compile("select * from c where abs(b) = 1 and abs(e) > ?;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");
    }

    public void testSkipNullPartialIndex() {
        AbstractPlanNode pn;

        //CREATE INDEX partial_idx_7 ON c (g) where g is not null;
        // skipNull predicate is redundant and eliminated
        pn = compile("select count(*) from c where g > 0;");
        checkCountUsesIndex(pn, "PARTIAL_IDX_7");
        checkIndexSkipNullPredicateIsNull(pn, false);

        //CREATE INDEX partial_idx_7 ON c (g) where g is not null;
        // skipNull predicate is redundant and eliminated
        pn = compile("select e from c where g > 0;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_7");
        checkIndexSkipNullPredicateIsNull(pn, false);

        //CREATE INDEX partial_idx_6 ON c (g) where g < 0;
        // skipNull predicate is redundant and eliminated
        pn = compile("select count(*) from c where g < 0;");
        checkCountUsesIndex(pn, "PARTIAL_IDX_6");
        checkIndexSkipNullPredicateIsNull(pn, false);

        //CREATE INDEX partial_idx_6 ON c (g) where g < 0;
        // skipNull predicate is redundant and eliminated
        pn = compile("select g from c where g < 0;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_6");
        checkIndexSkipNullPredicateIsNull(pn, false);

        // CREATE UNIQUE INDEX z_full_idx_a ON c (a);
        // skipNull is required - full index
        pn = compile("select count(*) from c where a > 0;");
        checkCountUsesIndex(pn, "Z_FULL_IDX_A");
        checkIndexSkipNullPredicateIsNull(pn, true);

        // CREATE UNIQUE INDEX z_full_idx_a ON c (a);
        // skipNull is required - full index
        pn = compile("select e from c where a > 0;");
        checkScanUsesIndex(pn, "Z_FULL_IDX_A");
        checkIndexSkipNullPredicateIsNull(pn, true);

        // CREATE INDEX partial_idx_3 ON c (b) where d > 0;
        // skipNull is required - index predicate is not NULL-rejecting for column B
        pn = compile("select count(*) from c where b > 0 and d > 0;");
        checkCountUsesIndex(pn, "PARTIAL_IDX_3");
        checkIndexSkipNullPredicateIsNull(pn, true);

        // CREATE INDEX partial_idx_3 ON c (b) where d > 0;
        // skipNull is required - index predicate is not NULL-rejecting for column B
        pn = compile("select b from c where b > 0 and d > 0;");
        checkScanUsesIndex(pn, "PARTIAL_IDX_3");
        checkIndexSkipNullPredicateIsNull(pn, true);
    }

    public void testENG15616PenalizeGeoIndex() {
        AbstractPlanNode pn;

        pn = compile("SELECT R.VCHAR_INLINE_MAX FROM R WHERE NOT R.TINY = R.TINY;");
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        IndexScanPlanNode ispn = (IndexScanPlanNode) pn;
        assertFalse(ispn.getTargetIndexName().equalsIgnoreCase("IDX"));
    }

    private void checkDualIndexedJoin(AbstractPlanNode pn,
            String leftIndexName, String rightIndexName, int nJoinKeys) {
        IndexScanPlanNode ispn;
        // Skip the Send and Projection plan nodes.
        assertEquals(PlanNodeType.SEND, pn.getPlanNodeType());
        pn = pn.getChild(0);
        if (PlanNodeType.PROJECTION == pn.getPlanNodeType()) {
            pn = pn.getChild(0);
        }
        assertTrue(pn instanceof NestLoopIndexPlanNode);
        ispn = ((NestLoopIndexPlanNode) pn).getInlineIndexScan();
        assertEquals(leftIndexName, ispn.getTargetIndexName());
        pn = pn.getChild(0);
        assertTrue(pn instanceof IndexScanPlanNode);
        ispn = (IndexScanPlanNode) pn;
        assertEquals(rightIndexName, ispn.getTargetIndexName());
        assertEquals(nJoinKeys, ispn.getSearchKeyExpressions().size());
    }

    private void checkScanUsesIndex(AbstractPlanNode pn,
            String targetIndexName) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ispn = (IndexScanPlanNode) pn;
        assertEquals(targetIndexName, ispn.getTargetIndexName());
    }

    private void checkCountUsesIndex(AbstractPlanNode pn,
            String targetIndexName) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXCOUNT, pn.getPlanNodeType());
        IndexCountPlanNode icpn = (IndexCountPlanNode) pn;
        assertTrue(icpn.hasTargetIndexName(targetIndexName));
    }

    private void checkIndexPredicateIsNull(AbstractPlanNode pn) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
        AbstractExpression pred = ipn.getPredicate();
        assertNull(pred);
    }

    private void checkIndexPredicateContains(AbstractPlanNode pn,
            String... columns) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
        AbstractExpression pred = ipn.getPredicate();
        assertNotNull(pred);
        List<TupleValueExpression> tves = pred.findAllTupleValueSubexpressions();
        for (TupleValueExpression tve : tves) {
            boolean match = false;
            for (String column: columns) {
                if (tve.getColumnName().equals(column)) {
                    match = true;
                    break;
                }
            }
            assertTrue(match);
        }
    }

    private void checkIndexPredicateDoesNotHave(AbstractPlanNode pn,
            String... columns) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        assertEquals(PlanNodeType.INDEXSCAN, pn.getPlanNodeType());
        IndexScanPlanNode ipn = (IndexScanPlanNode) pn;
        AbstractExpression pred = ipn.getPredicate();
        assertNotNull(pred);
        List<TupleValueExpression> tves = pred.findAllTupleValueSubexpressions();
        for (TupleValueExpression tve : tves) {
            for (String column: columns) {
                assertFalse(tve.getColumnName().equals(column));
            }
        }
    }

    private void checkIndexSkipNullPredicateIsNull(AbstractPlanNode pn,
            boolean hasSkipNullPredicate) {
        assertEquals(1, pn.getChildCount());
        pn = pn.getChild(0);
        if (pn instanceof IndexCountPlanNode) {
            assertEquals(hasSkipNullPredicate,
                    ((IndexCountPlanNode) pn).hasSkipNullPredicate());
        }
        else {
            // index scan
            AbstractExpression skipNull =
                    ((IndexScanPlanNode) pn).getSkipNullPredicate();
            assertEquals(hasSkipNullPredicate, skipNull != null);
        }
    }
}
