/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannerv2.rel.util;

import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.catalog.Index;
import org.voltdb.planner.AccessPath;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Helper utilities to determine Plan Cost.
 */
public final class PlanCostUtil {
    // Scan filters
    private static final double MAX_PER_POST_FILTER_DISCOUNT = 0.1;
    private static final double MAX_PER_COLLATION_DISCOUNT = 0.1;

    // A factor to denote an average column value repetition -
    // 1/TABLE_COLUMN_SPARCITY of all rows has distinct values
    private static final int TABLE_COLUMN_SPARCITY = 10;

    // Join filters
    private static final double MAX_EQ_FILTER_DISCOUNT = 0.09;
    private static final double MAX_OTHER_FILTER_DISCOUNT = 0.045;

    // If Limit ?, it's likely to be a small number. So pick up 50 here.
    private static final int DEFAULT_LIMIT_VALUE_PARAMETERIZED = 50;

    // SetOP overlap
    public static final double SET_OP_OVERLAP = 0.2;

    private PlanCostUtil() {
    }

    public static double discountJoinRowCount(double rowCount, RexNode joinCondition) {
        if (joinCondition == null) {
            return rowCount;
        }
        List<RexNode> predicates = RelOptUtil.conjunctions(joinCondition);
        // Counters to count the number of equality and all other expressions
        int eqCount = 0;
        int otherCount = 0;
        double discountCountFactor = 1.0;
        // Discount tuple count.
        for (RexNode predicate: predicates) {
            if (predicate.isA(SqlKind.EQUALS)) {
                discountCountFactor -= Math.pow(MAX_EQ_FILTER_DISCOUNT, ++eqCount);
            } else {
                discountCountFactor -= Math.pow(MAX_OTHER_FILTER_DISCOUNT, ++otherCount);
            }
        }
        return rowCount * discountCountFactor;
    }

    public static double discountTableScanRowCount(double rowCount, RexProgram program) {
        if (program != null && program.getCondition() != null) {
            double discountFactor = 1.0;
            // Eliminated filters discount the cost of processing tuples with a rapidly
            // diminishing effect that ranges from a discount of 0.9 for one skipped filter
            // to a discount approaching 0.888... (=8/9) for many skipped filters.

            // Avoid applying the discount to an initial tie-breaker value of 2 or 3
            int condSize = RelOptUtil.conjunctions(program.getCondition()).size();
            for (int i = 0; i < condSize; ++i) {
                discountFactor -= Math.pow(MAX_PER_POST_FILTER_DISCOUNT, i + 1);
            }
            if (discountFactor < 1.0) {
                rowCount *= discountFactor;
                if (rowCount < 4) {
                    rowCount = 4;
                }
            }
        }
        if (Double.compare(0., rowCount) == 0) {
            // Ensure non-zero cost
            rowCount += 1.;
        }
        return rowCount;
    }

    public static double discountSerialAggregateRowCount(double rowCount, int groupCount) {
        // Give a discount to the Aggregate based on the number of the collation fields.
        //  - Serial Aggregate - the collation size is equal to the number of the GROUP BY columns
        //          and max discount 1 - 0.1 -  0.01 - 0.001 - ...
        //  - Partial Aggregate - anything in between
        // The required order will be enforced by some index which collation would match / satisfy
        // the aggregate's collation. If a table has more than one index multiple Aggregate / IndexScan
        // combinations are possible and we want to pick the one that has the maximum GROUP BY columns
        // covered resulting in a more efficient aggregation (less hashing)
        double discountFactor = 1.0;

        for (int i = 0; i < groupCount; ++i) {
            discountFactor -= Math.pow(MAX_PER_COLLATION_DISCOUNT, i + 1);
        }
        rowCount *= discountFactor;
        return rowCount;
    }

    /**
     * Discourage Calcite from picking a plan with a Calc that have a RelDistributions.ANY
     * distribution trait.
     *
     * @param rowCount
     * @param traitSet
     * @return new rowCount
     */
    public static double adjustRowCountOnRelDistribution(double rowCount, RelTraitSet traitSet) {
        if (traitSet.getTrait(RelDistributionTraitDef.INSTANCE) != null &&
                RelDistributions.ANY.getType().equals(traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType())) {
            rowCount *= 10000;
        }
        return rowCount;
    }

    /**
     * Adjust row count in a presence of a possible limit and / or offset
     *
     * @param rowCount
     * @param offsetNode
     * @param limitNode
     * @return
     */
    public static double discountLimitOffsetRowCount(double rowCount, RexNode offsetNode, RexNode limitNode) {
        double limit = 0f;
        // limit and offset can be question marks
        if (limitNode != null) {
            limit = (limitNode.getKind() != SqlKind.DYNAMIC_PARAM) ?
                    RexLiteral.intValue(limitNode) : DEFAULT_LIMIT_VALUE_PARAMETERIZED;
        }
        if (offsetNode != null) {
            limit += (offsetNode.getKind() != SqlKind.DYNAMIC_PARAM) ?
                    RexLiteral.intValue(offsetNode) : DEFAULT_LIMIT_VALUE_PARAMETERIZED;
        }
        if (limit == 0f || rowCount < limit) {
            limit = rowCount;
        }
        return limit;
    }

    public static double computeIndexCost(Index index, AccessPath accessPath, double rowCount, RelMetadataQuery mq) {

        // HOW WE COST INDEXES
        // unique, covering index always wins
        // otherwise, pick the index with the most columns covered
        // otherwise, count non-equality scans as -0.5 coverage
        // prefer hash index to tree, all else being equal
        // prefer partial index, all else being equal

        // get the width of the index - number of columns or expression included in the index
        // need doubles for math
        final double colCount = CatalogUtil.getCatalogIndexSize(index);
        final double keyWidth = getSearchExpressionKeyWidth(accessPath, colCount);
        Preconditions.checkState(keyWidth <= colCount);

        // Estimate the cost of the scan (AND each projection and sort thereafter).
        // This "tuplesToRead" is not strictly speaking an expected count of tuples.
        // It's a vague measure of the cost of the scan whose accuracy depends a lot
        // on what kind of post-filtering needs to happen.
        // The tuplesRead value is also used here to estimate the number of RESULT rows.
        // This value is estimated without regard to any post-filtering effect there might be
        // -- as if all rows found in the index passed any additional post-filter conditions.
        // This ignoring of post-filter effects is at least consistent with the SeqScanPlanNode.
        // In effect, it gives index scans an "unfair" advantage
        // -- follow-on sorts (etc.) are costed lower as if they are operating on fewer rows
        // than would have come out of the seqscan, though that's nonsense.
        // It's just an artifact of how SeqScanPlanNode costing ignores ALL filters but
        // IndexScanPlanNode costing only ignores post-filters.
        // In any case, it's important to keep this code roughly in synch with any changes to
        // SeqScanPlanNode's costing to make sure that SeqScanPlanNode never gains an unfair advantage.
        double tuplesToRead = 0.;

        // Assign minor priorities for different index types (tiebreakers).
        if (index.getType() == IndexType.HASH_TABLE.getValue()) {
            tuplesToRead = 2.;
        } else if ((index.getType() == IndexType.BALANCED_TREE.getValue()) ||
                (index.getType() == IndexType.BTREE.getValue())) {
            tuplesToRead = 3.;
        } else if (index.getType() == IndexType.COVERING_CELL_INDEX.getValue()) {
            // "Covering cell" indexes get further special treatment below that tries to
            // properly credit their benefit even when they do not actually eliminate
            // the expensive exact contains post-filter.
            tuplesToRead = 3.;
        }
        Preconditions.checkState(tuplesToRead > 0);

        // special case a unique match for the output count
        if (index.getUnique() && (colCount == keyWidth)) {
            tuplesToRead = 1.;
        } else {
            // If not a unique, covering index, then it is a TREE based index with a complexity of
            // log((N / Sparsity)**(keywidth - colCount + 1))  + Sparsity**(keywidth - colCount +1)  - 1
            tuplesToRead = (keyWidth - colCount + 1) * Math.log(rowCount / TABLE_COLUMN_SPARCITY) +
                    Math.pow(TABLE_COLUMN_SPARCITY, keyWidth - colCount + 1) - 1;

            // "Covering cell" indexes get a special adjustment to make them look more favorable
            // than non-unique range filters in particular.
            // I can't quite justify that rationally, but it "seems reasonable". --paul
            if (index.getType() == IndexType.COVERING_CELL_INDEX.getValue()) {
                final double GEO_INDEX_ARTIFICIAL_TUPLE_DISCOUNT_FACTOR = 0.08;
                tuplesToRead *= GEO_INDEX_ARTIFICIAL_TUPLE_DISCOUNT_FACTOR;
            }

            // With all this discounting, make sure that any non-"covering unique" index scan costs more
            // than any "covering unique" one, no matter how many indexed column filters get piled on.
            // It's theoretically possible to be wrong here -- that a not-strictly-unique combination of
            // indexed column filters statistically selects fewer (fractional) rows per scan
            // than a unique index, but we favor the unique index anyway because:
            // -- the "unique" declaration guarantees a worse-case upper limit of 1 row per scan.
            // -- the per-indexed-column selectivity factors used above are highly fictionalized
            //    -- actual cardinality for individual components of compound indexes MIGHT be very low,
            //       making them much less selective than estimated.
            if (tuplesToRead < 4) {
                tuplesToRead = 4; // i.e. costing 1 unit more than a covered unique btree.
            }
        }

        // @TODO Need to discount dCpu for a partial index
        // Apply discounts similar to the keyWidth one for the additional post-filters that get
        // eliminated by exactly matched partial index filters. The existing discounts are not
        // supposed to give a "full refund" of the optimized-out post filters, because there is
        // an offsetting cost to using the index, typically order log(n). That offset cost will
        // be lower (order log(smaller n)) for partial indexes, but it's not clear what the typical
        // relative costs are of a partial index with x key components and y partial index predicates
        // vs. a full or partial index with x+n key components and y-m partial index predicates.
        //

        return tuplesToRead;
    }

    private static double getSearchExpressionKeyWidth(AccessPath accessPath, final double colCount) {
        double keyWidth = accessPath.getIndexExpressions().size();
        Preconditions.checkState(keyWidth <= colCount);
        // count a range scan as a half covered column
        if (keyWidth > 0.0 &&
                accessPath.getIndexLookupType() != IndexLookupType.EQ &&
                accessPath.getIndexLookupType() != IndexLookupType.GEO_CONTAINS) {
            keyWidth -= 0.5;
        } else if (keyWidth == 0.0 && !accessPath.getIndexExpressions().isEmpty()) {
            // When there is no start key, count an end-key as a single-column range scan key.

            // TODO: ( (double) ExpressionUtil.uncombineAny(m_endExpression).size() ) - 0.5
            // might give a result that is more in line with multi-component start-key-only scans.
            keyWidth = 0.5;
        }
        return keyWidth;
    }

    /**
     * SetOps CPU cost is a total of individual row counts
     * @param setOpChildren
     * @param mq
     * @return
     */
    public static double computeSetOpCost(List<RelNode> setOpChildren, RelMetadataQuery mq) {
        return setOpChildren.stream().map(child -> child.estimateRowCount(mq))
                .reduce(1., (accumCount, childCount) -> accumCount + childCount);
    }
}
