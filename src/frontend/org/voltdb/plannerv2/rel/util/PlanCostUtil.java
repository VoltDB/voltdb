/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.catalog.ColumnRef;
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
    // 1/TABLE_COLUMN_SPARSITY of all rows has distinct values
    private static final double TABLE_COLUMN_SPARSITY = 0.1;

    // Join filters
    private static final double MAX_EQ_FILTER_DISCOUNT = 0.09;
    private static final double MAX_OTHER_FILTER_DISCOUNT = 0.045;

    // If Limit ?, it's likely to be a small number. So pick up 50 here.
    private static final int DEFAULT_LIMIT_VALUE_PARAMETERIZED = 50;

    // SetOP overlap
    public static final double SET_OP_OVERLAP = 0.2;

    // "Covering cell" indexes get further special treatment below that tries to
    // properly credit their benefit even when they do not actually eliminate
    // the expensive exact contains post-filter.
    // I can't quite justify that rationally, but it "seems reasonable". --paul
    private static final double GEO_INDEX_ARTIFICIAL_TUPLE_DISCOUNT_FACTOR = 0.08;

    private static final double E_CONST = Math.exp(1);

    private PlanCostUtil() {
    }

    public static double discountJoinRowCount(double rowCount, RexNode joinCondition) {
        if (joinCondition == null) {
            return rowCount;
        }
        // Counters to count the number of equality and all other expressions
        int eqCount = 0;
        int otherCount = 0;
        double discountCountFactor = 0;
        // Discount tuple count.
        for (RexNode predicate: RelOptUtil.conjunctions(joinCondition)) {
            if (predicate.isA(SqlKind.EQUALS)) {
                discountCountFactor += Math.pow(MAX_EQ_FILTER_DISCOUNT, ++eqCount);
            } else {
                discountCountFactor += Math.pow(MAX_OTHER_FILTER_DISCOUNT, ++otherCount);
            }
        }
        return rowCount * (1.0 - discountCountFactor);
    }

    public static double discountTableScanRowCount(double rowCount, RexProgram program) {
        if (program != null && program.getCondition() != null) {
            return calculateFilterDiscount(rowCount, RelOptUtil.conjunctions(program.getCondition()).size());
        } else {
            return rowCount;
        }
    }

    public static double discountPartialIndexRowCount(double rowCount, AccessPath accessPath) {
        Preconditions.checkArgument(accessPath != null);
        return calculateFilterDiscount(rowCount, accessPath.getPartialIndexExpression().size());
    }

    private static double calculateFilterDiscount(double rowCount, int filterCount) {
        double discountFactor = 1.0;
        // Eliminated filters discount the cost of processing tuples with a rapidly
        // diminishing effect that ranges from a discount of 0.9 for one skipped filter
        // to a discount approaching 0.888... (=8/9) for many skipped filters.

        // Avoid applying the discount to an initial tie-breaker value of 2 or 3
        for (int i = 0; i < filterCount; ++i) {
            discountFactor -= Math.pow(MAX_PER_POST_FILTER_DISCOUNT, i + 1);
        }
        rowCount *= discountFactor;
        // Ensure non-zero cost
        return Math.max(1., rowCount);
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
        return rowCount * discountFactor;
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
                RelDistributions.ANY.getType().equals(
                        traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType())) {
            return rowCount * 10000;
        } else {
            return rowCount;
        }
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

    public static double computeIndexCost(Index index, AccessPath accessPath, RelCollation collation, double rowCount) {

        // HOW WE COST INDEXES
        // Since Volt only supports TREE based indexes, the CPU cost can be approximated as following
        // log(e - 1 + N * sparsity) / sparsity
        // where N is the number of rows in a table and Sparsity represents the reciprocal of number of identical rows -
        // sparsity is 0.1 if on average, 10 rows have identical values on the column(s) of the index.
        // For a UNIQUE index, sparsity is 1 (cannot be more sparse than this).
        //
        // We favor a large sparsity value, by penalizing various cases that index match is not perfect.
        //
        // The above formula favors sparser cases, UNIQUE index being an extreme in that direction. The extreme in
        // the opposite direction is sparsity == 1/N, where the cost becomes N, and index is not useful at all!
        //
        // To account for a multi-column indexes and partial coverage, Sparsity is adjusted to be
        // Adjusted sparsity *= log(Index Column Count + e) / log(|Index Column Count - Key Width| + e)
        // where the Index Column Count denotes number of columns or expressions in the index,
        // and Key Width denotes the number of Index columns or expressions covered by a given access path.
        // When matching DQL with equi-filter that matches some indexes, we want to choose the index with best match,
        // and maximum number of columns, so the query:
        // SELECT c FROM t WHERE a = 0 AND b = 0;
        // and index1(a), index2(a, b), index3(a, b, c), then the denominator of above calculation favors index1 and index2
        // equally over index3; but the numerator factor favors index2 because it contains more columns.
        //
        // Then, we further adjust the sparsity by penalizing collation mismatch, where the index with the larger number
        // of columns is not necessarily the best, e.g.
        // SELECT a, b FROM t ORDER BY a, b;
        // the cost for index2 wins, and cost of index1 and index2 should equally be dis-favored.
        // That is done with discounting factor calculated between the candidate index and query collation, if any.
        //
        // "Covering cell" indexes get a special adjustment to make them look more favorable.
        // Partial Indexes are discounted further

        // get the width of the index - number of columns or expression included in the index
        // need doubles for math
        final double colCount = CatalogUtil.getCatalogIndexSize(index);
        final double keyWidth = getSearchExpressionKeyWidth(accessPath, colCount);
        Preconditions.checkState(keyWidth <= colCount);

        double sparsity = (index.getUnique() || index.getAssumeunique()) ? 1 : TABLE_COLUMN_SPARSITY;
        // penalize partial index match, and favor longest index match
        sparsity *= Math.log(E_CONST + colCount) / Math.log(Math.abs(colCount - keyWidth) + E_CONST);
        // penalize for collation mismatch
        sparsity /= discountIndexCollation(index, collation);
        // discount partial indexes
        // The discount favors indexes that eliminate the most post filters
        // (accessPath.getEliminatedPostExpressions()).
        // The empirical const 0.7 subtracted from the number of the eliminated
        // filter ensures the advantage of a regular index with keyWidth = 0.5
        // The sum of the counts "other" and the "eliminated" expressions is
        // always a const representing the initial count of the post filters
        // for a give query
        if (!accessPath.getEliminatedPostExpressions().isEmpty()) {
            sparsity *= Math.log(E_CONST + (accessPath.getEliminatedPostExpressions().size() - 0.7)
                            / (accessPath.getOtherExprs().size() + accessPath.getEliminatedPostExpressions().size()));
        }

        double tuplesToRead = Math.log(E_CONST - 1 + rowCount * sparsity) / sparsity;

        if (index.getType() == IndexType.COVERING_CELL_INDEX.getValue()) {
            tuplesToRead *= GEO_INDEX_ARTIFICIAL_TUPLE_DISCOUNT_FACTOR;
        }

        return tuplesToRead;
    }

    /**
     * Translate sorting order to column indices. Ascending order are set to be positive, and descending order negative.
     * @param collation collation
     * @return a list of integers, whose absolute value are the column index of the collation; and sign mark whether the
     * sort order is ascending (+) or not (-).
     */
    public static List<Integer> collationIndices(RelCollation collation) {
        return collation.getFieldCollations().stream()
                .map(col -> (col.getDirection().isDescending() ? -1 : 1) * col.getFieldIndex())
                .collect(Collectors.toList());
    }

    /**
     * Get the length of longest common prefix of two lists.
     * @param lhs the first list
     * @param rhs the second list
     * @return the length of longest common prefix from two lists
     */
    public static<O> int commonPrefixLength(List<O> lhs, List<O> rhs) {
        int index = 0;
        final int len = Math.min(lhs.size(), rhs.size());
        while (index < len && lhs.get(index).equals(rhs.get(index))) {
            ++index;
        }
        return index;
    }

    public static List<Integer> indexColumns(Index index) {
        return StreamSupport.stream((index.getColumns()).spliterator(), false)
                .map(ColumnRef::getIndex).collect(Collectors.toList());
    }

    private static double discountIndexCollation(Index index, RelCollation collation) {
        // No discount if the collation is empty
        if (collation.getFieldCollations().isEmpty()) {
            return 1.;
        } else {
            return Math.log(index.getColumns().size() -
                commonPrefixLength(collationIndices(collation), indexColumns(index)) + E_CONST);
        }
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
