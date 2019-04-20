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

package org.voltdb.plannerv2.rel.physical;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.catalog.Index;
import org.voltdb.planner.AccessPath;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogUtil;

public class VoltPhysicalTableIndexScan extends VoltPhysicalTableScan {

    private final Index m_index;
    private final AccessPath m_accessPath;
    private final RelCollation m_indexCollation;

    /**
     *
     * @param cluster
     * @param traitSet
     * @param table
     * @param voltDBTable
     * @param program
     * @param index
     * @param accessPath
     * @param offset
     * @param limit
     * @param aggregate
     * @param preAggregateRowType
     * @param preAggregateProgram
     */
    public VoltPhysicalTableIndexScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, VoltTable voltDBTable, RexProgram program,
            Index index, AccessPath accessPath, RexNode offset, RexNode limit, RelNode aggregate,
            RelDataType preAggregateRowType, RexProgram preAggregateProgram, int splitCount,
            RelCollation indexCollation) {
        super(cluster, traitSet, table, voltDBTable, updateProgram(program, accessPath),
              offset, limit, aggregate, preAggregateRowType, preAggregateProgram, splitCount);
        Preconditions.checkNotNull(index, "index is null");
        Preconditions.checkNotNull(accessPath, "access path is null");
        Preconditions.checkNotNull(indexCollation, "index collation is null");
        m_index = index;
        m_accessPath = accessPath;
        m_indexCollation = indexCollation;
    }

    private String explain() {
        // Need to differentiate between the same index chosen for ORDER_BY or search purposes
        return m_index.getTypeName() + "_" +
                m_accessPath.getSortDirection() +
                m_accessPath.getIndexLookupType() +
                m_accessPath.getIndexExpressions().size() + "_" +
                m_accessPath.getEndExpressions().size();
    }

    /**
     * The digest needs to be updated because Calcite considers any two nodes with the same digest
     * to be identical.
     */
    @Override
    protected String computeDigest() {
        return super.computeDigest() + "_index_" + explain();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("index", explain());
        return pw;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final IndexScanPlanNode ispn = new IndexScanPlanNode(new StmtTargetTableScan(
                getVoltTable().getCatalogTable(), getVoltTable().getCatalogTable().getTypeName(), 0),
                m_index);
        // Set limit/offset
        if (m_aggregate == null) {
            // Limit / Offset will be inlined with aggregate node
            addLimitOffset(ispn);
        }
        // Set projection
        addProjection(ispn);
        // No need to set Index's predicate from its program condition.
        // It will be set from accessPath.OTHER expressions instead
        // addPredicate(ispn);

        // Set aggregate
        addAggregate(ispn);

        // At the moment this will override the predicate set by the addPredicate call
        return IndexUtil.buildIndexAccessPlanForTable(ispn, m_accessPath);
    }

    public Index getIndex() {
        return m_index;
    }

    public AccessPath getAccessPath() {
        return m_accessPath;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;

        // @TODO Need to discount dCpu for a partial index
        // Apply discounts similar to the keyWidth one for the additional post-filters that get
        // eliminated by exactly matched partial index filters. The existing discounts are not
        // supposed to give a "full refund" of the optimized-out post filters, because there is
        // an offsetting cost to using the index, typically order log(n). That offset cost will
        // be lower (order log(smaller n)) for partial indexes, but it's not clear what the typical
        // relative costs are of a partial index with x key components and y partial index predicates
        // vs. a full or partial index with x+n key components and y-m partial index predicates.
        //
        double discountFactor = 1.0;
        // Eliminated filters discount the cost of processing tuples with a rapidly
        // diminishing effect that ranges from a discount of 0.9 for one skipped filter
        // to a discount approaching 0.888... (=8/9) for many skipped filters.
        final double MAX_PER_POST_FILTER_DISCOUNT = 0.1;
        // Avoid applying the discount to an initial tie-breaker value of 2 or 3
        if (!m_accessPath.getEliminatedPostExpressions().isEmpty() && dCpu > 3) {
            for (int i = 0; i < m_accessPath.getEliminatedPostExpressions().size(); ++i) {
                discountFactor -= Math.pow(MAX_PER_POST_FILTER_DISCOUNT, i + 1);
            }
        }
        if (discountFactor < 1.0) {
            // @TODO Should it be dCpu instead?
            dRows *= discountFactor;
            if (dRows < 4) {
                dRows = 4;
            }
        }
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        // HOW WE COST INDEXES
        // unique, covering index always wins
        // otherwise, pick the index with the most columns covered
        // otherwise, count non-equality scans as -0.5 coverage
        // prefer hash index to tree, all else being equal
        // prefer partial index, all else being equal

        // FYI: Index scores should range between 2 and 800003 (I think)

        // get the width of the index - number of columns or expression included in the index
        // need doubles for math
        final double colCount = CatalogUtil.getCatalogIndexSize(m_index);
        final double keyWidth = getSearchExpressionKeyWidth(colCount);

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
        int tuplesToRead = 0;

        // Assign minor priorities for different index types (tiebreakers).
        if (m_index.getType() == IndexType.HASH_TABLE.getValue()) {
            tuplesToRead = 2;
        } else if ((m_index.getType() == IndexType.BALANCED_TREE.getValue()) ||
                (m_index.getType() == IndexType.BTREE.getValue())) {
            tuplesToRead = 3;
        } else if (m_index.getType() == IndexType.COVERING_CELL_INDEX.getValue()) {
            // "Covering cell" indexes get further special treatment below that tries to
            // properly credit their benefit even when they do not actually eliminate
            // the expensive exact contains post-filter.
            tuplesToRead = 3;
        }
        Preconditions.checkState(tuplesToRead > 0);

        // special case a unique match for the output count
        if (m_index.getUnique() && (colCount == keyWidth)) {
            tuplesToRead = 1;
        } else {
            // If not a unique, covering index, favor (discount)
            // the choice with the most columns pre-filtered by the index.
            // Cost starts at 90% of a comparable seqscan AND
            // gets scaled down by an additional factor of 0.1 for each fully covered indexed column.
            // One intentional benchmark is for a single range-covered
            // (i.e. half-covered, keyWidth == 0.5) column to have less than 1/3 the cost of a
            // "for ordering purposes only" index scan (keyWidth == 0).
            // This is to completely compensate for the up to 3X final cost resulting from
            // the "order by" and non-inlined "projection" nodes that must be added later to the
            // inconveniently ordered scan result.
            // Using a factor of 0.1 per FULLY covered (equality-filtered) column,
            // the effective scale factor for a single PARTIALLY covered (range-filtered) column
            // comes to SQRT(0.1) which is just under 32% FTW!
            tuplesToRead += (int) (VoltPhysicalTableScan.MAX_TABLE_ROW_COUNT * 0.90 * Math.pow(0.10, keyWidth));
            // "Covering cell" indexes get a special adjustment to make them look more favorable
            // than non-unique range filters in particular.
            // I can't quite justify that rationally, but it "seems reasonable". --paul
            if (m_index.getType() == IndexType.COVERING_CELL_INDEX.getValue()) {
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

        // If table is distributed divide the row count by the split count.
        // The exchange node would combine individual fragments counts into a total.
        return estimateRowCountWithLimit(estimateRowCountWithPredicate(tuplesToRead)) / mq.splitCount(this);
    }

    private double getSearchExpressionKeyWidth(final double colCount) {
        double keyWidth = m_accessPath.getIndexExpressions().size();
        Preconditions.checkState(keyWidth <= colCount);
        // count a range scan as a half covered column
        if (keyWidth > 0.0 &&
                m_accessPath.getIndexLookupType() != IndexLookupType.EQ &&
                m_accessPath.getIndexLookupType() != IndexLookupType.GEO_CONTAINS) {
            keyWidth -= 0.5;
        } else if (keyWidth == 0.0 && !m_accessPath.getIndexExpressions().isEmpty()) {
            // When there is no start key, count an end-key as a single-column range scan key.

            // TODO: ( (double) ExpressionUtil.uncombineAny(m_endExpression).size() ) - 0.5
            // might give a result that is more in line with multi-component start-key-only scans.
            keyWidth = 0.5;
        }
        return keyWidth;
    }

    /**
     * Replace current program's condition with the accessPath.other condition
     * @param program
     * @param accessPath
     * @return
     */
    private static RexProgram updateProgram(RexProgram program, AccessPath accessPath) {
        // @TODO eliminate index expressions from the program
        return program;
    }

    @Override
    public VoltPhysicalTableIndexScan copyWithLimitOffset(RelTraitSet traitSet, RexNode offset, RexNode limit) {
        return new VoltPhysicalTableIndexScan(
                getCluster(), traitSet, getTable(), getVoltTable(), getProgram(),
                getIndex(), getAccessPath(), offset, limit, getAggregateRelNode(),
                getPreAggregateRowType(), getPreAggregateProgram(), getSplitCount(), m_indexCollation);
    }

    @Override
    public VoltPhysicalTableIndexScan copyWithProgram(
            RelTraitSet traitSet, RexProgram newProgram, RexBuilder rexBuilder) {
        // Merge two programs program / m_program into a new merged program
        final RexProgram  mergedProgram = RexProgramBuilder.mergePrograms(newProgram, m_program, rexBuilder);

        // If a new program has a condition the condition needs to be added to the index's accessParh
        // as an OTHER expression to contribute to the Index Scan predicate.
        final RexNode newCondition = newProgram.getCondition();
        if (newCondition != null) {
            m_accessPath.getOtherExprs().add(RexConverter.convertRefExpression(newCondition, newProgram));
        }

        // Adjust the collation for a new program
        return new VoltPhysicalTableIndexScan(getCluster(), traitSet, getTable(), getVoltTable(), mergedProgram,
                getIndex(), getAccessPath(), getOffsetRexNode(), getLimitRexNode(), getAggregateRelNode(),
                getPreAggregateRowType(), getPreAggregateProgram(), getSplitCount(),
                VoltRexUtil.adjustCollationForProgram(rexBuilder, mergedProgram, m_indexCollation));
    }

    @Override
    public VoltPhysicalTableIndexScan copyWithAggregate(RelTraitSet traitSet, RelNode aggregate) {
        // Need to create a Program for the inline aggregate because it will define
        // the output row type for the scan
        // Preserve the original program and row type
        return new VoltPhysicalTableIndexScan(getCluster(), traitSet, getTable(), getVoltTable(),
                RexProgram.createIdentity(aggregate.getRowType()),
                getIndex(), getAccessPath(), getOffsetRexNode(), getLimitRexNode(), aggregate, getRowType(),
                getProgram(), getSplitCount(), m_indexCollation);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    public RelCollation getIndexCollation() {
        return m_indexCollation;
    }
}
