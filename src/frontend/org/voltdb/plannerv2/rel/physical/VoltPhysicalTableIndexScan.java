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

package org.voltdb.plannerv2.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.AccessPath;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;

import com.google_voltpatches.common.base.Preconditions;

public class VoltPhysicalTableIndexScan extends VoltPhysicalTableScan {

    private final Index m_index;
    private final AccessPath m_accessPath;
    private final RelCollation m_indexCollation;

    // TRUE if this index scan is an inline inner scan of a NLIJ or a MJ
    // It is required to properly set a corresponding table index in all TVEs from
    // the index access's path. TVEs are evaluated in a parent join context during runtime
    // and must have their table index set to 1 in this case
    // Since this is only for Volt TVE resolution, it shouldn't be part of the Calcite's digest
    private final boolean m_isInlinedInnerScan;

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
     * @param indexCollation
     */
    public VoltPhysicalTableIndexScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, VoltTable voltDBTable, RexProgram program,
            Index index, AccessPath accessPath, RexNode offset, RexNode limit, RelNode aggregate,
            RelDataType preAggregateRowType, RexProgram preAggregateProgram,
            RelCollation indexCollation, boolean isInlinedInnerScan) {
        super(cluster, traitSet, table, voltDBTable, updateProgram(program, accessPath),
              offset, limit, aggregate, preAggregateRowType, preAggregateProgram);
        Preconditions.checkNotNull(index, "index is null");
        Preconditions.checkNotNull(accessPath, "access path is null");
        Preconditions.checkNotNull(indexCollation, "index collation is null");
        m_index = index;
        m_accessPath = accessPath;
        m_indexCollation = indexCollation;
        m_isInlinedInnerScan = isInlinedInnerScan;
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
        return IndexUtil.buildIndexAccessPlanForTable(ispn, m_accessPath, m_isInlinedInnerScan? 1 : 0);
    }

    public Index getIndex() {
        return m_index;
    }

    public AccessPath getAccessPath() {
        return m_accessPath;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double rowCount = super.estimateRowCount(mq);
        // reduce number of returned rows for a partial index based on the number of it's predicates
        rowCount = PlanCostUtil.discountPartialIndexRowCount(rowCount, m_accessPath);
        return rowCount;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        double cpu = PlanCostUtil.computeIndexCost(m_index, m_accessPath,
                getTraitSet().getTrait(RelCollationTraitDef.INSTANCE),  rowCount);
        return planner.getCostFactory().makeCost(rowCount, cpu, 0.);
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
                getPreAggregateRowType(), getPreAggregateProgram(),
                m_indexCollation, m_isInlinedInnerScan);
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
            AbstractExpression programExpr = RexConverter.convertRefExpression(newCondition, newProgram);
            programExpr.findAllTupleValueSubexpressions().forEach(tve -> tve.setTableIndex(m_isInlinedInnerScan? 1 : 0));
            m_accessPath.getOtherExprs().add(programExpr);
        }

        // Adjust the collation for a new program
        return new VoltPhysicalTableIndexScan(getCluster(), traitSet, getTable(), getVoltTable(), mergedProgram,
                getIndex(), getAccessPath(), getOffsetRexNode(), getLimitRexNode(), getAggregateRelNode(),
                getPreAggregateRowType(), getPreAggregateProgram(),
                VoltRexUtil.adjustCollationForProgram(rexBuilder, mergedProgram,m_indexCollation),
                m_isInlinedInnerScan);
    }

    @Override
    public VoltPhysicalTableIndexScan copyWithAggregate(RelTraitSet traitSet, RelNode aggregate) {
        // Need to create a Program for the inline aggregate because it will define
        // the output row type for the scan
        // Preserve the original program and row type
        return new VoltPhysicalTableIndexScan(getCluster(), traitSet, getTable(), getVoltTable(),
                RexProgram.createIdentity(aggregate.getRowType()),
                getIndex(), getAccessPath(), getOffsetRexNode(), getLimitRexNode(), aggregate, getRowType(),
                getProgram(), m_indexCollation, m_isInlinedInnerScan);
    }

    public RelCollation getIndexCollation() {
        return m_indexCollation;
    }

    public boolean isInlinedInnerScan() {
        return m_isInlinedInnerScan;
    }
}
