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

import java.util.Collections;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.plannerv2.VoltTable;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.rel.AbstractVoltTableScan;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Abstract sub-class of {@link AbstractVoltTableScan}
 * target at {@link #CONVENTION} convention
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public abstract class VoltPhysicalTableScan extends AbstractVoltTableScan implements VoltPhysicalRel {

    protected final RexProgram m_program;

    // Inline Rels
    protected RexNode m_offset;
    protected RexNode m_limit;
    protected RelNode m_aggregate;
    protected RelDataType m_preAggregateRowType;
    protected RexProgram m_preAggregateProgram;

    /**
     * Constructor.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table The table definition
     * @param voltDBTable The target {@link VoltTable}
     * @param program Program
     * @param offset Offset
     * @param limit Limmit
     * @param aggregate Aggregate
     * @param preAggregateRowType The type of the rows returned by this relational expression before aggregation
     * @param preAggregateProgram The program before aggregation
     */
    protected VoltPhysicalTableScan(
            RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, VoltTable voltDBTable, RexProgram program,
            RexNode offset, RexNode limit, RelNode aggregate, RelDataType preAggregateRowType, RexProgram preAggregateProgram) {
        super(cluster, traitSet.plus(VoltPhysicalRel.CONVENTION), table, voltDBTable);
        Preconditions.checkNotNull(program);
        Preconditions.checkArgument(aggregate == null || aggregate instanceof VoltPhysicalAggregate);
        Preconditions.checkArgument(program.getOutputRowType().getFieldCount() > 0, "Column count can not be 0.");
        m_program = program;
        m_offset = offset;
        m_limit = limit;
        m_aggregate = aggregate;
        m_preAggregateRowType = preAggregateRowType;
        m_preAggregateProgram = preAggregateProgram;
    }

    public RexProgram getProgram() {
        return m_program;
    }

    /**
     * The digest needs to be updated because Calcite considers any two nodes with the same digest
     * to be identical.
     */
    @Override
    protected String computeDigest() {
        // Make an instance of the scan unique for Calcite to be able to distinguish them
        // specially when we merge scans with other redundant nodes like sort for example.
        // Are there better ways of doing this?
        String dg = super.computeDigest();
        if (m_program != null) {
            dg += "_program_" + m_program.toString();
        }
        if (m_limit != null) {
            dg += "_limit_" + getLimit();
        }
        if (m_offset != null) {
            dg += "_offset_" + getOffset();
        }
        if (m_aggregate != null) {
            dg += "_aggr_" + m_aggregate.getDigest();
        }
        return dg;
    }

    @Override
    public VoltTable getVoltTable() {
        return m_voltTable;
    }

    @Override
    public RelDataType deriveRowType() {
        if (m_program == null) {
            return table.getRowType();
        } else {
            final RelDataType rowDataType = m_program.getOutputRowType();
            if (rowDataType.getFieldCount() > 0) {
                return rowDataType;
            } else {
                throw new IllegalStateException("Column count can not be 0.");
            }
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_program != null) {
            m_program.explainCalc(pw);
        }
        pw.itemIf("limit", m_limit, m_limit != null);
        pw.itemIf("offset", m_offset, m_offset != null);
        if (m_aggregate != null) {
            pw.item("aggregate", m_aggregate.getDigest());
        }

        return pw;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        Preconditions.checkNotNull(mq);
        double rowCount = PlanCostUtil.discountTableScanRowCount(estimateInitialRowCount(mq), m_program);
        return PlanCostUtil.discountLimitOffsetRowCount(rowCount, m_offset, m_limit);
    }

    protected double estimateInitialRowCount(RelMetadataQuery mq) {
        return getTable().getRowCount();
    }

    public RexNode getLimitRexNode() {
        return m_limit;
    }

    protected int getLimit() {
        if (m_limit != null) {
            if (m_limit instanceof RexDynamicParam) { // when LIMIT ?
                return -1;
            } else {
                return RexLiteral.intValue(m_limit);
            }
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public RexNode getOffsetRexNode() {
        return m_offset;
    }

    protected int getOffset() {
        if (m_offset != null) {
            if (m_offset instanceof RexDynamicParam) {
                return -1;
            } else {
                return RexLiteral.intValue(m_offset);
            }
        } else {
            return 0;
        }
    }

    public RelNode getAggregateRelNode() {
        return m_aggregate;
    }

    public RelDataType getPreAggregateRowType() {
        return m_preAggregateRowType;
    }

    public RexProgram getPreAggregateProgram() {
        return m_preAggregateProgram;
    }

    /**
     * Returns a copy of this {@link AbstractVoltTableScan} but with a new
     * traitSet, limit and offset.
     */
    public abstract AbstractVoltTableScan copyWithLimitOffset(RelTraitSet traitSet, RexNode offset, RexNode limit);

    /**
     * Returns a copy of this {@link AbstractVoltTableScan} but with a new traitSet and program.
     */
    public abstract AbstractVoltTableScan copyWithProgram(RelTraitSet traitSet, RexProgram program, RexBuilder rexBuilder);

    /**
     * Returns a copy of this {@link AbstractVoltTableScan} but with a new traitSet and aggregate.
     */
    public abstract AbstractVoltTableScan copyWithAggregate(RelTraitSet traitSet, RelNode aggregate);


    private boolean hasLimitOffset() {
        return (m_limit != null || m_offset != null);
    }

    /**
     * Convert Scan's predicate (condition) to VoltDB AbstractExpressions
     *
     * @param scan
     * @return
     */
    protected AbstractPlanNode addPredicate(AbstractScanPlanNode scan) {
        // If there is an inline aggregate, the scan's original program is saved as a m_preAggregateProgram
        final RexProgram program = (m_aggregate == null) ? m_program : m_preAggregateProgram;
        Preconditions.checkNotNull(program);

        final RexLocalRef condition = program.getCondition();
        if (condition != null) {
            scan.setPredicate(Collections.singletonList(RexConverter.convert(program.expandLocalRef(condition))));
        }
        return scan;
    }

    /**
     * Convert Scan's LIMIT / OFFSET to an inline LimitPlanNode
     *
     * @param node
     * @return
     */
    protected AbstractPlanNode addLimitOffset(AbstractPlanNode node) {
        if (hasLimitOffset()) {
            node.addInlinePlanNode(VoltPhysicalLimit.toPlanNode(m_limit, m_offset));
        }
        return node;
    }

    /**
     * Convert Scan's Project to an inline ProjectionPlanNode
     *
     * @param node
     * @return
     */
    protected AbstractPlanNode addProjection(AbstractPlanNode node) {
        // If there is an inline aggregate, the scan's original program is saved as a m_preAggregateProgram
        RexProgram program = m_aggregate == null ? m_program : m_preAggregateProgram;
        Preconditions.checkNotNull(program, "Cannot add projection when program is null");
        final ProjectionPlanNode ppn = new ProjectionPlanNode();
        ppn.setOutputSchemaWithoutClone(RexConverter.convertToVoltDBNodeSchema(program, 0));
        node.addInlinePlanNode(ppn);
        return node;
    }

    /**
     * Convert Scan's aggregate to an inline AggregatePlanNode / HashAggregatePlanNode
     *
     * @param node
     * @return
     */
    protected AbstractPlanNode addAggregate(AbstractPlanNode node) {
        if (m_aggregate != null) {
            Preconditions.checkNotNull(m_preAggregateRowType);
            final AbstractPlanNode aggr = ((VoltPhysicalAggregate) m_aggregate).toPlanNode(m_preAggregateRowType);
            aggr.clearChildren();
            node.addInlinePlanNode(aggr);
            node.setOutputSchema(aggr.getOutputSchema());
            node.setHaveSignificantOutputSchema(true);
            // Inline limit /offset with Serial Aggregate only. This is enforced by the VoltPhysicalLimitScanMergeRule.
            // The VoltPhysicalAggregateScanMergeRule should be triggered prior to the VoltPhysicalLimitScanMergeRule
            // allowing the latter to avoid merging VoltDBLimit and Scan nodes if the scan already has an inline aggregate
            Preconditions.checkState((aggr instanceof HashAggregatePlanNode && !hasLimitOffset()) ||
                    aggr instanceof AggregatePlanNode);
            node = addLimitOffset(aggr);
        }
        return node;
    }
}
