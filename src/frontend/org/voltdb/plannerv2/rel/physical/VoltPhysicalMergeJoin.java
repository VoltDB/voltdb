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

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.plannerv2.converter.RelConverter;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.MergeJoinPlanNode;
import org.voltdb.plannodes.NodeSchema;

import com.google.common.collect.ImmutableList;
import com.google_voltpatches.common.base.Preconditions;

public class VoltPhysicalMergeJoin extends VoltPhysicalJoin {

    private final String m_outerIndexName;
    private final String m_innerIndexName;

    public VoltPhysicalMergeJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, RexNode whereCondition,
            String outerIndex, String innerIndex) {
        this(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, whereCondition, outerIndex, innerIndex, null, null);
    }

    private VoltPhysicalMergeJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, RexNode whereCondition,
            String outerIndex, String innerIndex, RexNode offset, RexNode limit) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, whereCondition, offset, limit);
        Preconditions.checkNotNull(outerIndex, "Outer index is null");
        Preconditions.checkNotNull(innerIndex, "Inner index is null");
        m_outerIndexName = outerIndex;
        m_innerIndexName = innerIndex;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
                     RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltPhysicalMergeJoin(getCluster(), traitSet, left, right, conditionExpr,
                variablesSet, joinType, semiJoinDone, ImmutableList.copyOf(getSystemFieldList()),
                whereCondition, m_outerIndexName, m_innerIndexName, m_offset, m_limit);
    }

    @Override
    public VoltPhysicalJoin copyWithLimitOffset(RelTraitSet traits, RexNode offset, RexNode limit) {
        return new VoltPhysicalMergeJoin(getCluster(), traits, left, right, condition,
                variablesSet, joinType, isSemiJoinDone(), ImmutableList.copyOf(getSystemFieldList()),
                whereCondition, m_outerIndexName, m_innerIndexName, offset, limit);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("outerIndex", m_outerIndexName);
        pw.item("innerIndex", m_innerIndexName);
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        double outerRowCount = getInput(0).estimateRowCount(mq);
        double innerRowCount = getInput(1).estimateRowCount(mq);
        // Since we support only equality join conditions for a Merje Join (equi join)
        // and its inputs are guaranteed to be sorted, the MJ cost is simply M + N
        double cpu = outerRowCount + innerRowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final MergeJoinPlanNode mjpn = new MergeJoinPlanNode();
        mjpn.setJoinType(RelConverter.convertJointType(joinType));

        // Outer node
        AbstractPlanNode outerPlanNode = inputRelNodeToPlanNode(this, 0);
        mjpn.addAndLinkChild(outerPlanNode);
        RexProgram outerProgram = null;
        if (getInput(0) instanceof VoltPhysicalTableIndexScan) {
            outerProgram = ((VoltPhysicalTableIndexScan)getInput(0)).getProgram();
        }

        // Inline inner index scan
        AbstractPlanNode innerScan = inputRelNodeToPlanNode(this, 1);
        Preconditions.checkState(innerScan instanceof IndexScanPlanNode);
        mjpn.addInlinePlanNode(innerScan);
        // Set join predicate
        assert(getInput(1) instanceof VoltPhysicalTableIndexScan);
        VoltPhysicalTableIndexScan innerIndexScan = (VoltPhysicalTableIndexScan) getInput(1);

        mjpn.setJoinPredicate(RexConverter.convertJoinPred(
                getInput(0).getRowType().getFieldCount(), getCondition(), outerProgram, innerIndexScan.getProgram()));
        mjpn.setWherePredicate(RexConverter.convertJoinPred(
                getInput(0).getRowType().getFieldCount(), getWhereCondition(), outerProgram, innerIndexScan.getProgram()));

        // Inline LIMIT / OFFSET
        addLimitOffset(mjpn);
        // Set output schema
        setOutputSchema(mjpn);
        return mjpn;
    }

    @Override
    protected void setOutputSchema(AbstractJoinPlanNode node) {
        Preconditions.checkNotNull(node, "Plan node is null");
        // An inner node has to be an index scan
        assert getInput(1) instanceof VoltPhysicalTableIndexScan;
        // Since it's going to be inlined and MJ executor will be iterating directly over
        // its persistent table all the expression references must be resolved
        // in context of the persistent table
        final VoltPhysicalTableIndexScan innerIndexScan = (VoltPhysicalTableIndexScan) getInput(1);
        final RexProgram innerProgram = innerIndexScan.getProgram();
        final NodeSchema innerSchema = RexConverter.convertToVoltDBNodeSchema(innerProgram, 1);

        // Outer node
        final NodeSchema outerSchema;
        if (getInput(0) instanceof VoltPhysicalTableIndexScan) {
            // If the outer node is an index scan, then all the references must be resolved
            // using the persistent table similar to the inner node
            outerSchema = RexConverter.convertToVoltDBNodeSchema(
                    ((VoltPhysicalTableIndexScan) getInput(0)).getProgram(), 0);
        } else {
            outerSchema = RexConverter.convertToVoltDBNodeSchema(
                    getInput(0).getRowType(), 0);
        }
        final NodeSchema schema = outerSchema.join(innerSchema);
        node.setOutputSchemaPreInlineAgg(schema);
        node.setOutputSchema(schema);
        node.setHaveSignificantOutputSchema(true);
    }

}
