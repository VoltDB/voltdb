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

import java.util.Set;

import com.google_voltpatches.common.base.Preconditions;
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
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.MergeJoinPlanNode;
import org.voltdb.types.JoinType;

import com.google.common.collect.ImmutableList;

public class VoltPhysicalMergeJoin extends VoltPhysicalJoin {

    private final String m_outerIndexName;
    private final String m_innerIndexName;

    public VoltPhysicalMergeJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, String outerIndex, String innerIndex) {
        this(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, outerIndex, innerIndex, null, null);
    }

    private VoltPhysicalMergeJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, String outerIndex,
            String innerIndex, RexNode offset, RexNode limit) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, offset, limit);
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
                m_outerIndexName, m_innerIndexName);
    }

    @Override
    public VoltPhysicalJoin copyWithLimitOffset(RelTraitSet traits, RexNode offset, RexNode limit) {
        return new VoltPhysicalMergeJoin(getCluster(), traits, left, right, condition,
                variablesSet, joinType, isSemiJoinDone(), ImmutableList.copyOf(getSystemFieldList()),
                m_outerIndexName, m_innerIndexName, offset, limit);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("outerIndex", m_outerIndexName);
        pw.item("innerIndex", m_innerIndexName);
        return pw;
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        final double dRows = estimateRowCount(mq);
        final double dCpu = dRows + 1; // ensure non-zero cost
        final double dIo = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        if (joinType != JoinRelType.INNER) {        // We support inner join for now
            // change/remove this when we support more join types
            throw new PlannerFallbackException("Join type not supported: " + joinType.name());
        }
        final MergeJoinPlanNode mjpn = new MergeJoinPlanNode();
        mjpn.setJoinType(JoinType.INNER);
        mjpn.addAndLinkChild(inputRelNodeToPlanNode(this, 0));
        mjpn.addAndLinkChild(inputRelNodeToPlanNode(this, 1));
        // Set join predicate
        mjpn.setJoinPredicate(RexConverter.convertJoinPred(getInput(0).getRowType().getFieldCount(), getCondition()));
        // Inline LIMIT / OFFSET
        addLimitOffset(mjpn);
        // Set output schema
        return setOutputSchema(mjpn);
    }
}
