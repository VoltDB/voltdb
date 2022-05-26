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

package org.voltdb.plannerv2.rel.logical;

import java.util.List;
import java.util.Objects;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Sub-class of {@link Join} targeted at the VoltDB logical calling convention.
 *
 * @see org.apache.calcite.rel.logical.LogicalJoin
 * @author Chao Zhou
 * @since 9.1
 */
public class VoltLogicalJoin extends Join implements VoltLogicalRel {

    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;
    private final RexNode whereCondition;

    /**
     * Creates a VoltLogicalJoin.
     *
     * @param cluster          Cluster
     * @param traitSet         Trait set
     * @param left             Left input
     * @param right            Right input
     * @param condition        Join condition
     * @param joinType         Join type
     * @param variablesSet     Set of variables that are set by the
     *                         LHS and used by the RHS and are not available to
     *                         nodes above this LogicalJoin in the tree
     * @param semiJoinDone     Whether this join has been translated to a
     *                         semi-join
     * @param systemFieldList  List of system fields that will be prefixed to
     *                         output row type; typically empty but must not be
     *                         null
     * @param whereCondition   Additional optional Join WHERE condition
     * @see #isSemiJoinDone()
     */
    public VoltLogicalJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList,
            RexNode whereCondition) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Objects.requireNonNull(systemFieldList);
        this.whereCondition = whereCondition;
    }

    public VoltLogicalJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList) {
        this(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, null);
    }

    @Override public VoltLogicalJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
            RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltLogicalJoin(getCluster(), traitSet, left, right, conditionExpr,
                variablesSet, joinType, semiJoinDone, systemFieldList, whereCondition);
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
                .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
                .itemIf("whereCOndition", whereCondition, whereCondition != null);
    }

    @Override public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    @Override public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    public RexNode getWhereCondition() {
        return whereCondition;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(cost.getRows(),
                cost.getRows(),     // NOTE: CPU cost comes into effect in physical planning stage.
                cost.getIo());
    }

}
