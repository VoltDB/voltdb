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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.types.JoinType;

import com.google.common.collect.ImmutableList;

public class VoltPhysicalNestLoopIndexJoin extends VoltPhysicalJoin {

    // For digest only
    private final String m_innerIndexName;

    public VoltPhysicalNestLoopIndexJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, int splitCount, String innnerIndexName) {
        this(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, splitCount, innnerIndexName, null, null);
    }

    private VoltPhysicalNestLoopIndexJoin(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType,
            boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, int splitCount,
            String innnerIndexName,
            RexNode offset, RexNode limit) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType,
                semiJoinDone, systemFieldList, splitCount, offset, limit);
        Preconditions.checkNotNull(innnerIndexName, "Inner index name is null");
        m_innerIndexName = innnerIndexName;
    }

    @Override
    protected String computeDigest() {
        // Make inner Index to be part of the digest to disambiguate the node
        return new StringBuilder(super.computeDigest())
                .append("_index_")
                .append(m_innerIndexName)
                .toString();
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final NestLoopIndexPlanNode nlipn = new NestLoopIndexPlanNode();

        // TODO: INNER join for now
        assert(joinType == JoinRelType.INNER);
        nlipn.setJoinType(JoinType.INNER);

        // Set children
        nlipn.addAndLinkChild(inputRelNodeToPlanNode(this, 0));
        final AbstractPlanNode rhn = inputRelNodeToPlanNode(this, 1);
        assert(rhn instanceof IndexScanPlanNode);
        nlipn.addInlinePlanNode(rhn);

        // We don't need to set the join predicate explicitly here because it will be
        // an index and/or filter expressions for the inline index scan
        // but we need to adjust the index scan's predicate - all TVE expressions there belong to the scan node
        // and their indexes have to be set to 1 because its an inner table
        // All other index scan expressions are part of a join expressions and should already have
        // the correct TVE index set
        final AbstractExpression postPredicate = ((IndexScanPlanNode) rhn).getPredicate();
        if (postPredicate != null) {
            postPredicate.findAllSubexpressionsOfClass(TupleValueExpression.class)
                    .forEach(expr -> ((TupleValueExpression) expr).setTableIndex(1));
        }
        // Inline LIMIT / OFFSET
        addLimitOffset(nlipn);
        // Set output schema
        return setOutputSchema(nlipn);
    }

    @Override
    public Join copy(
            RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
            RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltPhysicalNestLoopIndexJoin(getCluster(),
                getTraitSet(), left, right, conditionExpr,
                variablesSet, joinType, semiJoinDone, ImmutableList.copyOf(getSystemFieldList()),
                getSplitCount(), m_innerIndexName);
    }

    @Override
    public VoltPhysicalJoin copyWithLimitOffset(RelTraitSet traits, RexNode offset, RexNode limit) {
        ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.copyOf(getSystemFieldList());
        return new VoltPhysicalNestLoopIndexJoin(getCluster(),
                traits, left, right, condition,
                variablesSet, joinType, isSemiJoinDone(), systemFieldList, getSplitCount(), m_innerIndexName, offset, limit);
    }

}
