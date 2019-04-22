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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class VoltPhysicalJoin extends Join implements VoltPhysicalRel {
    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;

    // Inline rels
    protected final RexNode m_offset;
    protected final RexNode m_limit;

    private final int m_splitCount;

    public VoltPhysicalJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, int splitCount, RexNode offset, RexNode limit) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION,
                "PhysicalJoin node convention mismatch");
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Objects.requireNonNull(systemFieldList);
        m_splitCount = splitCount;
        m_offset = offset;
        m_limit = limit;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
                .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
                .item("split", m_splitCount)
                .itemIf("offset", m_offset, m_offset != null)
                .itemIf("limit", m_limit, m_limit != null);
    }

    abstract public VoltPhysicalJoin copyWithLimitOffset(RelTraitSet traits, RexNode offset, RexNode limit);

    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    protected AbstractPlanNode setOutputSchema(AbstractJoinPlanNode node) {
        Preconditions.checkNotNull(node, "Plan node is null");
        final NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getInput(0).getRowType(), 0)
                .join(RexConverter.convertToVoltDBNodeSchema(getInput(1).getRowType(), 0));
        node.setOutputSchemaPreInlineAgg(schema);
        node.setOutputSchema(schema);
        node.setHaveSignificantOutputSchema(true);
        return node;
    }

    /**
     * Convert JOIN's LIMIT/OFFSET to an inline LimitPlanNode
     * @param node join node
     * @return possibly inlined join node
     */
    protected AbstractPlanNode addLimitOffset(AbstractPlanNode node) {
        if (m_limit != null || m_offset != null) {
            node.addInlinePlanNode(VoltPhysicalLimit.toPlanNode(m_limit, m_offset));
        }
        return node;
    }
}
