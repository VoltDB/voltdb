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
import org.apache.calcite.plan.RelOptUtil;
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
import org.voltdb.plannerv2.utils.Cacheable;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;

import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract class VoltPhysicalJoin extends Join implements VoltPhysicalRel {
    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;
    private final Cacheable<RelMetadataQuery, Double> ROW_COUNT_CACHE =
            new Cacheable<RelMetadataQuery, Double>(64) {
                @Override protected Double calculate(RelMetadataQuery key) {
                    return estimateRowCountImpl(key);
                }
            };

    // Inline rels
    protected final RexNode m_offset;
    protected final RexNode m_limit;

    public VoltPhysicalJoin(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType, boolean semiJoinDone,
            ImmutableList<RelDataTypeField> systemFieldList, RexNode offset, RexNode limit) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION,
                "PhysicalJoin node convention mismatch");
        if (joinType != JoinRelType.INNER) {        // We support inner join for now
            // change/remove this when we support more join types
            throw new PlannerFallbackException("Join type not supported: " + joinType.name());
        }
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Objects.requireNonNull(systemFieldList);
        m_offset = offset;
        m_limit = limit;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
                .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
                .item("split", 1)
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

    // NOTE: we set this to 1 for SP queries.
    @Override
    public int getSplitCount() {
        return 1;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return ROW_COUNT_CACHE.cache_get(mq);
    }

    protected double estimateRowCountImpl(RelMetadataQuery mq) {
        // Give it a discount based on the number of equivalence expressions
        return Math.min(getInput(0).estimateRowCount(mq), getInput(1).estimateRowCount(mq)) *
                Math.pow(0.10, RelOptUtil.conjunctions(getCondition()).size());
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
