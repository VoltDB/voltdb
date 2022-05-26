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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannerv2.converter.ExpressionTypeConverter;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.types.ExpressionType;

import java.util.List;

public abstract class VoltPhysicalAggregate extends Aggregate implements VoltPhysicalRel {

    // HAVING expression
    final private RexNode m_postPredicate;

    // TRUE if this aggregate relation is part of a coordinator tree.
    // The indicator may be useful during the Exchange Transform rule when a coordinator aggregate
    // differs from a fragment one
    final private boolean m_isCoordinatorAggr;

    /**
     * Constructor.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param child Child
     * @param indicator Whether row type should include indicator fields to
     *                  indicate which grouping set is active; true is deprecated
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
     * @param havingExpression HAVING expression
     * @param isCoordinatorAggr If this aggregate relation is part of a coordinator tree.
     */
    VoltPhysicalAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode havingExpression,
            boolean isCoordinatorAggr) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
        m_postPredicate = havingExpression;
        m_isCoordinatorAggr = isCoordinatorAggr;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("coordinator", m_isCoordinatorAggr);
        pw.itemIf("having", m_postPredicate, m_postPredicate != null);
        return pw;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_coordinator_" + m_isCoordinatorAggr;
        if (m_postPredicate != null) {
            digest += m_postPredicate.toString();
        }
        return digest;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double rowCount = getInput().estimateRowCount(mq);
        final double cpu = rowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    public double estimateRowCount(RelMetadataQuery mq) {
        Preconditions.checkNotNull(mq);

        // Adopted from Calicte's Aggregate.estimateRowCount
        // Assume that each GROUP BY column has 50% of the value count.
        // Therefore one GROUP BY column has .5 * rowCount,
        // 2 sort columns give .75 * rowCount.
        // Zero GROUP BY columns yields 1 row (or 0 if the input is empty).
        final int groupCount = groupSet.cardinality();
        if (groupCount == 0) {
            return 1;
        } else {
            return getInput(0).estimateRowCount(mq) * (1 - Math.pow(.5, groupCount));
        }
    }

    /**
     * Copy self, refer to the constructor.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param input Input
     * @param indicator Whether row type should include indicator fields to
     *                  indicate which grouping set is active; true is deprecated
     * @param groupSet Bit set of grouping fields
     * @param groupSets List of all grouping sets; null for just {@code groupSet}
     * @param aggCalls Collection of calls to aggregate functions
     * @param havingExpression HAVING expression
     * @param isCoordinatorAggr If this aggregate relation is part of a coordinator tree.
     * @return A cloned {@link VoltPhysicalAggregate}.
     */
    public abstract VoltPhysicalAggregate copy(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode havingExpression,
            boolean isCoordinatorAggr);

    public RexNode getPostPredicate() {
        return m_postPredicate;
    }

    public boolean getIsCoordinatorAggr() {
        return m_isCoordinatorAggr;
    }

    protected abstract AggregatePlanNode getAggregatePlanNode();

    @Override
    public AbstractPlanNode toPlanNode() {
        AbstractPlanNode apn = toPlanNode(getInput().getRowType());

        // Convert child
        AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        apn.addAndLinkChild(child);

        return apn;
    }

    /**
     * Convert self to a VoltDB plan node
     *
     * @param inputRowType
     * @return
     */
    AbstractPlanNode toPlanNode(RelDataType inputRowType) {
        AggregatePlanNode apn = getAggregatePlanNode();

        // Generate output schema
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getRowType(), 0);
        apn.setOutputSchema(schema);

        // The Aggregate's record layout seems to be
        // - GROUP BY expressions
        // - AGGR expressions form SELECT clause - corresponding aggrCall has name matching the filed name
        // - AGGR expressions from HAVING clause - aggrCall name is NULL
        RelDataType aggrRowType = getRowType();
        List<RelDataTypeField> fields = inputRowType.getFieldList();
        // Aggreagte fields start right after the grouping ones in order of the aggregate calls
        int aggrFieldIdx = getGroupCount();
        for(AggregateCall aggrCall : getAggCallList()) {
            // Aggr type
            ExpressionType aggrType;
            if (aggrCall.getAggregation() instanceof SqlUserDefinedAggFunction) {
                AggregateFunction agg = ((SqlUserDefinedAggFunction) aggrCall.getAggregation()).getFunction();
                aggrType = ExpressionType.get(((AggregateFunctionImpl) agg).getAggType());
            } else {
                aggrType = ExpressionTypeConverter.calciteTypeToVoltType(aggrCall.getAggregation().kind);
            }
            if (aggrType == ExpressionType.INVALID) {
                throw new CalcitePlanningException("Unsupported aggregate function: " + aggrCall.getAggregation().kind.lowerName);
            }

            List<Integer> aggrExprIndexes = aggrCall.getArgList();
            // VoltDB supports aggregates with only one parameter
            Preconditions.checkState(aggrExprIndexes.size() < 2);
            AbstractExpression aggrExpr = null;
            if (!aggrExprIndexes.isEmpty()) {
                RelDataTypeField field = fields.get(aggrExprIndexes.get(0));
                aggrExpr = RexConverter.convertDataTypeField(field);
            } else if (ExpressionType.AGGREGATE_COUNT == aggrType) {
                aggrType = ExpressionType.AGGREGATE_COUNT_STAR;
            }

            assert(aggrFieldIdx < aggrRowType.getFieldCount());
            apn.addAggregate(aggrType, aggrCall.isDistinct(),  aggrFieldIdx, aggrExpr);
            // Increment aggregate field index
            aggrFieldIdx++;
        }
        // Group by
        setGroupByExpressions(apn);
        // Having
        setPostPredicate(apn);

        return apn;
    }

    private void setGroupByExpressions(AggregatePlanNode apn) {
        ImmutableBitSet groupBy = getGroupSet();
        List<RelDataTypeField> rowTypeList = this.getRowType().getFieldList();
        for (int index = groupBy.nextSetBit(0); index != -1; index = groupBy.nextSetBit(index + 1)) {
            assert(index < rowTypeList.size());
            AbstractExpression groupByExpr = RexConverter.convertDataTypeField(rowTypeList.get(index));
            apn.addGroupByExpression(groupByExpr);
        }
    }

    private void setPostPredicate(AggregatePlanNode apn) {
        if (m_postPredicate != null) {
            AbstractExpression havingExpression = RexConverter.convert(m_postPredicate);
            apn.setPostPredicate(havingExpression);
        }
    }
}
