/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.calciteadapter.CalcitePlanningException;
import org.voltdb.calciteadapter.ExpressionTypeConverter;
import org.voltdb.calciteadapter.RelConverter;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.types.ExpressionType;

public class VoltDBAggregate extends Aggregate implements VoltDBRel {

    RexNode m_postPredicate = null;

    public VoltDBAggregate(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode childNode, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls,
            RexNode postPredicate) {
        super(cluster,
              traitSet,
              childNode,
              indicator,
              groupSet,
              groupSets,
              aggCalls);
        if (postPredicate != null) {
            m_postPredicate = postPredicate;
        }
    }

    public VoltDBAggregate(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode childNode, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        this(cluster,
              traitSet,
              childNode,
              indicator,
              groupSet,
              groupSets,
              aggCalls,
              null);
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input,
            boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        VoltDBAggregate newAggr = new VoltDBAggregate(getCluster(), getTraitSet(), getInput(), indicator,
                getGroupSet(), getGroupSets(), getAggCallList(), m_postPredicate);
        return newAggr;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Need to explain partitioning
        super.explainTerms(pw);
        if (m_postPredicate != null) {
            pw.item("having", m_postPredicate);
        }
        return pw;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        HashAggregatePlanNode hapn = new HashAggregatePlanNode();

        // Convert child
        VoltDBRel inputNode = getInputNode(this);
        assert(inputNode != null);
        AbstractPlanNode child = inputNode.toPlanNode();
        hapn.addAndLinkChild(child);

        // Generate output schema
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getRowType());
        hapn.setOutputSchema(schema);

        // The Aggregate's record layout seems to be
        // - GROUP BY expressions
        // - AGGR expressions form SELECT clause - corresponding aggrCall has name matching the filed name
        // - AGGR expressions from HAVING clause - aggrCall name is NULL
        RelDataType aggrRowType = getRowType();
        RelDataType scanRowType = inputNode.getRowType();
        List<RelDataTypeField> fields = scanRowType.getFieldList();
        // Aggreagte fields start right after the grouping ones in order of the aggregate calls
        int aggrFieldIdx = 0 + getGroupCount();
        for(AggregateCall aggrCall : getAggCallList()) {
            // Aggr type
            ExpressionType aggrType =
                    ExpressionTypeConverter.calicteTypeToVoltType(aggrCall.getAggregation().kind);
            if (aggrType == null) {
                throw new CalcitePlanningException("Unsupported aggregate function: " + aggrCall.getAggregation().kind.lowerName);
            }

            List<Integer> aggrExprIndexes = aggrCall.getArgList();
            // VoltDB supports aggregates with only one parameter
            assert(aggrExprIndexes.size() < 2);
            AbstractExpression aggrExpr = null;
            if (!aggrExprIndexes.isEmpty()) {
                RelDataTypeField field = fields.get(aggrExprIndexes.get(0));
                aggrExpr = RelConverter.convertDataTypeField(field);
            } else if (ExpressionType.AGGREGATE_COUNT == aggrType) {
                aggrType = ExpressionType.AGGREGATE_COUNT_STAR;
            }

            assert(aggrFieldIdx < aggrRowType.getFieldCount());
            hapn.addAggregate(aggrType, aggrCall.isDistinct(),  aggrFieldIdx, aggrExpr);
            // Increment aggregate field index
            aggrFieldIdx++;
        }
        // Group by
        setGroupByExpressions(hapn);
        // Having
        setPostPredicate(hapn);

        return hapn;
    }

    private void setGroupByExpressions(HashAggregatePlanNode hapn) {
        ImmutableBitSet groupBy = getGroupSet();
        List<RelDataTypeField> rowTypeList = this.getRowType().getFieldList();
        for (int index = groupBy.nextSetBit(0); index != -1; index = groupBy.nextSetBit(index + 1)) {
            assert(index < rowTypeList.size());
            AbstractExpression groupByExpr = RelConverter.convertDataTypeField(rowTypeList.get(index));
            hapn.addGroupByExpression(groupByExpr);
        }
    }

    private void setPostPredicate(HashAggregatePlanNode hapn) {
        if (m_postPredicate != null) {
            AbstractExpression havingExpression = RexConverter.convert(m_postPredicate);
            hapn.setPostPredicate(havingExpression);
        }
    }

}
