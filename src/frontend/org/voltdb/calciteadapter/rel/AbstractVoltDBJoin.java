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
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;

public abstract class AbstractVoltDBJoin extends Join implements VoltDBRel {

    final RexProgram m_program;

    protected AbstractVoltDBJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexProgram program) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        m_program = program;
    }

    @Override
    public RelDataType deriveRowType() {
        if (m_program == null) {
            return super.deriveRowType();
        } else {
            return m_program.getOutputRowType();
        }
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_program != null) {
            m_program.explainCalc(pw);
        }
        return pw;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        RelOptCost cost = planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        return cost;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        assert(left != null);
        double outerEstimate = left.estimateRowCount(mq);

        assert(right != null);
        double innerEstimate = right.estimateRowCount(mq);

        double discount = estimateRowCountPredicateDiscount();
        return outerEstimate * discount * innerEstimate;
    }

    protected double estimateRowCountPredicateDiscount() {
        double discount = 1.0;
        if (getCondition() != null) {
            // Counters to count the number of equality and all other expressions
            int eqCount = 0;
            int otherCount = 0;
            final double MAX_EQ_POST_FILTER_DISCOUNT = 0.09;
            final double MAX_OTHER_POST_FILTER_DISCOUNT = 0.045;
            List<RexNode> filters = RelOptUtil.conjunctions(getCondition());
            // Discount tuple count.
            for (RexNode filter: filters) {
                if (SqlKind.EQUALS == filter.getKind()) {
                    discount -= Math.pow(MAX_EQ_POST_FILTER_DISCOUNT, ++eqCount);
                } else {
                    discount -= Math.pow(MAX_OTHER_POST_FILTER_DISCOUNT, ++otherCount);
                }
            }
        }
        return discount;
    }

    public RexProgram getProgram() {
        return m_program;
    }

    protected AbstractPlanNode setOutputSchema(AbstractJoinPlanNode ajpn) {
        assert(ajpn != null);
        assert m_program == null;
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getInput(0).getRowType());
        schema = schema.join(RexConverter.convertToVoltDBNodeSchema(getInput(1).getRowType()));
        ajpn.setOutputSchemaPreInlineAgg(schema);
        ajpn.setOutputSchema(schema);
        ajpn.setHaveSignificantOutputSchema(true);
        return ajpn;
    }

}
