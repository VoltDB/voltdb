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

import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.types.JoinType;

public class VoltDBJoin extends Join implements VoltDBRel {

    final RexProgram m_program;

    public VoltDBJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        m_program = null;
    }

    protected VoltDBJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexProgram program) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType);
//        assert program != null;
        m_program = program;
        //rowType = m_program.getOutputRowType();
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
    public AbstractPlanNode toPlanNode() {
        NestLoopPlanNode nlpn = new NestLoopPlanNode();

        AbstractPlanNode lch = ((VoltDBRel)getInput(0)).toPlanNode();
        AbstractPlanNode rch = ((VoltDBRel)getInput(1)).toPlanNode();
        nlpn.addAndLinkChild(lch);
        nlpn.addAndLinkChild(rch);
        int numLhsFields = getInput(0).getRowType().getFieldCount();
        nlpn.setJoinPredicate(RexConverter.convertJoinPred(numLhsFields, getCondition()));

        assert m_program == null;
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getInput(0).getRowType());
        schema = schema.join(RexConverter.convertToVoltDBNodeSchema(getInput(1).getRowType()));
        nlpn.setOutputSchemaPreInlineAgg(schema);
        nlpn.setOutputSchema(schema);
        nlpn.setJoinType(JoinType.INNER);
        nlpn.setHaveSignificantOutputSchema(true);

        return nlpn;
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
            RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltDBJoin(getCluster(),
                getTraitSet(), left, right, conditionExpr,
                variablesSet, joinType, m_program);
       }

    public RelNode copy(RelNode left, RelNode right) {
        return new VoltDBJoin(
                getCluster(),
                getTraitSet(),
                left,
                right,
                getCondition(),
                getVariablesSet(),
                getJoinType(),
                m_program
                );
    }

}
