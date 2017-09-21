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
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NestLoopPlanNode;
import org.voltdb.types.JoinType;

public class VoltDBNLJoin extends AbstractVoltDBJoin {

    public VoltDBNLJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexProgram program) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, program);
    }

    public VoltDBNLJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        this (cluster, traitSet, left, right, condition, variablesSet, joinType, null);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double estimates = super.estimateRowCount(mq);
        return estimates;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        NestLoopPlanNode nlpn = new NestLoopPlanNode();

        // INNER join for now
        assert(joinType == JoinRelType.INNER);
        nlpn.setJoinType(JoinType.INNER);

        // Set children
        AbstractPlanNode lch = ((VoltDBRel)getInput(0)).toPlanNode();
        AbstractPlanNode rch = ((VoltDBRel)getInput(1)).toPlanNode();
        nlpn.addAndLinkChild(lch);
        nlpn.addAndLinkChild(rch);

        // Set join predicate
        int numLhsFields = getInput(0).getRowType().getFieldCount();
        nlpn.setJoinPredicate(RexConverter.convertJoinPred(numLhsFields, getCondition()));

        // Set output schema
        return super.setOutputSchema(nlpn);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
            RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltDBNLJoin(getCluster(),
                getTraitSet(), left, right, conditionExpr,
                variablesSet, joinType, m_program);
       }

    public RelNode copy(RelNode left, RelNode right) {
        return new VoltDBNLJoin(
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
