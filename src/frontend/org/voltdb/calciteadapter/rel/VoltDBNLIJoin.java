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
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.types.JoinType;

public class VoltDBNLIJoin extends AbstractVoltDBJoin {

    // For digest only
    private String m_inlineIndexName;

    public VoltDBNLIJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RexProgram program,
            String inlineIndexName) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, program);
        assert(inlineIndexName != null);
        m_inlineIndexName = inlineIndexName;
    }

    public VoltDBNLIJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            String inlineIndexName) {
        this (cluster, traitSet, left, right, condition, variablesSet, joinType, null, inlineIndexName);
    }

    @Override
    protected String computeDigest() {
        // Make inner Index to be part of the digest to disambiguate the node
        String dg = super.computeDigest();
        dg += "_index_" + m_inlineIndexName;
        return dg;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double estimates = super.estimateRowCount(mq) / 2;
        return estimates;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        NestLoopIndexPlanNode nlipn = new NestLoopIndexPlanNode();

        // INNER join for now
        assert(joinType == JoinRelType.INNER);
        nlipn.setJoinType(JoinType.INNER);

        // Set children
        AbstractPlanNode lch = ((VoltDBRel)getInput(0)).toPlanNode();
        AbstractPlanNode rch = ((VoltDBRel)getInput(1)).toPlanNode();
        assert(rch instanceof IndexScanPlanNode);

        nlipn.addAndLinkChild(lch);
        nlipn.addInlinePlanNode(rch);

        // We don't need to set the join predicate explicitly here because it will be
        // an index and/or filter expressions for the inline index scan

        // Set output schema
        return super.setOutputSchema(nlipn);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left,
            RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new VoltDBNLIJoin(getCluster(),
                getTraitSet(), left, right, conditionExpr,
                variablesSet, joinType, m_program, m_inlineIndexName);
       }

}
