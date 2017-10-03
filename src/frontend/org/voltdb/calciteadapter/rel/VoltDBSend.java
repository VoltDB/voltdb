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
import org.apache.calcite.rel.SingleRel;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.calciteadapter.VoltDBPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;

public class VoltDBSend extends SingleRel implements VoltDBRel {

    private VoltDBPartitioning m_partitioning;

    public VoltDBSend(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode childNode, VoltDBPartitioning partitioning) {
        super(cluster, traitSet, childNode);
        m_partitioning = partitioning;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Need to explain partitioning
        super.explainTerms(pw);
        return pw;
    }

//    @Override
//    public RelOptCost computeSelfCost(RelOptPlanner planner,
//            RelMetadataQuery mq) {
//        // double rowCount = getInput().estimateRowCount(mq) * m_costFactor;
//        return planner.getCostFactory().makeCost(m_costFactor, 0, 0);
//    }

    @Override
    public AbstractPlanNode toPlanNode() {
        SendPlanNode spn = new SendPlanNode();

        AbstractPlanNode child = ((VoltDBRel) getInput(0)).toPlanNode();
        spn.addAndLinkChild(child);

        // Add a coordinator's Receive node
        ReceivePlanNode rpn = new ReceivePlanNode();
        rpn.addAndLinkChild(spn);
        // Generate output schema
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(getInput().getRowType());
        rpn.setOutputSchema(schema);
        rpn.setHaveSignificantOutputSchema(true);

        return rpn;
    }

    @Override
    public VoltDBSend copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VoltDBSend(getCluster(), traitSet, sole(inputs), m_partitioning);
      }

    public RelNode copy(RelNode input) {
        return new VoltDBSend(getCluster(), getTraitSet(), input, m_partitioning);
    }

    public VoltDBPartitioning getPartitioning() {
        return m_partitioning;
    }
}
