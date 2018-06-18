/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;

public class VoltDBPMergeExchange extends AbstractVoltDBPExchange implements VoltDBPRel {

    public VoltDBPMergeExchange(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelDistribution distribution,
            int childSplitCount) {
        super(cluster, traitSet, input, distribution, childSplitCount);
    }

    @Override
    public VoltDBPMergeExchange copy(RelTraitSet traitSet,
            RelNode newInput,
            RelDistribution newDistribution,
            int level) {
        VoltDBPMergeExchange exchange = new VoltDBPMergeExchange(
                getCluster(),
                traitSet,
                newInput,
                newDistribution,
                m_childSplitCount);
        exchange.setLevel(level);
        return exchange;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        MergeReceivePlanNode rpn = new MergeReceivePlanNode();
        return super.toPlanNode(rpn);
    }

}
