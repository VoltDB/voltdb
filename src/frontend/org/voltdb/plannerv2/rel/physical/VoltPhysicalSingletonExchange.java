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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

/**
 * We need this to plan send/recv pairs for MP queries.
 */
public class VoltPhysicalSingletonExchange extends VoltPhysicalExchange implements VoltPhysicalRel {

    public VoltPhysicalSingletonExchange(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
        super(cluster, traitSet, input);
    }

    @Override
    protected VoltPhysicalSingletonExchange copyInternal(
            RelTraitSet traitSet, RelNode newInput) {
        return new VoltPhysicalSingletonExchange(
                getCluster(), traitSet, newInput);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final SendPlanNode spn = new SendPlanNode();
        spn.addAndLinkChild(inputRelNodeToPlanNode(this, 0));
        return spn;
    }
}
