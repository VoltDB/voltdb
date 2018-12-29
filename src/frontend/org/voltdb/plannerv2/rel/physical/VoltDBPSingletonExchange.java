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

package org.voltdb.plannerv2.rel.physical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Sub-class of {@link AbstractVoltDBPExchange}
 * targeted at Exchange on a single partition.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltDBPSingletonExchange extends AbstractVoltDBPExchange implements VoltDBPRel {

    public VoltDBPSingletonExchange(RelOptCluster cluster,
                                    RelTraitSet traitSet,
                                    RelNode input,
                                    boolean isTopExchange) {
        super(cluster, traitSet, input, 1, isTopExchange);
    }

    @Override
    protected VoltDBPSingletonExchange copyInternal(
            RelTraitSet traitSet,
            RelNode newInput,
            boolean isTopExchange) {
        VoltDBPSingletonExchange exchange = new VoltDBPSingletonExchange(
                getCluster(),
                traitSet,
                newInput,
                isTopExchange);
        return exchange;
    }
}
