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

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;

public abstract class VoltPhysicalExchange extends Exchange implements VoltPhysicalRel {

    protected VoltPhysicalExchange(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
        super(cluster, traitSet, input, traitSet.getTrait(RelDistributionTraitDef.INSTANCE));
        Preconditions.checkArgument(! RelDistributions.ANY.getType().equals(
                traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType()));
    }

    @Override public VoltPhysicalExchange copy(
            RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        return copyInternal(traitSet, newInput);
    }

    /*public VoltPhysicalExchange copy(
            RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution, boolean isTopExchange) {
        return copyInternal(traitSet, newInput);
    }*/

    protected abstract VoltPhysicalExchange copyInternal(RelTraitSet traitSet, RelNode newInput);
}
