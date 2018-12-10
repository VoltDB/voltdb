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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;

/**
 * Abstract sub-class of {@link Exchange}
 * with VoltDB specific attributes about distribution.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public abstract class AbstractVoltDBPExchange extends Exchange implements VoltDBPRel {

    // Exchange's split count equals the count of (host number * site number per host)  its input runs on
    protected final int m_splitCount;

    // An indicator to be set to TRUE only for a top(coordinator) exchange for a multi-partitioned queries
    // Other relations could take advantage of this flag during Exchange Transpose rules if a relation
    // behavior depends whether it's part of the coordinator or fragment stack
    protected final boolean m_isTopExchange;

    protected AbstractVoltDBPExchange(RelOptCluster cluster,
                                      RelTraitSet traitSet,
                                      RelNode input,
                                      int splitCount,
                                      boolean topExchange) {
        super(cluster, traitSet, input, traitSet.getTrait(RelDistributionTraitDef.INSTANCE));
        Preconditions.checkArgument(!RelDistributions.ANY.getType().equals(traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType()));
        m_splitCount = splitCount;
        m_isTopExchange = topExchange;
    }

    @Override
    protected String computeDigest() {
        return super.computeDigest();
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    @Override
    public AbstractVoltDBPExchange copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelDistribution newDistribution) {
        return copyInternal(
                traitSet,
                newInput,
                isTopExchange());
    }

    public AbstractVoltDBPExchange copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelDistribution newDistribution,
            boolean isTopExchange) {
        return copyInternal(
                traitSet,
                newInput,
                isTopExchange);
    }

    protected abstract AbstractVoltDBPExchange copyInternal(
            RelTraitSet traitSet,
            RelNode newInput,
            boolean isTopExchang);

    public boolean isTopExchange() {
        return m_isTopExchange;
    }
}
