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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Intersect;

import com.google.common.base.Preconditions;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Intersect} targeted at the VoltDB physical calling convention.
 *
 * @author Mike Alexeev
 * @since 9.0
 */
public class VoltPhysicalIntersect extends Intersect implements VoltPhysicalRel {

    private final int m_splitCount;

    /**
     * Creates a VoltPhysicalIntersect.
     *
     * @param cluster          Cluster
     * @param traitSet         Trait set
     * @param inputs           inputs
     * @param all              SetOps ALL qualifier
     */
    public VoltPhysicalIntersect(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelNode> inputs,
            boolean all,
            int splitCount) {
        super(cluster, traitSet, inputs, all);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
        m_splitCount = splitCount;
    }

    @Override public VoltPhysicalIntersect copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new VoltPhysicalIntersect(getCluster(), traitSet, inputs, all, m_splitCount);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

}
