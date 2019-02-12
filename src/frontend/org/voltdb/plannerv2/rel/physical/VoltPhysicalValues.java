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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

public class VoltPhysicalValues extends LogicalValues implements VoltPhysicalRel {
    private final int m_splitCount;

    public VoltPhysicalValues(RelOptCluster cluster,
                              RelTraitSet traitSet,
                              RelDataType rowType,
                              ImmutableList<ImmutableList<RexLiteral>> tuples,
                              int splitCount) {
        super(cluster, traitSet, rowType, tuples);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
        m_splitCount = splitCount;
    }

    @Override
    public VoltPhysicalValues copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VoltPhysicalValues(getCluster(),
                traitSet, rowType, tuples, m_splitCount);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
                .item("split", m_splitCount);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }
}
