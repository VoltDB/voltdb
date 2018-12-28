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

package org.voltdb.plannerv2.rel.logical;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Limit operator with <code>VOLTDB_LOGICAL</code> convention trait.
 *
 * @author Michael Alexeev
 * @since 8.4
 */
public class VoltLLimit extends SingleRel implements VoltLogicalRel {

    private RexNode m_offset;
    private RexNode m_limit;

    public VoltLLimit(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode offset,
            RexNode limit) {
            super(cluster, traitSet, input);
            Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
            m_offset = offset;
            m_limit = limit;
        }

    public VoltLLimit copy(RelTraitSet traitSet, RelNode input,
                             RexNode offset, RexNode limit) {
        return new VoltLLimit(getCluster(), traitSet, input, offset, limit);
    }

    @Override
    public VoltLLimit copy(RelTraitSet traitSet,
                             List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), m_offset, m_limit);
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("limit", m_limit, m_limit != null)
                .itemIf("offset", m_offset, m_offset != null);
    }

}
