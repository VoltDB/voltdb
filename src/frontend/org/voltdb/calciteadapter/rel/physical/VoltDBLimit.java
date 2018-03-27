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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;

public class VoltDBLimit extends SingleRel implements VoltDBPhysicalRel {

    private RexNode m_offset;
    private RexNode m_limit;

    public VoltDBLimit(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode offset,
            RexNode limit) {
            super(cluster, traitSet, input);
            assert traitSet.contains(VoltDBPhysicalRel.VOLTDB_PHYSICAL);
            m_offset = offset;
            m_limit = limit;
        }

    public VoltDBLimit copy(RelTraitSet traitSet, RelNode input,
            RexNode offset, RexNode limit) {
        return new VoltDBLimit(getCluster(), traitSet, input, offset, limit);
    }

    @Override
    public VoltDBLimit copy(RelTraitSet traitSet,
            List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), m_offset, m_limit);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        LimitPlanNode lpn = new LimitPlanNode();
        lpn = new LimitPlanNode();
        if (m_limit != null) {
            lpn.setLimit(RexLiteral.intValue(m_limit));
        }
        if (m_offset != null) {
            lpn.setLimit(RexLiteral.intValue(m_offset));
        }
        return lpn;
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (m_limit != null) {
            pw.item("limit", m_limit);
        }
        if (m_offset != null) {
            pw.item("offset", m_offset);
        }
        return pw;
    }

}
