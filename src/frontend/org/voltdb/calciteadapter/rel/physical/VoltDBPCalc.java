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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;

public class VoltDBPCalc extends Calc implements VoltDBPRel {

    private final int m_splitCount;

    public VoltDBPCalc(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexProgram program,
            int splitCount) {
            super(cluster, traitSet, input, program);
            assert VoltDBPRel.VOLTDB_PHYSICAL.equals(getConvention());
            m_splitCount = splitCount;
        }

    @Override
    public AbstractPlanNode toPlanNode() {
        // Calc can be converted to the ProjectionPlanNode only if its program
        // contains Project fields and no condition (Filter)
        RexProgram program = getProgram();
        if (program.getCondition() != null) {
            throw new IllegalStateException(
                    "VoltDBCalc(with Condition).toPlanNode is not implemented.");
        }
        AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(program);
        ProjectionPlanNode ppn = new ProjectionPlanNode(schema);
        ppn.addAndLinkChild(child);
        return ppn;
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new VoltDBPCalc(
                getCluster(),
                traitSet,
                child,
                program,
                m_splitCount);
    }

    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program, int splitCount) {
        return new VoltDBPCalc(
                getCluster(),
                traitSet,
                child,
                program,
                splitCount);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_split_" + m_splitCount;
        return digest;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        return pw;
    }

}
