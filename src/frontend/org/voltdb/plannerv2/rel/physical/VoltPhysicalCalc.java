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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;

/**
 * Sub-class of {@link Calc}
 * target at {@link #CONVENTION} convention
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPhysicalCalc extends Calc implements VoltPhysicalRel {

    private final int m_splitCount;

    public VoltPhysicalCalc(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program, int splitCount) {
        super(cluster, traitSet, input, program);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
        m_splitCount = splitCount;
    }

    @Override
    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program) {
        return new VoltPhysicalCalc(
                getCluster(),
                traitSet,
                child,
                program,
                m_splitCount);
    }

    public Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program, int splitCount) {
        return new VoltPhysicalCalc(
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

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        rowCount = PlanCostUtil.adjustRowCountOnRelDistribution(rowCount, getTraitSet());

        RelOptCost defaultCost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(rowCount, defaultCost.getCpu(), defaultCost.getIo());

    }

    @Override
    public AbstractPlanNode toPlanNode() {
        // Calc can be converted to the ProjectionPlanNode only if its program
        // contains Project fields and no condition (Filter)
        RexProgram program = getProgram();
        if (program.getCondition() != null) {
            // we always push the filter down and then inline the filter in the table scan.
            throw new CalcitePlanningException(
                    "VoltDBCalc(with Condition).toPlanNode is not implemented.");
        }
        AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(program);
        ProjectionPlanNode ppn = new ProjectionPlanNode(schema);
        ppn.addAndLinkChild(child);
        return ppn;
    }
}
