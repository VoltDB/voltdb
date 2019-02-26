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

package org.voltdb.plannerv2.utils;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

public class VoltRelUtil {

    /**
     * Add a new RelTrait to a RelNode and its descendants.
     *
     * @param rel
     * @param newTrait
     * @return
     */
    public static RelNode addTraitRecursively(RelNode rel, RelTrait newTrait) {
        Preconditions.checkNotNull(rel);
        if (newTrait instanceof RelDistribution) {  // cleanse partition value left over from RelDistributions.ANY
            ((RelDistribution) newTrait).setPartitionEqualValue(null);
        }
        RelTraitShuttle traitShuttle = new RelTraitShuttle(newTrait);
        return rel.accept(traitShuttle);
    }

    public static int decideSplitCount(RelNode rel) {
        return (rel instanceof VoltPhysicalRel) ?
                ((VoltPhysicalRel) rel).getSplitCount() : 1;
    }

    public static CompiledPlan calciteToVoltDBPlan(VoltPhysicalRel rel, CompiledPlan compiledPlan) {

        RexConverter.resetParameterIndex();

        AbstractPlanNode root = new SendPlanNode();
        root.addAndLinkChild(rel.toPlanNode());

        compiledPlan.rootPlanGraph = root;

        PostBuildVisitor postPlannerVisitor = new PostBuildVisitor();
        root.acceptVisitor(postPlannerVisitor);

        compiledPlan.setReadOnly(true);
        compiledPlan.statementGuaranteesDeterminism(
                postPlannerVisitor.hasLimitOffset(), // no limit or offset
                postPlannerVisitor.isOrderDeterministic(),  // is order deterministic
                null); // no details on determinism

        compiledPlan.setStatementPartitioning(StatementPartitioning.forceSP());

        compiledPlan.setParameters(postPlannerVisitor.getParameterValueExpressions());

        return compiledPlan;
    }
}
