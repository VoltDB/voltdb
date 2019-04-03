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
import com.google_voltpatches.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

import java.util.List;

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
        RelTraitShuttle traitShuttle = new RelTraitShuttle(newTrait);
        return rel.accept(traitShuttle);
    }

    public static int decideSplitCount(RelNode rel) {
        return (rel instanceof VoltPhysicalRel) ?
                ((VoltPhysicalRel) rel).getSplitCount() : 1;
    }

    /**
     * Transform sort's collation pushing it through a Calc.
     * If the collationsort relies on non-trivial expressions we can't transform and
     * and return an empty collation
     *
     * @param sortRel
     * @param calcRel
     * @return
     */
    public static RelCollation sortCollationCalcTranspose(RelCollation collation, Calc calc) {
        final RexProgram program = calc.getProgram();
        final RelOptCluster cluster = calc.getCluster();

        List<RexLocalRef> projectRefList = program.getProjectList();
        List<RexNode> projectList = RexConverter.expandLocalRef(projectRefList, program);
        final Mappings.TargetMapping map =
                RelOptUtil.permutationIgnoreCast(
                        projectList, calc.getInput().getRowType());
        for (RelFieldCollation fc : collation.getFieldCollations()) {
            if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
                return RelCollations.EMPTY;
            }
            final RexNode node = projectList.get(fc.getFieldIndex());
            if (node.isA(SqlKind.CAST)) {
                // Check whether it is a monotonic preserving cast, otherwise we cannot push
                final RexCall cast = (RexCall) node;
                final RexCallBinding binding =
                        RexCallBinding.create(cluster.getTypeFactory(), cast,
                                ImmutableList.of(RelCollations.of(RexUtil.apply(map, fc))));
                if (cast.getOperator().getMonotonicity(binding) == SqlMonotonicity.NOT_MONOTONIC) {
                    return RelCollations.EMPTY;
                }
            }
        }

        final RelCollation newCollation =
                cluster.traitSet().canonize(
                        RexUtil.apply(map, collation));
        return newCollation;
    }

    public static CompiledPlan calciteToVoltDBPlan(VoltPhysicalRel rel, CompiledPlan compiledPlan) {

        RexConverter.PARAM_COUNTER.reset();

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
