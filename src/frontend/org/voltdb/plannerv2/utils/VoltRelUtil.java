/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SendPlanNode;

import com.google.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableList;

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

        AbstractPlanNode root = rel.toPlanNode();
        // if the root has a distribution other than SINGLETON
        // and the partitioning value is not set
        // we need to add an additional Receive / Send pair to collect
        // partitions results
        RelDistribution rootDist = rel.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        if (RelDistributions.SINGLETON.getType() != rootDist.getType()
                && rootDist.getPartitionEqualValue() == null) {
            SendPlanNode fragmentSend = new SendPlanNode();
            fragmentSend.addAndLinkChild(root);
            root = new ReceivePlanNode();
            root.setOutputSchema(RexConverter.convertToVoltDBNodeSchema(rel.getRowType(), 0));
            root.setHaveSignificantOutputSchema(true);
            root.addAndLinkChild(fragmentSend);
        }
        // Add final Send node to be the root
        SendPlanNode coordinatorSend = new SendPlanNode();
        coordinatorSend.addAndLinkChild(root);
        root = coordinatorSend;

        compiledPlan.rootPlanGraph = root;

        PostBuildVisitor postPlannerVisitor = new PostBuildVisitor();
        root.acceptVisitor(postPlannerVisitor);

        compiledPlan.setReadOnly(true);
        compiledPlan.statementGuaranteesDeterminism(
                postPlannerVisitor.hasLimitOffset(), // no limit or offset
                postPlannerVisitor.isOrderDeterministic(),  // is order deterministic
                null); // no details on determinism

        compiledPlan.setStatementPartitioning(StatementPartitioning.inferPartitioning());

        compiledPlan.setParameters(postPlannerVisitor.getParameterValueExpressions());

        compiledPlan.setPartitioningValue(VoltRexUtil.extractPartitioningValue(rootDist));

        return compiledPlan;
    }

    /**
     * Given a coordinator's LIMIT node build a fragment's LIMIT node
     *
     * @param coordinatorLimit
     * @param fragmentDist
     * @param fragmentNode
     * @return
     */
    public static Optional<VoltPhysicalLimit> buildFragmentLimit(VoltPhysicalLimit coordinatorLimit, RelDistribution fragmentDist, RelNode fragmentNode) {
        RexNode limit = coordinatorLimit.getLimit();
        RexNode offset = coordinatorLimit.getOffset();
        // Can't push OFFEST only to fragments
        if (limit == null) {
            return Optional.empty();
        }

        VoltPhysicalLimit fragmentLimit = null;
        if (offset == null) {
            // Simply push the limit to fragments
            fragmentLimit = new VoltPhysicalLimit(
                    coordinatorLimit.getCluster(),
                    coordinatorLimit.getTraitSet().replace(fragmentDist),
                    fragmentNode,
                    null, limit, false);
        } else {
            // fragment's limit is coordinator's limit plus coordinator's offset
            // fragment's offset is always 0
            final RexNode combinedLimit;
            RexBuilder builder = coordinatorLimit.getCluster().getRexBuilder();

            if (RexUtil.isLiteral(limit, true) && RexUtil.isLiteral(offset, true)) {
                int intLimit = RexLiteral.intValue(limit);
                int intOffset = RexLiteral.intValue(offset);
                combinedLimit = builder.makeLiteral(intLimit + intOffset, limit.getType(), true);
            } else {
                combinedLimit = builder.makeCall(
                        SqlStdOperatorTable.PLUS,
                        offset,
                        limit);
            }
            fragmentLimit = new VoltPhysicalLimit(
                    coordinatorLimit.getCluster(), coordinatorLimit.getTraitSet().replace(fragmentDist), fragmentNode,
                    null, combinedLimit, false);
        }
        return Optional.of(fragmentLimit);
    }
}
