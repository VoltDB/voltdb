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

package org.voltdb.plannerv2.rel;

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.AggregateFactory;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.RelFactories.JoinFactory;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.core.RelFactories.SetOpFactory;
import org.apache.calcite.rel.core.RelFactories.SortFactory;
import org.apache.calcite.rel.core.RelFactories.ValuesFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalHashAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalIntersect;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalMinus;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSort;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalUnion;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalValues;

import com.google.common.collect.ImmutableList;
import com.google_voltpatches.common.base.Preconditions;

/**
 * Contains implementation for creating various VoltPhysical nodes.
 *
 */
class VoltPRelFactories {

    /**
     * Implementation of ProjectFactory
     * {@link org.apache.calcite.rel.core.RelFactories.ProjectFactory}
     * that returns a VoltPhysicalCalc
     * {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc}.
     */
    static ProjectFactory VOLT_PHYSICAL_PROJECT_FACTORY =
            (RelNode input, List<? extends RexNode> childExprs, List<String> fieldNames) -> {
                final RelDataType outputRowType =
                        RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs,
                                fieldNames, SqlValidatorUtil.F_SUGGESTER);
                final RexProgram program =
                        RexProgram.create(input.getRowType(), childExprs, null,
                                outputRowType, input.getCluster().getRexBuilder());
                return new VoltPhysicalCalc(input.getCluster(),
                        input.getTraitSet().replace(VoltPhysicalRel.CONVENTION),
                        input, program);
            };

    /**
     * Implementation of {@link RelFactories.FilterFactory} that
     * that returns a VoltPhysicalCalc
     * {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc}.
     */
    static FilterFactory VOLT_PHYSICAL_FILTER_FACTORY = (RelNode input, RexNode condition) -> {
        // Create a program containing a filter.
        final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
        final RelDataType inputRowType = input.getRowType();
        final RexProgramBuilder programBuilder =
                new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        programBuilder.addCondition(condition);
        final RexProgram program = programBuilder.getProgram();
        return new VoltPhysicalCalc(input.getCluster(),
                input.getTraitSet().replace(VoltPhysicalRel.CONVENTION),
                input, program);
    };

    /**
     * Implementation of {@link JoinFactory} that returns a VoltPhysicalNestLoopJoin
     * {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopJoin}.
     */
    static final JoinFactory VOLT_PHYSICAL_JOIN_FACTORY = new JoinFactory() {
        @Override
        public RelNode createJoin(RelNode left, RelNode right, RexNode condition, Set<CorrelationId> variablesSet,
                                  JoinRelType joinType, boolean semiJoinDone) {
            return new VoltPhysicalNestLoopJoin(left.getCluster(),
                    left.getTraitSet().replace(VoltPhysicalRel.CONVENTION),
                    left, right, condition, variablesSet,
                    joinType, semiJoinDone, ImmutableList.of());
        }
        @Override @Deprecated
        public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
                                  JoinRelType joinType, Set<String> variablesStopped, boolean semiJoinDone) {
            return createJoin(left, right, condition, CorrelationId.setOf(variablesStopped), joinType, semiJoinDone);
        }
    };

    /**
     * Implementation of {@link RelFactories.SortFactory} that
     * returns a vanilla {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalSort}.
     */
    static final SortFactory VOLT_PHYSICAL_SORT_FACTORY = new SortFactory() {
        @Override
        public RelNode createSort(RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
            return new VoltPhysicalSort(input.getCluster(),
                    input.getTraitSet().plus(collation).replace(VoltPhysicalRel.CONVENTION),
                    input, collation, false);
        }
        @Override @Deprecated // to be removed before 2.0
        public RelNode createSort(
                RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
            return createSort(input, collation, offset, fetch);
        }
    };

    /**
     * Implementation of {@link RelFactories.AggregateFactory}
     * that returns a VoltPhysicalHashAggregate
     * {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalHashAggregate}.
     */
    static final AggregateFactory VOLT_PHYSICAL_AGGREGATE_FACTORY =
            (RelNode input, boolean indicator, ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
             List<AggregateCall> aggCalls) -> new VoltPhysicalHashAggregate(
                            input.getCluster(),
                            // Hash destroys ORDERING
                            input.getTraitSet().replace(RelCollations.EMPTY).replace(VoltPhysicalRel.CONVENTION),
                            input, indicator, groupSet, groupSets, aggCalls,
                    null, false);

    /**
     * Implementation of {@link RelFactories.SetOpFactory} that
     * returns a {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalUnion}
     * or a {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalMinus}
     * or a {@link org.voltdb.plannerv2.rel.physical.VoltPhysicalIntersect}
     * for the particular kind of set operation (UNION, EXCEPT, INTERSECT).
     */
    static final SetOpFactory VOLT_PHYSICAL_SET_OP_FACTORY = (SqlKind kind, List<RelNode> inputs, boolean all) -> {
        Preconditions.checkArgument(!inputs.isEmpty());
        final RelNode firstChild = inputs.get(0);
        final RelTraitSet traits = firstChild.getTraitSet()
                .replace(RelCollations.EMPTY)
                .replace(VoltPhysicalRel.CONVENTION);
        switch (kind) {
            case UNION:
                return new VoltPhysicalUnion(firstChild.getCluster(), traits, inputs, all);
            case EXCEPT:
                return new VoltPhysicalMinus(firstChild.getCluster(), traits, inputs, all);
            case INTERSECT:
                return new VoltPhysicalIntersect(firstChild.getCluster(), traits, inputs, all);
            default:
                throw new AssertionError("not a set op: " + kind);
        }
    };

    /**
     * Implementation of {@link ValuesFactory} that returns a
     * {@link VoltPhysicalValues}.
     */
    static final ValuesFactory VOLT_PHYSICAL_VALUES_FACTORY =
            (RelOptCluster cluster, RelDataType rowType, List<ImmutableList<RexLiteral>> tuples) ->
                    new VoltPhysicalValues(cluster,
                            cluster.traitSet()
                                    .plus(VoltPhysicalRel.CONVENTION)
                                    .plus(RelCollations.EMPTY)
                                    .plus(RelDistributions.ANY),
                            rowType, ImmutableList.copyOf(tuples));

    // @TODO Scan Factory
}
