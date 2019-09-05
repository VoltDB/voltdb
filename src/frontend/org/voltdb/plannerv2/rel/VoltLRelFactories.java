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
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalIntersect;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalMinus;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalSort;
import org.voltdb.plannerv2.rel.logical.VoltLogicalUnion;
import org.voltdb.plannerv2.rel.logical.VoltLogicalValues;

import com.google.common.collect.ImmutableList;
import com.google_voltpatches.common.base.Preconditions;

/**
 * Contains implementation for creating various VoltLogical nodes.
 *
 */
public class VoltLRelFactories {

    public static ProjectFactory VOLT_LOGICAL_PROJECT_FACTORY =
        new VoltLProjectFactoryImpl();

    public static FilterFactory VOLT_LOGICAL_FILTER_FACTORY =
            new VoltLFilterFactoryImpl();

    public static final JoinFactory VOLT_LOGICAL_JOIN_FACTORY =
            new VoltLJoinFactoryImpl();

    public static final SortFactory VOLT_LOGICAL_SORT_FACTORY =
            new VoltLSortFactoryImpl();

    public static final AggregateFactory VOLT_LOGICAL_AGGREGATE_FACTORY =
            new VoltLAggregateFactoryImpl();

    public static final SetOpFactory VOLT_LOGICAL_SET_OP_FACTORY =
            new VoltLSetOpFactoryImpl();

    public static final ValuesFactory VOLT_LOGICAL_VALUES_FACTORY =
            new VoltPValuesFactoryImpl();

    // @TODO Scan Factory

    /**
     * Implementation of ProjectFactory
     * {@link org.apache.calcite.rel.core.RelFactories.ProjectFactory}
     * that returns a VoltPhysicalCalc
     * {@link org.voltdb.plannerv2.rel.logical.VoltLogicalCalc}.
     */
    private static class VoltLProjectFactoryImpl implements ProjectFactory {

        public RelNode createProject(RelNode input,
                List<? extends RexNode> childExprs, List<String> fieldNames) {

            final RelDataType outputRowType =
                    RexUtil.createStructType(input.getCluster().getTypeFactory(), childExprs,
                        fieldNames, SqlValidatorUtil.F_SUGGESTER);

            final RexProgram program =
                    RexProgram.create(
                            input.getRowType(),
                            childExprs,
                            null,
                            outputRowType,
                            input.getCluster().getRexBuilder());

            return new VoltLogicalCalc(input.getCluster(),
                    input.getTraitSet().replace(VoltLogicalRel.CONVENTION),
                    input,
                    program);
        }
    }

    /**
     * Implementation of {@link RelFactories.FilterFactory} that
     * that returns a VoltPhysicalCalc
     * {@link org.voltdb.plannerv2.rel.logical.VoltLogicalCalc}.
     */
    private static class VoltLFilterFactoryImpl implements FilterFactory {
        public RelNode createFilter(RelNode input, RexNode condition) {
            // Create a program containing a filter.
            final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
            final RelDataType inputRowType = input.getRowType();
            final RexProgramBuilder programBuilder =
                new RexProgramBuilder(inputRowType, rexBuilder);
            programBuilder.addIdentity();
            programBuilder.addCondition(condition);
            final RexProgram program = programBuilder.getProgram();
            return new VoltLogicalCalc(input.getCluster(),
                    input.getTraitSet().replace(VoltLogicalRel.CONVENTION),
                    input,
                    program);
        }
    }

    /**
     * Implementation of {@link JoinFactory} that returns a VoltPhysicalNestLoopJoin
     * {@link org.voltdb.plannerv2.rel.logical.VoltLogicalJoin}.
     */
    private static class VoltLJoinFactoryImpl implements JoinFactory {
      public RelNode createJoin(RelNode left, RelNode right,
          RexNode condition, Set<CorrelationId> variablesSet,
          JoinRelType joinType, boolean semiJoinDone) {
          return new VoltLogicalJoin(
                  left.getCluster(),
                  left.getTraitSet().replace(VoltLogicalRel.CONVENTION),
                  left,
                  right,
                  condition,
                  variablesSet,
                  joinType,
                  semiJoinDone,
                  ImmutableList.of()
                  );
      }

      public RelNode createJoin(RelNode left, RelNode right, RexNode condition,
          JoinRelType joinType, Set<String> variablesStopped,
          boolean semiJoinDone) {
        return createJoin(left, right, condition,
            CorrelationId.setOf(variablesStopped), joinType, semiJoinDone);
      }
    }

    /**
     * Implementation of {@link RelFactories.SortFactory} that
     * returns a vanilla {@link org.voltdb.plannerv2.rel.logical.VoltLogicalSort}.
     */
    private static class VoltLSortFactoryImpl implements SortFactory {
        public RelNode createSort(RelNode input, RelCollation collation,
                RexNode offset, RexNode fetch) {
            return new VoltLogicalSort(
                    input.getCluster(),
                    input.getTraitSet().plus(collation).replace(VoltLogicalRel.CONVENTION),
                    input,
                    collation);
        }

        @Deprecated // to be removed before 2.0
        public RelNode createSort(RelTraitSet traits, RelNode input,
                RelCollation collation, RexNode offset, RexNode fetch) {
            return createSort(input, collation, offset, fetch);
        }
    }

    /**
     * Implementation of {@link RelFactories.AggregateFactory}
     * that returns a VoltPhysicalHashAggregate
     * {@link org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate}.
     */
    private static class VoltLAggregateFactoryImpl implements AggregateFactory {
        @SuppressWarnings("deprecation")
        public RelNode createAggregate(RelNode input, boolean indicator,
                ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls) {
            return new VoltLogicalAggregate(
                    input.getCluster(),
                    // Aggregation destroys ORDERING
                    input.getTraitSet().replace(RelCollations.EMPTY).replace(VoltLogicalRel.CONVENTION),
                    input,
                    groupSet,
                    groupSets,
                    aggCalls);
        }
    }

    /**
     * Implementation of {@link RelFactories.SetOpFactory} that
     * returns a {@link org.voltdb.plannerv2.rel.logical.VoltLogicalUnion}
     * or a {@link org.voltdb.plannerv2.rel.logical.VoltLogicalMinus}
     * or a {@link org.voltdb.plannerv2.rel.logical.VoltLogicalIntersect}
     * for the particular kind of set operation (UNION, EXCEPT, INTERSECT).
     */
    private static class VoltLSetOpFactoryImpl implements SetOpFactory {
        public RelNode createSetOp(SqlKind kind, List<RelNode> inputs,
                boolean all) {
            Preconditions.checkArgument(!inputs.isEmpty());
            RelNode firstChild = inputs.get(0);
            RelTraitSet traits = firstChild.getTraitSet()
                    .replace(RelCollations.EMPTY).replace(VoltLogicalRel.CONVENTION);
            switch (kind) {
            case UNION:
                return new VoltLogicalUnion(
                        firstChild.getCluster(),
                        traits,
                        inputs,
                        all);
            case EXCEPT:
                return new VoltLogicalMinus(
                        firstChild.getCluster(),
                        traits,
                        inputs,
                        all);
            case INTERSECT:
                return new VoltLogicalIntersect(
                        firstChild.getCluster(),
                        traits,
                        inputs,
                        all);
            default:
                throw new AssertionError("not a set op: " + kind);
            }
        }
    }

    /**
     * Implementation of {@link ValuesFactory} that returns a
     * {@link VoltLogicalValues}.
     */
    private static class VoltPValuesFactoryImpl implements ValuesFactory {
        public RelNode createValues(RelOptCluster cluster, RelDataType rowType,
                List<ImmutableList<RexLiteral>> tuples) {
            RelTraitSet traitSet = cluster.traitSet()
                    .plus(VoltLogicalRel.CONVENTION)
                    .plus(RelCollations.EMPTY)
                    .plus(RelDistributions.ANY);
            return new VoltLogicalValues(
                    cluster,
                    traitSet,
                    rowType,
                    ImmutableList.copyOf(tuples));
        }
    }

}
