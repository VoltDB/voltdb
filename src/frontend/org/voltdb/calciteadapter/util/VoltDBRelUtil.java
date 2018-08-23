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

package org.voltdb.calciteadapter.util;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.util.mapping.Mappings;
import org.voltdb.calciteadapter.converter.RexConverter;

import com.google.common.collect.ImmutableList;

public class VoltDBRelUtil {

    /**
     * Add a new RelTrait to a RelNode and its descendants.
     * @param rel
     * @param newTrait
     * @return
     */
    public static RelNode addTraitRecurcively(RelNode rel, RelTrait newTrait) {
        assert(rel != null);
        RelTraitSuttle traitShuttle = new RelTraitSuttle(rel.getCluster().getPlanner(), newTrait);
        return rel.accept(traitShuttle);
    }

    private static class RelTraitSuttle extends RelShuttleImpl {

        private final RelTrait m_newTrait;

        public RelTraitSuttle(RelOptPlanner planner, RelTrait newTrait) {
            m_newTrait = newTrait;
        }

        @Override
        public RelNode visit(RelNode other) {
            RelTraitSet newTraitSet = other.getTraitSet().plus(m_newTrait);
            RelNode newRel = other.copy(newTraitSet, other.getInputs());
            return visitChildren(newRel);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            RelNode newAggregate = aggregate.copy(aggregate.getTraitSet().plus(m_newTrait), aggregate.getInputs());
            return visitChild(newAggregate, 0, aggregate.getInput());
        }

        @Override
        public RelNode visit(LogicalMatch match) {
            RelNode newMatch = match.copy(match.getTraitSet().plus(m_newTrait), match.getInputs());
            return visitChild(newMatch, 0, match.getInput());
        }

        @Override
        public RelNode visit(TableScan scan) {
            RelNode newScan = scan.copy(scan.getTraitSet().plus(m_newTrait), scan.getInputs());
            return newScan;
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
            RelNode newScan = scan.copy(scan.getTraitSet().plus(m_newTrait), scan.getInputs());
            return visitChildren(newScan);
        }

        @Override
        public RelNode visit(LogicalValues values) {
            RelNode newValues = values.copy(values.getTraitSet().plus(m_newTrait), values.getInputs());
            return newValues;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RelNode newFilter = filter.copy(filter.getTraitSet().plus(m_newTrait), filter.getInputs());
            return visitChild(newFilter, 0, filter.getInput());
        }

        @Override
        public RelNode visit(LogicalProject project) {
            RelNode newProject = project.copy(project.getTraitSet().plus(m_newTrait), project.getInputs());
            return visitChild(newProject, 0, project.getInput());
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            RelNode newJoin = join.copy(join.getTraitSet().plus(m_newTrait), join.getInputs());
            return visitChildren(newJoin);
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            RelNode newCorrelate = correlate.copy(correlate.getTraitSet().plus(m_newTrait), correlate.getInputs());
            return visitChildren(newCorrelate);
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            RelNode newUnion = union.copy(union.getTraitSet().plus(m_newTrait), union.getInputs());
            return visitChildren(newUnion);
        }

        public RelNode visit(LogicalIntersect intersect) {
            RelNode newIntersect = intersect.copy(intersect.getTraitSet().plus(m_newTrait), intersect.getInputs());
            return visitChildren(newIntersect);
        }

        @Override
        public RelNode visit(LogicalMinus minus) {
            RelNode newMinus = minus.copy(minus.getTraitSet().plus(m_newTrait), minus.getInputs());
            return visitChildren(newMinus);
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            RelNode newSort = sort.copy(sort.getTraitSet().plus(m_newTrait), sort.getInputs());
            return visitChildren(newSort);
        }

        @Override
        public RelNode visit(LogicalExchange exchange) {
            RelNode newExchange = exchange.copy(exchange.getTraitSet().plus(m_newTrait), exchange.getInputs());
            return visitChildren(newExchange);
        }

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
}
