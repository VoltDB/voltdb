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

package org.voltdb.newplanner.util;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.SingleRel;
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
import org.voltdb.calciteadapter.converter.RexConverter;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.SendPlanNode;

public class VoltDBRelUtil {

    /**
     * Add a new RelTrait to a RelNode and its descendants.
     *
     * @param rel
     * @param newTrait
     * @return
     */
    public static RelNode addTraitRecurcively(RelNode rel, RelTrait newTrait) {
        Preconditions.checkNotNull(rel);
        RelTraitShuttle traitShuttle = new RelTraitShuttle(newTrait);
        return rel.accept(traitShuttle);
    }

    public static int decideSplitCount(RelNode rel) {
        return (rel instanceof VoltDBPRel) ?
                ((VoltDBPRel) rel).getSplitCount() : 1;
    }

    public static CompiledPlan calciteToVoltDBPlan(VoltDBPRel rel, CompiledPlan compiledPlan) {

        RexConverter.resetParameterIndex();

        AbstractPlanNode root = rel.toPlanNode();
        assert (root instanceof SendPlanNode);

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

    private static class RelTraitShuttle extends RelShuttleImpl {

        private final RelTrait m_newTrait;

        public RelTraitShuttle(RelTrait newTrait) {
            m_newTrait = newTrait;
        }

        private <T extends RelNode> RelNode internal_visit(T visitor) {
            RelNode newRel = visitor.copy(visitor.getTraitSet().plus(m_newTrait), visitor.getInputs());
            return visitChildren(newRel);
        }

        @Override
        public RelNode visit(RelNode other) {
            RelTraitSet newTraitSet = other.getTraitSet().plus(m_newTrait);
            RelNode newRel = other.copy(newTraitSet, other.getInputs());
            return visitChildren(newRel);
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            return internal_visit(aggregate);
        }

        @Override
        public RelNode visit(LogicalMatch match) {
            return internal_visit(match);
        }

        @Override
        public RelNode visit(TableScan scan) {
            RelNode newScan = scan.copy(scan.getTraitSet().plus(m_newTrait), scan.getInputs());
            return newScan;
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
            return internal_visit(scan);
        }

        @Override
        public RelNode visit(LogicalValues values) {
            RelNode newValues = values.copy(values.getTraitSet().plus(m_newTrait), values.getInputs());
            return newValues;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            return internal_visit(filter);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            return internal_visit(project);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return internal_visit(join);
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            return internal_visit(correlate);
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            return internal_visit(union);
        }

        public RelNode visit(LogicalIntersect intersect) {
            return internal_visit(intersect);
        }

        @Override
        public RelNode visit(LogicalMinus minus) {
            return internal_visit(minus);
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            return internal_visit(sort);
        }

        @Override
        public RelNode visit(LogicalExchange exchange) {
            return internal_visit(exchange);
        }
    }
}
