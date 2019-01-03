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

package org.voltdb.plannerv2.utils;

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
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

public class RelTraitShuttle extends RelShuttleImpl {

    private final RelTrait m_newTrait;

    public RelTraitShuttle(RelTrait newTrait) {
        m_newTrait = newTrait;
    }

    private <T extends RelNode> RelNode _visit(T visitor) {
        RelNode newRel = visitor.copy(visitor.getTraitSet().plus(m_newTrait), visitor.getInputs());
        return visitChildren(newRel);
    }

    @Override public RelNode visit(RelNode other) {
        RelTraitSet newTraitSet = other.getTraitSet().plus(m_newTrait);
        RelNode newRel = other.copy(newTraitSet, other.getInputs());
        return visitChildren(newRel);
    }

    @Override public RelNode visit(LogicalAggregate aggregate) {
        return _visit(aggregate);
    }

    @Override public RelNode visit(LogicalMatch match) {
        return _visit(match);
    }

    @Override public RelNode visit(TableScan scan) {
        RelNode newScan = scan.copy(scan.getTraitSet().plus(m_newTrait), scan.getInputs());
        return newScan;
    }

    @Override public RelNode visit(TableFunctionScan scan) {
        return _visit(scan);
    }

    @Override public RelNode visit(LogicalValues values) {
        RelNode newValues = values.copy(values.getTraitSet().plus(m_newTrait), values.getInputs());
        return newValues;
    }

    @Override public RelNode visit(LogicalFilter filter) {
        return _visit(filter);
    }

    @Override public RelNode visit(LogicalProject project) {
        return _visit(project);
    }

    @Override public RelNode visit(LogicalJoin join) {
        return _visit(join);
    }

    @Override public RelNode visit(LogicalCorrelate correlate) {
        return _visit(correlate);
    }

    @Override public RelNode visit(LogicalUnion union) {
        return _visit(union);
    }

    @Override public RelNode visit(LogicalIntersect intersect) {
        return _visit(intersect);
    }

    @Override public RelNode visit(LogicalMinus minus) {
        return _visit(minus);
    }

    @Override public RelNode visit(LogicalSort sort) {
        return _visit(sort);
    }

    @Override public RelNode visit(LogicalExchange exchange) {
        return _visit(exchange);
    }
}
