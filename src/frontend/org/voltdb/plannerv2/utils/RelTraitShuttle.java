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

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;

public class RelTraitShuttle extends RelShuttleImpl {

    private final RelTrait m_newTrait;

    public RelTraitShuttle(RelTrait newTrait) {
        m_newTrait = newTrait;
    }

    private <T extends RelNode> RelNode _visit(T visitor) {
        RelTraitSet newTraitSet = visitor.getTraitSet().plus(m_newTrait).simplify();
        RelNode newRel = visitor.copy(newTraitSet, visitor.getInputs());
        return visitChildren(newRel);
    }

    @Override public RelNode visit(TableScan scan) {
        return _visit(scan);
    }

    @Override public RelNode visit(RelNode other) {
        return _visit(other);
    }
}
