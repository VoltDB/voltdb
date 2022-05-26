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

package org.voltdb.plannerv2.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * Relational expressions that uses the VoltDB calling convention.
 */
public interface VoltLogicalRel extends RelNode  {
    /**
     * Note - ethan - 12/29/2018 - Why is this necessary?
     * The default convention is Convention.NONE.
     * It means that a relational expression cannot be implemented; typically there
     * are rules which can transform it to equivalent, implementable expressions.
     * In {@link VolcanoPlanner#getCost(RelNode, RelMetadataQuery)},
     * you can see that if the convention is NONE, the relational node
     * will be made infinite cost. The planner will fail to come up
     * with a plan with best cost.
     */
    Convention CONVENTION = new Convention.Impl("CONVENTION", VoltLogicalRel.class);
}
