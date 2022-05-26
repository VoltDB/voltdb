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
package org.voltdb.types;

import org.voltdb.plannodes.AbstractPlanNode;

/**
 * A class which can match an abstract plan node.
 *
 * PlanNodeType implements this, so plan nodes of
 * a given type can be matched.
 *
 * @author bwhite
 *
 */
public interface PlanMatcher {
    /**
     * Match an AbstractPlanNode.  Return null for a match
     * and an error message otherwise.
     *
     * @param node
     * @return
     */
    String match(AbstractPlanNode node);
}
