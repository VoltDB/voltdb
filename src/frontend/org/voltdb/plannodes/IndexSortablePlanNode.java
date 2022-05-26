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
package org.voltdb.plannodes;

/**
 * Scan nodes and join nodes may have ordering information
 * which we can use to avoid creating ORDERBY nodes, and so to
 * avoid unnecessary sorting.  All the information in these nodes
 * is carried in m_indexUse members.
 * The class hierarchy for AbstractJoinPlanNode
 * and IndexScanPlanNode define such a member to implement this.
 */
public interface IndexSortablePlanNode  {
    // Characterizations of the order provided by the underlying index
    IndexUseForOrderBy indexUse();
    // return "this" index scan or join plan node
    AbstractPlanNode planNode();
}
