/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.List;

import org.voltdb.types.PlanNodeType;

/**
 * Plan node representing an Aggregate with a Partial Serial based implementation of grouping.
 *
 */
public class PartialAggregatePlanNode extends AggregatePlanNode {
    public PartialAggregatePlanNode() {
        super();
    }

    public PartialAggregatePlanNode(List<Integer> partialGroupByColumns) {
        super();
        setPartialAggregateColumns(partialGroupByColumns);
    }

    public PartialAggregatePlanNode(PartialAggregatePlanNode origin) {
        super();
    }

    public void setPartialAggregateColumns(List<Integer> partialGroupByColumns) {
        m_partialGroupByColumns = partialGroupByColumns;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PARTIALAGGREGATE;
    }

}
