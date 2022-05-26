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

import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

public class NestLoopPlanNode extends AbstractJoinPlanNode {

    public NestLoopPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOP;
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate,
                                     DatabaseEstimates estimates,
                                     ScalarValueHints[] paramHints) {

        m_estimatedOutputTupleCount = childOutputTupleCountEstimate;
        // Discount outer child estimates based on the number of its filters
        assert(m_children.size() == 2);
        m_estimatedProcessedTupleCount = discountEstimatedProcessedTupleCount(m_children.get(0)) +
                m_children.get(1).m_estimatedProcessedTupleCount;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "NEST LOOP " + this.m_joinType.toString() + " JOIN" +
                (m_sortDirection == SortDirectionType.INVALID ? "" : " (" + m_sortDirection + ")") +
                explainFilters(indent);
    }

}
