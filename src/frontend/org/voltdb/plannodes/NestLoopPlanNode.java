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

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
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
                                     Cluster cluster,
                                     Database db,
                                     DatabaseEstimates estimates,
                                     ScalarValueHints[] paramHints)
    {
        // This method doesn't do anything besides what the parent method does,
        // but it is a nice place to put a comment.
        // Since both children's' cost get included in the costing, this
        // already mirrors the kind of estimating we do in a nestloopjoin.

        m_estimatedOutputTupleCount = childOutputTupleCountEstimate;
        m_estimatedProcessedTupleCount = childOutputTupleCountEstimate;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "NEST LOOP " + this.m_joinType.toString() + " JOIN" +
                (m_sortDirection == SortDirectionType.INVALID ? "" : " (" + m_sortDirection + ")") +
                explainFilters(indent);
    }

}
