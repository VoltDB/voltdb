/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.plannodes;

import java.util.ArrayList;

import org.voltdb.catalog.Database;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.*;

public class NestLoopIndexPlanNode extends AbstractJoinPlanNode {

    public NestLoopIndexPlanNode(PlannerContext context) {
        super(context);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.NESTLOOPINDEX;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrayList<Integer> createOutputColumns(Database db, ArrayList<Integer> input) {
        IndexScanPlanNode inlineScan = (IndexScanPlanNode) m_inlineNodes.get(PlanNodeType.INDEXSCAN);
        assert(inlineScan != null);

        inlineScan.updateOutputColumns(db);
        input.addAll(inlineScan.m_outputColumns);

        return (ArrayList<Integer>) input.clone();
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Check that we have an inline IndexScanPlanNode
        if (m_inlineNodes.isEmpty()) {
            throw new Exception("ERROR: No inline PlanNodes are set for " + this);
        } else if (!m_inlineNodes.containsKey(PlanNodeType.INDEXSCAN)) {
            throw new Exception("ERROR: No inline PlanNode with type '" + PlanNodeType.INDEXSCAN + "' was set for " + this);
        }
    }
}
