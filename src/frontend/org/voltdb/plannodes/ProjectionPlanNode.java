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

import java.util.*;
import org.json.JSONException;
import org.json.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.*;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.planner.PlannerContext;
import org.voltdb.types.*;

public class ProjectionPlanNode extends AbstractPlanNode {

    public ProjectionPlanNode(PlannerContext context) {
        super(context);
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.PROJECTION;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        // Validate Expression Trees
        for (int ctr = 0; ctr < m_outputColumns.size(); ctr++) {
            PlanColumn column = m_context.get(m_outputColumns.get(ctr));
            AbstractExpression exp = column.getExpression();
            if (exp == null) {
                throw new Exception("ERROR: The Output Column Expression at position '" + ctr + "' is NULL");
            }
            exp.validate();
        }
    }

    public void appendOutputColumn(PlanColumn column)
    {
        m_outputColumns.add(column.guid());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected ArrayList<Integer> createOutputColumns(Database db, ArrayList<Integer> input) {
        return (ArrayList<Integer>)m_outputColumns.clone();
    }
    @Override
    public boolean computeEstimatesRecursively(PlanStatistics stats,
            Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        // TODO Auto-generated method stub
        return super.computeEstimatesRecursively(stats, cluster, db, estimates, paramHints);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
    }
}
