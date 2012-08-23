/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.planner.PlanStatistics;
import org.voltdb.types.PlanNodeType;

public class TableCountPlanNode extends AbstractScanPlanNode {

    public TableCountPlanNode() {
        super();
    }

    public TableCountPlanNode(SeqScanPlanNode ssp, AggregatePlanNode apn) {
        super();
        m_outputSchema = apn.getOutputSchema().clone();
        m_estimatedOutputTupleCount = 1;
        m_targetTableAlias = ssp.getTargetTableAlias();
        m_targetTableName = ssp.getTargetTableName();
        m_tableSchema = ssp.getTableSchema();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.TABLECOUNT;
    }

    @Override
    public void generateOutputSchema(Database db){}

    @Override
    public void resolveColumnIndexes(){}

    @Override
    public boolean computeEstimatesRecursively(PlanStatistics stats, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
//        Table target = db.getTables().getIgnoreCase(m_targetTableName);
//        assert(target != null);
//        DatabaseEstimates.TableEstimates tableEstimates = estimates.getEstimatesForTable(target.getTypeName());
//        stats.incrementStatistic(0, StatsField.TUPLES_READ, tableEstimates.maxTuples);
        m_estimatedOutputTupleCount = 1;
        return true;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "TABLE COUNT of \"" + m_targetTableName + "\"";
    }

}
