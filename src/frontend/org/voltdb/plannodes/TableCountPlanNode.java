/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.types.PlanNodeType;

public class TableCountPlanNode extends AbstractScanPlanNode {

    public enum Members {
        TMP_TABLE
    }

    private boolean m_tmpTable;

    public TableCountPlanNode() {
        super();
    }

    public TableCountPlanNode(String tableName, String tableAlias) {
        super(tableName, tableAlias);
        assert(tableName != null && tableAlias != null);
    }

    public TableCountPlanNode(AbstractScanPlanNode child, AggregatePlanNode apn) {
        super(child.getTargetTableName(), child.getTargetTableAlias());
        m_outputSchema = apn.getOutputSchema().clone();
        m_hasSignificantOutputSchema = true;
        m_estimatedOutputTupleCount = 1;
        m_tableSchema = child.getTableSchema();

        m_tmpTable = child.isSubQuery();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.TABLECOUNT;
    }

    /**
     * Should just return true -- there's only one order for a single row
     * @return true
     */
    @Override
    public boolean isOrderDeterministic() {
        return true;
    }

    @Override
    public void generateOutputSchema(Database db){}

    @Override
    public void resolveColumnIndexes(){}

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        m_estimatedProcessedTupleCount = 1;
        m_estimatedOutputTupleCount = 1;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        if (m_tmpTable) {
            stringer.key(Members.TMP_TABLE.name());
            stringer.value(true);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_tmpTable = false;
        if (jobj.has(Members.TMP_TABLE.name())) {
            m_tmpTable = true;
        }
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String explainStr = "TABLE COUNT of \"" + m_targetTableName + "\"";
        if (m_tmpTable) {
            explainStr = "TEMPORARY " + explainStr;
        }
        return explainStr;
    }

}
