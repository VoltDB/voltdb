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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.ScalarValueHints;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.SortDirectionType;

/**
 * Used for SQL-IN that are accelerated with indexes.
 * A MaterializedScanPlanNode is created from the list part
 * of the SQL-IN-LIST. It is inner-joined with NLIJ to another
 * table to make the SQL-IN fast.
 *
 */
public class MaterializedScanPlanNode extends AbstractPlanNode {

    private AbstractExpression m_tableData;
    private final TupleValueExpression m_outputExpression = new TupleValueExpression(
            "materialized_temp_table", "materialized_temp_table", "list_element", null, 0);
    private SortDirectionType m_sortDirection = SortDirectionType.INVALID;

    public enum Members {
        TABLE_DATA,
        SORT_DIRECTION;
    }

    public MaterializedScanPlanNode() {
        super();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.MATERIALIZEDSCAN;
    }

    public void setRowData(AbstractExpression tableData) {
        assert(tableData instanceof VectorValueExpression || tableData instanceof ParameterValueExpression);
        m_tableData = tableData;

        m_outputExpression.setTypeSizeBytes(m_tableData.getValueType(), m_tableData.getValueSize(),
                m_tableData.getInBytes());
    }

    public void setSortDirection(SortDirectionType direction) {
        m_sortDirection = direction;
    }

    public SortDirectionType getSortDirection() {
        return m_sortDirection;
    }

    // Extract a TVE for the single column of a MaterializedScan for use as a join key for an IndexScan
    public AbstractExpression getOutputExpression()
    {
        return m_outputExpression;
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return true
     */
    @Override
    public boolean isOrderDeterministic() {
        return true;
    }

    @Override
    public void computeCostEstimates(long childOutputTupleCountEstimate, Cluster cluster, Database db, DatabaseEstimates estimates, ScalarValueHints[] paramHints) {
        // assume constant cost. Most of the cost of the SQL-IN will be measured by the NLIJ that is always paired with this element
        m_estimatedProcessedTupleCount = 1;
        m_estimatedOutputTupleCount = 1;
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "MATERIALIZED SCAN of SQL-IN-LIST (Sort " + m_sortDirection.toString() + ")";
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        assert(m_children.size() == 0);
        m_hasSignificantOutputSchema = true;
        // fill in the table schema if we haven't already
        if (m_outputSchema == null) {
            m_outputSchema = new NodeSchema();
            // must produce a tuple value expression for the one column.
            m_outputSchema.addColumn(
                new SchemaColumn(m_outputExpression.getTableName(),
                                 m_outputExpression.getTableAlias(),
                                 m_outputExpression.getColumnName(),
                                 m_outputExpression.getColumnAlias(),
                                 m_outputExpression));
        }
    }

    @Override
    public void resolveColumnIndexes() {
        // MaterializedScanPlanNodes have no children
        assert(m_children.size() == 0);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key(Members.TABLE_DATA.name());
        stringer.object();
        assert(m_tableData != null);
        m_tableData.toJSONString(stringer);
        stringer.endObject();

        if (m_sortDirection == SortDirectionType.DESC) {
            stringer.key(Members.SORT_DIRECTION.name()).value(m_sortDirection.toString());
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        helpLoadFromJSONObject(obj, db);

        assert(!obj.isNull(Members.TABLE_DATA.name()));
        m_tableData = AbstractExpression.fromJSONChild(obj, Members.TABLE_DATA.name());

        if (!obj.isNull(Members.SORT_DIRECTION.name())) {
            m_sortDirection = SortDirectionType.get(obj.getString( Members.SORT_DIRECTION.name()));
        }
    }

}
