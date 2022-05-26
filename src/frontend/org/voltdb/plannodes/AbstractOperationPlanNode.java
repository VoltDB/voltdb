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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;

public abstract class AbstractOperationPlanNode extends AbstractPlanNode {

    private static class Members {
        static final String TARGET_TABLE_NAME = "TARGET_TABLE_NAME";
    }

    // The target table is the table that the plannode wants to perform some operation on.
    protected String m_targetTableName = "";

    protected AbstractOperationPlanNode() {
        super();
    }

    @Override
    public String getUpdatedTable() {
        assert(m_targetTableName.length() > 0);
        return m_targetTableName;
    }

    protected String debugInfo(String spacer) {
        return spacer + "TargetTableName[" + m_targetTableName + "]\n";
    }

    @Override
    public void validate() {
        super.validate();

        // All Operation nodes need to have a target table
        if (m_targetTableName == null) {
            throw new ValidationError("The Target TableId is null for PlanNode '%s'", this);
        }
    }

    /**
     * Accessor for flag marking the plan as guaranteeing an identical result/effect
     * when "replayed" against the same database state, such as during replication or CL recovery.
     * @return Force subclasses to assess determinism (INSERT INTO ... SELECT may be nondeterministic)
     */
    @Override
    public abstract boolean isOrderDeterministic();

    /**
     * @return the target_table_name
     */
    public final String getTargetTableName() {
        return m_targetTableName;
    }

    /**
     * @param target_table_name the target_table_name to set
     */
    public final void setTargetTableName(final String target_table_name) {
        m_targetTableName = target_table_name;
    }

    @Override
    public void generateOutputSchema(Database db) {
        // Operation nodes (Insert/update/delete) have an output schema
        // of one column, which is the number of modified tuples
        // Delete nodes have a special case with no child node when they
        // are truncating the entire table
        if (m_children.size() == 1) {
            m_children.get(0).generateOutputSchema(db);
        }
        // Our output schema isn't ever going to change, only generate this once
        if (m_outputSchema == null) {
            m_outputSchema = new NodeSchema();
            // If there is a child node, its output schema will depend on that.
            // If not, mark this flag true to get initialized in EE.
            m_hasSignificantOutputSchema = (m_children.size() == 0);

            // This TVE is magic and repeats unfortunately like this
            // throughout the planner.  Consolidate at some point --izzy
            TupleValueExpression tve = new TupleValueExpression(
                    AbstractParsedStmt.TEMP_TABLE_NAME, AbstractParsedStmt.TEMP_TABLE_NAME,
                    "modified_tuples", "modified_tuples", 0);
            tve.setValueType(VoltType.BIGINT);
            tve.setValueSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
            m_outputSchema.addColumn(
                    AbstractParsedStmt.TEMP_TABLE_NAME, AbstractParsedStmt.TEMP_TABLE_NAME,
                    "modified_tuples", "modified_tuples",
                    tve);
        }
        return;
    }

    @Override
    public void resolveColumnIndexes() {
        if (m_children.size() == 1) {
            m_children.get(0).resolveColumnIndexes();
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.TARGET_TABLE_NAME, m_targetTableName);
    }

    @Override
    public void loadFromJSONObject(JSONObject jobj, Database db)
            throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_targetTableName = jobj.getString(Members.TARGET_TABLE_NAME);
    }
}
