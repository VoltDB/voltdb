/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import org.voltdb.catalog.Database;
import org.voltdb.types.PlanNodeType;

public class CommonTablePlanNode extends AbstractPlanNode {
    private String m_commonTableName;
    private int m_recursiveStatementId;

    public enum Members {
        COMMON_TABLE_NAME,
        RECURSIVE_STMT_ID
    };

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.COMMONTABLE;
    }

    @Override
    public void resolveColumnIndexes() {
        // This node gets its output schema from its
        // only child, and it has no expressions or columns
        // on its own.  So there are no columns to resolve.
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "COMMON TABLE "
                 + m_commonTableName
                 + ", recursive statment id "
                 + m_recursiveStatementId
                 + "\n";
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.COMMON_TABLE_NAME.name()).value(m_commonTableName);
        stringer.key(Members.RECURSIVE_STMT_ID.name()).value(m_recursiveStatementId);
    }

    @Override
    protected void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_commonTableName = jobj.getString( Members.COMMON_TABLE_NAME.name() );
        m_recursiveStatementId = jobj.getInt( Members.RECURSIVE_STMT_ID.name() );
    }

    public final String getCommonTableName() {
        return m_commonTableName;
    }

    public final void setCommonTableName(String commonTableName) {
        m_commonTableName = commonTableName;
    }

    public final int getRecursiveStatementId() {
        return m_recursiveStatementId;
    }

    public final void setRecursiveStatementId(int recursiveStatementId) {
        m_recursiveStatementId = recursiveStatementId;
    }

}
