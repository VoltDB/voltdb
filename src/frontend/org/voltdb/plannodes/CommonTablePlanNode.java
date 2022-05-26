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
import org.voltdb.catalog.Database;
import org.voltdb.planner.parseinfo.StmtCommonTableScan;
import org.voltdb.types.PlanNodeType;

public class CommonTablePlanNode extends AbstractPlanNode {
    private String m_commonTableName;
    private StmtCommonTableScan m_tableScan;
    private Integer m_recursiveStatementId;
    private AbstractPlanNode m_recurseNode;

    // Flags to help generate proper explain string outputs.
    private boolean m_shouldExplainInDetail = true;
    private boolean m_explainingRecurseNode = false;

    public enum Members {
        COMMON_TABLE_NAME,
        RECURSIVE_STATEMENT_ID
    };

    public CommonTablePlanNode() {
    };

    public CommonTablePlanNode(StmtCommonTableScan scan, String tableName) {
        m_tableScan = scan;
        // The table name and alias are the same here.
        m_commonTableName = scan.getTableName();
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.COMMONTABLE;
    }

    @Override
    public void resolveColumnIndexes() {
        // Common table nodes do not own any expressions or schema of their own,
        // so just resolve the indices of the child node.
        assert(m_children.size() == 1);
        AbstractPlanNode childNode = m_children.get(0);
        childNode.resolveColumnIndexes();
    }

    @Override
    protected String explainPlanForNode(String indent) {
        if (m_explainingRecurseNode) {
            // If we are explaining this node in a recursive query,
            // make a note that the scan is only on previous iteration results.
            return "\n" + indent + "only fetch results from the previous iteration";
        }
        if (! m_shouldExplainInDetail) {
            // If this common table scan node is already explained once, do not
            // explain repeatedly.
            return "";
        }
        m_shouldExplainInDetail = false;

        StringBuilder sb = new StringBuilder("\n");
        sb.append(indent)
          .append("MATERIALIZE COMMON TABLE \"")
          .append(m_commonTableName)
          .append("\"\n").append(indent);
        if (m_recurseNode == null) {
            sb.append("FROM ");
        }
        else {
            sb.append("START WITH ");
        }

        // Explain the base query.
        assert(m_children.size() == 1);
        AbstractPlanNode CTEChild = m_children.get(0);
        CTEChild.setSkipInitalIndentationForExplain(true);
        CTEChild.explainPlan_recurse(sb, indent);

        if (m_recurseNode != null) {
            m_explainingRecurseNode = true;
            sb.append(indent).append("ITERATE UNTIL EMPTY ");
            m_recurseNode.setSkipInitalIndentationForExplain(true);
            m_recurseNode.explainPlan_recurse(sb, indent);
            m_explainingRecurseNode = false;
        }
        return sb.toString();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.COMMON_TABLE_NAME.name()).value(m_commonTableName);
        if (m_recursiveStatementId == null && m_tableScan != null) {
            m_recursiveStatementId = m_tableScan.getRecursiveStmtId();
        }
        if (m_recursiveStatementId != null) {
            stringer.key(Members.RECURSIVE_STATEMENT_ID.name()).value(m_recursiveStatementId);
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject jobj, Database db) throws JSONException {
        helpLoadFromJSONObject(jobj, db);
        m_commonTableName = jobj.getString( Members.COMMON_TABLE_NAME.name() );
        if (jobj.has(Members.RECURSIVE_STATEMENT_ID.name())) {
            m_recursiveStatementId = jobj.getInt( Members.RECURSIVE_STATEMENT_ID.name() );
        }
        else {
            m_recursiveStatementId = null;
        }
    }

    public Integer getRecursiveNodeId() {
        return m_recursiveStatementId;
    }

    public void setRecursiveNode(AbstractPlanNode node) {
        m_recurseNode = node;
    }

    public AbstractPlanNode getRecursiveNode() {
        return m_recurseNode;
    }

    public final String getCommonTableName() {
        return m_commonTableName;
    }

    public final void setCommonTableName(String commonTableName) {
        m_commonTableName = commonTableName;
    }
}
