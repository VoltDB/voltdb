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
import org.voltdb.types.PlanNodeType;

public class InsertPlanNode extends AbstractOperationPlanNode {

    private static class Members {
        static final String MULTI_PARTITION = "MULTI_PARTITION";
        static final String FIELD_MAP = "FIELD_MAP";
        static final String UPSERT = "UPSERT";
        static final String SOURCE_IS_PARTITIONED = "SOURCE_IS_PARTITIONED";
    }

    protected boolean m_multiPartition = false;
    private int[] m_fieldMap;

    private boolean m_isUpsert = false;
    private boolean m_sourceIsPartitioned = false;

    public boolean isUpsert() {
        return m_isUpsert;
    }

    public InsertPlanNode() {
        super();
    }

    public InsertPlanNode(boolean isUpsert) {
        this();
        m_isUpsert = isUpsert;
    }

    public boolean getMultiPartition() {
        return m_multiPartition;
    }

    public void setMultiPartition(boolean multiPartition) {
        m_multiPartition = multiPartition;
    }

    public int[] getFieldMap() {
        return m_fieldMap;
    }

    public void setFieldMap(int[] fieldMap) {
        m_fieldMap = fieldMap;
    }

    public void setSourceIsPartitioned(boolean value) {
        m_sourceIsPartitioned = value;
    }

    public boolean getSourceIsPartitioned() {
        return m_sourceIsPartitioned;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.INSERT;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.MULTI_PARTITION, m_multiPartition);
        toJSONIntArrayString(stringer, Members.FIELD_MAP, m_fieldMap);

        if (m_isUpsert) {
            stringer.keySymbolValuePair(Members.UPSERT, true);
        }

        if (m_sourceIsPartitioned) {
            stringer.keySymbolValuePair(Members.SOURCE_IS_PARTITIONED, true);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_multiPartition = jobj.getBoolean(Members.MULTI_PARTITION);
        m_fieldMap = loadIntArrayMemberFromJSON(jobj, Members.FIELD_MAP);
        m_isUpsert = jobj.has(Members.UPSERT);
        m_sourceIsPartitioned = jobj.has(Members.SOURCE_IS_PARTITIONED);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        String type = "INSERT";
        if (m_isUpsert) {
            type = "UPSERT";
        }

        return type + " into \"" + m_targetTableName + "\"";
    }

    /** Order determinism for insert nodes depends on the determinism of child nodes.  For subqueries producing
     * unordered rows, the insert will be considered order-nondeterministic.
     * */
    @Override
    public boolean isOrderDeterministic() {
        assert(m_children != null);
        assert(m_children.size() == 1);

        // This implementation is very close to AbstractPlanNode's implementation of this
        // method, except that we assert just one child.
        // Java doesn't allow calls to super-super-class methods via super.super.
        AbstractPlanNode child = m_children.get(0);
        if (! child.isOrderDeterministic()) {
            m_nondeterminismDetail = child.m_nondeterminismDetail;
            return false;
        }

        return true;
    }
}
