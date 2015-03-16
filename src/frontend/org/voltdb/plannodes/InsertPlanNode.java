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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.PlanNodeType;

public class InsertPlanNode extends AbstractOperationPlanNode {

    public enum Members {
        MULTI_PARTITION,
        FIELD_MAP,
        UPSERT,
        SOURCE_IS_PARTITIONED
    }

    protected boolean m_multiPartition = false;
    private int[] m_fieldMap;

    private boolean m_isUpsert = false;
    private boolean m_sourceIsPartitioned = false;

    public boolean isUpsert() {
        return m_isUpsert;
    }

    public void setUpsert(boolean isUpsert) {
        this.m_isUpsert = isUpsert;
    }

    public InsertPlanNode() {
        super();
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
        stringer.key(Members.MULTI_PARTITION.name()).value(m_multiPartition);
        stringer.key(Members.FIELD_MAP.name()).array();
        for (int i : m_fieldMap) {
            stringer.value(i);
        }
        stringer.endArray();

        if (m_isUpsert) {
            stringer.key(Members.UPSERT.name()).value(true);
        }

        if (m_sourceIsPartitioned) {
            stringer.key(Members.SOURCE_IS_PARTITIONED.name()).value(true);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_multiPartition = jobj.getBoolean( Members.MULTI_PARTITION.name() );
        if (!jobj.isNull(Members.FIELD_MAP.name())) {
            JSONArray jarray = jobj.getJSONArray(Members.FIELD_MAP.name());
            int numFields = jarray.length();
            m_fieldMap = new int[numFields];
            for (int i = 0; i < numFields; ++i) {
                m_fieldMap[i] = jarray.getInt(i);
            }
        }

        m_isUpsert = false;
        if (jobj.has(Members.UPSERT.name())) {
            m_isUpsert = true;
        }

        m_sourceIsPartitioned = false;
        if (jobj.has(Members.SOURCE_IS_PARTITIONED.name())) {
            m_sourceIsPartitioned = true;
        }
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
