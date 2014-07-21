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

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.PlanNodeType;

public class InsertPlanNode extends AbstractOperationPlanNode {

    public enum Members {
        MULTI_PARTITION,
        FIELD_MAP
    }

    protected boolean m_multiPartition = false;
    private int[] m_fieldMap;

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
    }

    // TODO:Members not loaded
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
    }

    @Override
    protected String explainPlanForNode(String indent) {
        return "INSERT into \"" + m_targetTableName + "\"";
    }
}
