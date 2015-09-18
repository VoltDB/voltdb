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

package org.voltdb.expressions;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class HappenedAfterExpression extends AbstractValueExpression {

    public enum Members {
        CLUSTERID,
        TIMESTAMP
    }

    int m_clusterId;
    long m_lastSeenTimestamp;

    public HappenedAfterExpression(int clusterId, long lastSeenTimestamp) {
        super(ExpressionType.HAPPENED_AFTER);
        m_clusterId = clusterId;
        m_lastSeenTimestamp = lastSeenTimestamp;
        //See the comment in ConjunctionExpression
        setValueType(VoltType.BIGINT);
    }

    public void setClusterId(int clusterId) {
        m_clusterId = clusterId;
    }

    public int getClusterId() {
        return m_clusterId;
    }

    public void setTimestamp(long timestamp) {
        m_lastSeenTimestamp = timestamp;
    }

    public long getTimestamp() {
        return m_lastSeenTimestamp;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.CLUSTERID.name()).value(m_clusterId);
        stringer.key(Members.TIMESTAMP.name()).value(m_lastSeenTimestamp);
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        m_clusterId = obj.getInt(Members.CLUSTERID.name());
        m_lastSeenTimestamp = obj.getInt(Members.TIMESTAMP.name());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HappenedAfterExpression == false) {
            return false;
        }
        HappenedAfterExpression expr = (HappenedAfterExpression) obj;
        if (m_clusterId != expr.getClusterId()) {
            return false;
        }
        if (m_lastSeenTimestamp != expr.getTimestamp()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = 0;
        result += m_clusterId;
        result += m_lastSeenTimestamp;
        // defer to the superclass, which factors in other attributes
        return result += super.hashCode();
    }

    @Override
    public String explain(String impliedTableName) {
        return "happened after";
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if ((m_right != null) || (m_left != null))
            throw new Exception("ERROR: A Happened After expression has child expressions for '" + this + "'");

       if (m_clusterId < 0)
           throw new Exception("ERROR: A Happened After expression has no cluster id for '" + this + "'");

       if (m_lastSeenTimestamp < 0) {
           throw new Exception("ERROR: A Happened After expression has no timestamp for '" + this + "'");
       }
    }

}
