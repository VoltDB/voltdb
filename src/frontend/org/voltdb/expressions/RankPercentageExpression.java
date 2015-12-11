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

import java.math.BigDecimal;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.types.ExpressionType;

public class RankPercentageExpression extends AbstractExpression {
    public enum Members {
        VALUE,
        TARGET_TABLE_NAME,
        TARGET_INDEX_NAME,
        PARTITIONBY_SIZE;
    }

    private String m_value = null;
    private String m_tableName = null;
    private String m_indexName = null;
    private int m_partitionbySize = 0;


    public RankPercentageExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    public RankPercentageExpression(RankExpression rankExpr, ConstantValueExpression cve) {
        super(ExpressionType.WINDOWING_RANK_PERCENTAGE);
        m_value = cve.getValue();
        m_tableName = rankExpr.getTableName();
        m_indexName = rankExpr.getIndexName();
        m_partitionbySize = rankExpr.getPartitionbySize();
    }

    public String getValue() {
        return m_value;
    }

    public void setValue(String value) {
        m_value = value;
    }

    public String getTableName() {
        return m_tableName;
    }

    public void setTableName(String tableName) {
        m_tableName = tableName;
    }

    public String getIndexName() {
        return m_indexName;
    }

    public void setIndexName(String indexName) {
        m_indexName = indexName;
    }

    public int getPartitionbySize() {
        return m_partitionbySize;
    }

    public void setPartitionbySize(int partitionbySize) {
        m_partitionbySize = partitionbySize;
    }


    @Override
    public void finalizeValueTypes() {
        m_valueType = VoltType.BIGINT;
        m_valueSize = VoltType.BIGINT.getLengthInBytesForFixedTypes();
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) && obj instanceof RankPercentageExpression) {
            RankPercentageExpression rankPercentageExpr =
                    (RankPercentageExpression) obj;

            if (rankPercentageExpr.getValue().equals(m_value)
                    && rankPercentageExpr.getTableName().equals(m_tableName)
                    && rankPercentageExpr.getIndexName().equals(m_indexName)
                    && rankPercentageExpr.getPartitionbySize() == m_partitionbySize
                    ) {
                return true;
            }
        }
        return false;
    }


    @Override
    public int hashCode() {
        int hash = super.hashCode() + m_value.hashCode();
        if (m_tableName != null) {
            hash += m_tableName.hashCode();
        }
        if (m_indexName != null) {
            hash += m_indexName.hashCode();
        }

        hash += m_partitionbySize;
        return hash;
    }

    @Override
    public Object clone() {
        RankPercentageExpression clone = (RankPercentageExpression) super.clone();
        clone.setValue(m_value);
        clone.setTableName(m_tableName);
        clone.setIndexName(m_indexName);
        clone.setPartitionbySize(m_partitionbySize);
        return clone;
    }


    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        super.loadFromJSONObject(obj);
        m_value = obj.getString(Members.VALUE.name());
        m_tableName = obj.getString(Members.TARGET_TABLE_NAME.name());
        m_indexName = obj.getString(Members.TARGET_INDEX_NAME.name());
        m_partitionbySize = obj.getInt(Members.PARTITIONBY_SIZE.name());
    }


    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.VALUE.name()).value(m_value);
        stringer.key(Members.TARGET_TABLE_NAME.name()).value(m_tableName);
        stringer.key(Members.TARGET_INDEX_NAME.name()).value(m_indexName);
        stringer.key(Members.PARTITIONBY_SIZE.name()).value(m_partitionbySize);
    }

    @Override
    public String explain (String impliedTableName) {

        return "RankPercentageExpression with value: " + m_value;
    }

    public static boolean isConstantValueExpressionValid(ConstantValueExpression cve) {
        if (cve.getValueType() != VoltType.DECIMAL) {
            return false;
        }

        double val = new BigDecimal(cve.getValue()).doubleValue();
        if (val > 0 && val <= 1.0) {
            return true;
        }

        return false;
    }

}
