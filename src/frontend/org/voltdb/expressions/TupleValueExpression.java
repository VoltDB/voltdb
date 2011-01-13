/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.expressions;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class TupleValueExpression extends AbstractValueExpression {

    public enum Members {
        COLUMN_IDX,
        TABLE_NAME,
        COLUMN_NAME,
        COLUMN_ALIAS
    }

    protected int m_columnIndex = -1;
    protected String m_tableName = null;
    protected String m_columnName = null;
    protected String m_columnAlias = null;

    public TupleValueExpression() {
        super(ExpressionType.VALUE_TUPLE);
    }

    public TupleValueExpression(AbstractExpression left, AbstractExpression right) {
        super(ExpressionType.VALUE_TUPLE, null, null);
        assert(left == null);
        assert(right == null);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        TupleValueExpression clone = (TupleValueExpression)super.clone();
        clone.m_columnIndex = m_columnIndex;
        clone.m_tableName = m_tableName;
        clone.m_columnName = m_columnName;
        clone.m_columnAlias = m_columnAlias;
        return clone;
    }

    @Override
    public void validate() throws Exception {
        super.validate();

        if ((m_right != null) || (m_left != null))
            throw new Exception("ERROR: A value expression has child expressions for '" + this + "'");

        // Column Index
        if (m_columnIndex < 0) {
            throw new Exception("ERROR: Invalid column index '" + m_columnIndex + "' for '" + this + "'");
        }
    }

    /**
     * @return the column index
     */
    public Integer getColumnIndex() {
        return m_columnIndex;
    }

    /**
     * @param columnIndex The index of the column to set
     */
    public void setColumnIndex(Integer columnIndex) {
        m_columnIndex = columnIndex;
    }

    /**
     * @return the column_aliases
     */
    public String getColumnAlias() {
        return m_columnAlias;
    }

    /**
     * @param columnAlias the column_alias to set
     */
    public void setColumnAlias(String columnAlias) {
        m_columnAlias = columnAlias;
    }

    /**
     * @return the columns
     */
    public String getColumnName() {
        return m_columnName;
    }

    /**
     * @param name the column name to set
     */
    public void setColumnName(String name) {
        m_columnName = name;
    }

    /**
     * @return the tables
     */
    public String getTableName() {
        return m_tableName;
    }

    /**
     * @param name the table name to set
     */
    public void setTableName(String name) {
        m_tableName = name;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TupleValueExpression == false) return false;
        TupleValueExpression expr = (TupleValueExpression) obj;

        if ((expr.m_tableName == null) != (m_tableName == null))
            return false;
        if ((expr.m_columnName == null) != (m_columnName == null))
            return false;

        if (expr.m_tableName != null)
            if (expr.m_tableName.equals(m_tableName) == false)
                return false;
        if (expr.m_columnName != null)
            if (expr.m_columnName.equals(m_columnName) == false)
                return false;

        // if all seems well, defer to the superclass, which checks kids
        return super.equals(obj);
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.COLUMN_IDX.name()).value(m_columnIndex);
        stringer.key(Members.TABLE_NAME.name()).value(m_tableName);
        stringer.key(Members.COLUMN_NAME.name()).value(m_columnName);
        stringer.key(Members.COLUMN_ALIAS.name()).value(m_columnAlias);
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, Database db) throws JSONException {
        m_columnIndex = obj.getInt(Members.COLUMN_IDX.name());
        m_tableName = obj.getString(Members.TABLE_NAME.name());
        m_columnName = obj.getString(Members.COLUMN_NAME.name());
        m_columnAlias = obj.getString(Members.COLUMN_ALIAS.name());
    }
}
