/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class TupleValueExpression extends AbstractValueExpression {

    public enum Members {
        COLUMN_IDX,
        TABLE_NAME,
        COLUMN_NAME,
        TABLE_IDX,  // used for JOIN queries only, 0 for outer table, 1 for inner table
    }

    protected int m_columnIndex = -1;
    protected String m_tableName = null;
    protected String m_columnName = null;
    protected String m_columnAlias = null;
    protected int m_tableIdx = 0;

    private boolean m_hasAggregate = false;

    /// Only set for the special case of an aggregate function result used in an "ORDER BY" clause.
    /// This TupleValueExpression represents the corresponding "column" in the aggregate's generated output TEMP table.
    public boolean hasAggregate() {
        return m_hasAggregate;
    }

    public void setHasAggregate(boolean m_hasAggregate) {
        this.m_hasAggregate = m_hasAggregate;
    }

    public TupleValueExpression() {
        super(ExpressionType.VALUE_TUPLE);
    }

    @Override
    public Object clone() {
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

    public int getTableIndex() {
        return m_tableIdx;
    }

    public void setTableIndex(int idx) {
        m_tableIdx = idx;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TupleValueExpression == false) {
            return false;
        }
        TupleValueExpression expr = (TupleValueExpression) obj;

        if ((m_tableName == null) != (expr.m_tableName == null)) {
            return false;
        }
        if ((m_columnName == null) != (expr.m_columnName == null)) {
            return false;
        }
        if (m_tableName != null) { // Implying both sides non-null
            if (m_tableName.equals(expr.m_tableName) == false) {
                return false;
            }
        }
        if (m_columnName != null) { // Implying both sides non-null
            if (m_columnName.equals(expr.m_columnName) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // based on implementation of equals
        int result = 0;
        if (m_tableName != null) {
            result += m_tableName.hashCode();
        }
        if (m_columnName != null) {
            result += m_columnName.hashCode();
        }
        // defer to the superclass, which factors in other attributes
        return result += super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.key(Members.COLUMN_IDX.name()).value(m_columnIndex);
        stringer.key(Members.TABLE_NAME.name()).value(m_tableName);
        // Column name is not required in the EE but testing showed that it is
        // needed to support type resolution of indexed expressions in the planner
        // after they get round-tripped through the catalog's index definition.
        stringer.key(Members.COLUMN_NAME.name()).value(m_columnName);
        if (m_tableIdx > 0) {
            stringer.key(Members.TABLE_IDX.name()).value(m_tableIdx);
            //System.out.println("TVE: toJSONString(), tableIdx = " + m_tableIdx);
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException
    {
        m_columnIndex = obj.getInt(Members.COLUMN_IDX.name());
        m_tableName = obj.getString(Members.TABLE_NAME.name());
        m_columnName = obj.getString(Members.COLUMN_NAME.name());
        if (obj.has(Members.TABLE_IDX.name())) {
            m_tableIdx = obj.getInt(Members.TABLE_IDX.name());
            //System.out.println("TVE: loadFromJSONObject(), tableIdx = " + m_tableIdx);
        }
    }

    @Override
    public void resolveForDB(Database db) {
        if (m_tableName == null && m_columnName == null) {
            // This is a dummy TVE standing in for a simplecolumn
            // -- the assumption has to be that it is not being used in a general expression,
            // so the schema-dependent type implications don't matter
            // and its "target" value is getting properly validated, so we can shortcut checking here.
            assert(false);
            return;
        }
        // TODO(XIN): getIgnoreCase takes 2% of Planner CPU, Optimize it later
        Table table = db.getTables().getIgnoreCase(m_tableName);
        resolveForTable(table);
    }

    @Override
    public void resolveForTable(Table table) {
        assert(table != null);
        // It MAY be that for the case in which this function is called (expression indexes), the column's
        // table name is not specified (and not missed?).
        // It is possible to "correct" that here by cribbing it from the supplied table (base table for the index)
        // -- not bothering for now.
        Column column = table.getColumns().getIgnoreCase(m_columnName);
        assert(column != null);
        m_tableName = table.getTypeName();
        m_columnIndex = column.getIndex();
        setValueType(VoltType.get((byte)column.getType()));
        setValueSize(column.getSize());
    }

    // Even though this function applies generally to expressions and tables and not just to TVEs as such,
    // this function is somewhat TVE-related because TVEs DO represent the points where expression trees
    // depend on tables.
    public static AbstractExpression getOtherTableExpression(AbstractExpression expr, Table table) {
        assert(expr != null);
        AbstractExpression retval = expr.getLeft();
        if (isOperandDependentOnTable(retval, table)) {
            retval = expr.getRight();
            assert( ! isOperandDependentOnTable(retval, table));
        }
        return retval;
    }

    // Even though this function applies generally to expressions and tables and not just to TVEs as such,
    // this function is somewhat TVE-related because TVEs DO represent the points where expression trees
    // depend on tables.
    public static boolean isOperandDependentOnTable(AbstractExpression expr, Table table) {
        for (TupleValueExpression tve : ExpressionUtil.getTupleValueExpressions(expr)) {
            //TODO: This clumsy testing of table names regardless of table aliases is
            // EXACTLY why we can't have nice things like self-joins.
            if (table.getTypeName().equals(tve.getTableName()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String explain(String impliedTableName) {
        if (m_tableName.equals(impliedTableName)) {
            return m_columnName;
        } else {
            return m_tableName + "." + m_columnName;
        }
    }

}
