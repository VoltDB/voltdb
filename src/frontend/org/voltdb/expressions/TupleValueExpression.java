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
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;

/**
 *
 */
public class TupleValueExpression extends AbstractValueExpression {

    public enum Members {
        COLUMN_IDX,
        TABLE_IDX,  // used for JOIN queries only, 0 for outer table, 1 for inner table
    }

    protected int m_columnIndex = -1;
    protected String m_tableName = null;
    protected String m_tableAlias = null;
    protected String m_columnName = null;
    protected String m_columnAlias = null;
    protected int m_tableIdx = 0;

    private boolean m_hasAggregate = false;

    /**
     * Create a new TupleValueExpression
     * @param tableName  The name of the table where this column originated,
     *        if any.  Currently, internally created columns will be assigned
     *        the table name "VOLT_TEMP_TABLE" for disambiguation.
     * @param tableAlias  The alias assigned to this table, if any
     * @param columnName  The name of this column, if any
     * @param columnAlias  The alias assigned to this column, if any
     * @param columnIndex. The column index in the table
     */
    public TupleValueExpression(String tableName,
                                String tableAlias,
                                String columnName,
                                String columnAlias,
                                int columnIndex) {
        super(ExpressionType.VALUE_TUPLE);
        m_tableName = tableName;
        m_tableAlias = tableAlias;
        m_columnName = columnName;
        m_columnAlias = columnAlias;
        m_columnIndex = columnIndex;
    }

    public TupleValueExpression(String tableName,
                                String tableAlias,
                                String columnName,
                                String columnAlias) {
        this(tableName, tableAlias, columnName, columnAlias, -1);
    }

    public TupleValueExpression(String tableName,
                                String columnName,
                                int columnIndex) {
        this(tableName, null, columnName, null, columnIndex);
    }

    public TupleValueExpression() {
        super(ExpressionType.VALUE_TUPLE);
    }

    /// Only set for the special case of an aggregate function result used in an "ORDER BY" clause.
    /// This TupleValueExpression represents the corresponding "column" in the aggregate's generated output TEMP table.
    public boolean hasAggregate() {
        return m_hasAggregate;
    }

    public void setHasAggregate(boolean m_hasAggregate) {
        this.m_hasAggregate = m_hasAggregate;
    }

    @Override
    public Object clone() {
        TupleValueExpression clone = (TupleValueExpression)super.clone();
        clone.m_columnIndex = m_columnIndex;
        clone.m_tableName = m_tableName;
        clone.m_tableAlias = m_tableAlias;
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
    public int getColumnIndex() {
        return m_columnIndex;
    }

    /**
     * @param columnIndex The index of the column to set
     */
    public void setColumnIndex(int columnIndex) {
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

    /**
     * @return the tables
     */
    public String getTableAlias() {
        return m_tableAlias;
    }

    public void setTableAlias(String alias) {
        m_tableAlias = alias;
    }

    public int getTableIndex() {
        return m_tableIdx;
    }

    public void setTableIndex(int idx) {
        m_tableIdx = idx;
    }

    public void setTypeSizeBytes(VoltType SchemaColumnType, int size, boolean bytes) {
        setValueType(SchemaColumnType);
        setValueSize(size);
        m_inBytes = bytes;
    }

    public void setTypeSizeBytes(int columnType, int size, boolean bytes) {
        setTypeSizeBytes(VoltType.get((byte)columnType), size, bytes);
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
        if (m_tableAlias != null && expr.m_tableAlias != null) {
            // Implying both sides non-null
            // If one of the table aliases is NULL it is considered to be a wild card
            // matching any alias.
            if (m_tableAlias.equals(expr.m_tableAlias) == false) {
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
        if (m_tableIdx > 0) {
            stringer.key(Members.TABLE_IDX.name()).value(m_tableIdx);
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, StmtTableScan tableScan) throws JSONException
    {
        m_columnIndex = obj.getInt(Members.COLUMN_IDX.name());
        if (obj.has(Members.TABLE_IDX.name())) {
            m_tableIdx = obj.getInt(Members.TABLE_IDX.name());
        }
        if (tableScan != null) {
            m_tableAlias = tableScan.getTableAlias();
            m_tableName = tableScan.getTableName();
            m_columnName = tableScan.getColumnName(m_columnIndex);
        }
    }

    @Override
    public void resolveForTable(Table table) {
        assert(table != null);
        // It MAY be that for the case in which this function is called (expression indexes), the column's
        // table name is not specified (and not missed?).
        // It is possible to "correct" that here by cribbing it from the supplied table (base table for the index)
        // -- not bothering for now.
        Column column = table.getColumns().getExact(m_columnName);
        assert(column != null);
        m_tableName = table.getTypeName();
        m_columnIndex = column.getIndex();

        setTypeSizeBytes(column.getType(), column.getSize(), column.getInbytes());
    }

    /**
     * Given an input schema, resolve the TVE
     * expressions.
     */
    public int resolveColumnIndexesUsingSchema(NodeSchema inputSchema) {
        int index = inputSchema.getIndexOfTve(this);
        if (getValueType() == null && index != -1) {
            // In case of sub-queries the TVE may not have its value type and size
            // resolved yet. Try to resolve it now
            SchemaColumn inputColumn = inputSchema.getColumns().get(index);
            setTypeSizeBytes(inputColumn.getType(), inputColumn.getSize(),
                    inputColumn.getExpression().getInBytes());
        }
        return index;
    }

    // Even though this function applies generally to expressions and tables and not just to TVEs as such,
    // this function is somewhat TVE-related because TVEs DO represent the points where expression trees
    // depend on tables.
    public static AbstractExpression getOtherTableExpression(AbstractExpression expr, String tableAlias) {
        assert(expr != null);
        AbstractExpression retval = expr.getLeft();
        if (isOperandDependentOnTable(retval, tableAlias)) {
            retval = expr.getRight();
            assert( ! isOperandDependentOnTable(retval, tableAlias));
        }
        return retval;
    }

    // Even though this function applies generally to expressions and tables and not just to TVEs as such,
    // this function is somewhat TVE-related because TVEs DO represent the points where expression trees
    // depend on tables.
    public static boolean isOperandDependentOnTable(AbstractExpression expr, String tableAlias) {
        assert(tableAlias != null);
        for (TupleValueExpression tve : ExpressionUtil.getTupleValueExpressions(expr)) {
            //TODO: This clumsy testing of table names regardless of table aliases is
            // EXACTLY why we can't have nice things like self-joins.
            if (tableAlias.equals(tve.getTableAlias()))
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public String explain(String impliedTableName) {
        String tableName = m_tableName;
        String columnName = m_columnName;
        if (columnName == null || columnName.equals("")) {
            columnName = "column#" + m_columnIndex;
        }
        if (m_verboseExplainForDebugging) {
            columnName += " (as JSON: ";
            JSONStringer stringer = new JSONStringer();
            try
            {
                stringer.object();
                toJSONString(stringer);
                stringer.endObject();
                columnName += stringer.toString();
            }
            catch (Exception e)
            {
                columnName += "CORRUPTED beyond the ability to format? " + e;
                e.printStackTrace();
            }
            columnName += ")";
        }
        if (tableName == null) {
            if (m_tableIdx != 0) {
                assert(m_tableIdx == 1);
                // This is join inner table
                return "inner-table." + columnName;
            }
        }
        else if ( ! tableName.equals(impliedTableName)) {
            return tableName + "." + columnName;
        } else if (m_verboseExplainForDebugging) {
            // In verbose mode, always show an "implied' tableName that would normally be left off.
            return "{" + tableName + "}." + columnName;
        }
        return columnName;
    }

}
