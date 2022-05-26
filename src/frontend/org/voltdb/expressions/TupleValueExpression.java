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

package org.voltdb.expressions;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ValidationError;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;

public class TupleValueExpression extends AbstractValueExpression {

    private static class Members {
        static final String COLUMN_IDX = "COLUMN_IDX";
        // used for JOIN queries only, 0 for outer table, 1 for inner table
        static final String TABLE_IDX = "TABLE_IDX";
    }

    private int m_columnIndex = -1;
    private String m_tableName = null;
    private String m_tableAlias = null;
    private String m_columnName = null;
    private String m_columnAlias = null;
    private int m_tableIdx = 0;

    // Tables that are not persistent tables, but those produced internally,
    // (by subqueries for example) may contains columns whose names are the same.
    // Consider this statement for example:
    //     SELECT * FROM (SELECT * FROM T, T) as sub_t;
    // If the table T has a column named "C", then the output of the subquery will
    // have two columns named "C".  HSQL is able to tell these apart, so we use the
    // "index" field produced by voltXML as a differentiator between identical columns.
    private int m_differentiator = -1;
    private boolean m_needsDifferentiation = true;

    private boolean m_hasAggregate = false;
    /** The statement id this TVE refers to */
    private int m_origStmtId = -1;

    /**
     * Create a new TupleValueExpression
     * @param tableName  The name of the table where this column originated,
     *        if any.  Currently, internally created columns will be assigned
     *        the table name AbstractParsedStmt.TEMP_TABLE_NAME for disambiguation.
     * @param tableAlias  The alias assigned to this table, if any
     * @param columnName  The name of this column, if any
     * @param columnAlias  The alias assigned to this column, if any
     * @param columnIndex. The column index in the table
     */
    public TupleValueExpression(String tableName,
                                String tableAlias,
                                String columnName,
                                String columnAlias,
                                int columnIndex,
                                int differentiator) {
        super(ExpressionType.VALUE_TUPLE);
        m_tableName = tableName;
        m_tableAlias = tableAlias;
        m_columnName = columnName;
        m_columnAlias = columnAlias;
        m_columnIndex = columnIndex;
        m_differentiator = differentiator;
    }

    public TupleValueExpression(String tableName, String tableAlias, Column catalogCol, int columnIndex) {
        this(tableName, tableAlias,
                catalogCol.getName(), catalogCol.getName(),
                columnIndex, -1);
        setTypeSizeAndInBytes(catalogCol);
    }

    public TupleValueExpression(String tableName,
            String tableAlias,
            String columnName,
            String columnAlias,
            int columnIndex) {
        this(tableName, tableAlias, columnName, columnAlias, columnIndex, -1);
    }

    public TupleValueExpression(String tableName,
            String tableAlias,
            String columnName,
            String columnAlias,
            AbstractExpression typeSource,
            int columnIndex) {
        this(tableName, tableAlias, columnName, columnAlias, columnIndex, -1);
        setTypeSizeAndInBytes(typeSource);
    }

    public TupleValueExpression(String tableName, String tableAlias, String columnName, String columnAlias) {
        this(tableName, tableAlias, columnName, columnAlias, -1, -1);
    }

    public TupleValueExpression(String tableName, String columnName, int columnIndex) {
        this(tableName, null, columnName, null, columnIndex, -1);
    }

    public TupleValueExpression(Column column) {
        this(column.getParent().getTypeName(), column.getTypeName(), column.getIndex());
        setValueType(VoltType.get((byte) column.getType()));
    }

    public TupleValueExpression() {
        super(ExpressionType.VALUE_TUPLE);
    }

    @Override
    public TupleValueExpression anonymize() {
       setTableName(null);
       setTableAlias(null);
       setColumnName(null);
       setColumnAlias(null);
       setDifferentiator(-1);
       return this;
    }

    /*
     *  Only set for the special case of an aggregate function result used in
     *  an "ORDER BY" clause. This TupleValueExpression represents the
     *  corresponding "column" in the aggregate's generated output TEMP table.
     */
    public boolean hasAggregate() {
        return m_hasAggregate;
    }

    public void setHasAggregate(boolean hasAggregate) {
        m_hasAggregate = hasAggregate;
    }

    @Override
    public void validate() {
        super.validate();

        if (m_right != null || m_left != null) {
            throw new ValidationError("A value expression has child expressions for '%s", toString());
        }

        // Column Index
        if (m_columnIndex < 0) {
            throw new ValidationError("Invalid column index '%d' for '%s'", m_columnIndex, toString());
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
     * @return the table name for this column reference
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
     * @return the table alias for this column reference
     */
    public String getTableAlias() {
        return m_tableAlias;
    }

    public void setTableAlias(String alias) {
        m_tableAlias = alias;
    }

    boolean matchesTableAlias(String tableAlias) {
       if (m_tableAlias == null) {
          return m_tableName.equals(tableAlias);
       } else {
          return m_tableAlias.equals(tableAlias);
       }
    }

    public int getTableIndex() {
        return m_tableIdx;
    }

    public void setTableIndex(int idx) {
        m_tableIdx = idx;
    }

    /**
     * Get the differentiator field (a number used to make this field distinct
     * from other column with the same name with a table schema).
     */
    public int getDifferentiator() {
        return m_differentiator;
    }

    /**
     * Set the differentiator field (a number used to make this field distinct
     * from other column with the same name with a table schema).
     */
    public void setDifferentiator(int val) {
        m_differentiator = val;
    }

    public final boolean needsDifferentiation() {
        return m_needsDifferentiation;
    }

    public final void setNeedsNoDifferentiation() {
        m_needsDifferentiation = false;
    }

    /**
     *  Set the parent TVE indicator
     * @param parentTve
     */
    public void setOrigStmtId(int origStmtId) {
        m_origStmtId = origStmtId;
    }

    /**
     * @return parent TVE indicator
     */
    public int getOrigStmtId() {
        return m_origStmtId;
    }

    public void setTypeSizeAndInBytes(AbstractExpression typeSource) {
        setValueType(typeSource.getValueType());
        setValueSize(typeSource.getValueSize());
        m_inBytes = typeSource.getInBytes();
    }

    public void setTypeSizeAndInBytes(SchemaColumn typeSource) {
        setValueType(typeSource.getValueType());
        setValueSize(typeSource.getValueSize());
        m_inBytes = typeSource.getExpression().getInBytes();
    }

    private void setTypeSizeAndInBytes(Column typeSource) {
        setValueType(VoltType.get((byte)typeSource.getType()));
        setValueSize(typeSource.getSize());
        m_inBytes = typeSource.getInbytes();
    }

    @Override
    public boolean equals(Object obj) {
        if (! (obj instanceof TupleValueExpression)) {
            return false;
        }
        TupleValueExpression expr = (TupleValueExpression) obj;
        if (m_origStmtId != -1 && expr.m_origStmtId != -1) {
            // Implying both sides have statement id set
            // If one of the ids is not set it is considered to be a wild card
            // matching any other id.
            if (m_origStmtId != expr.m_origStmtId) {
                return false;
            }
        }

        if ((m_tableName == null) != (expr.m_tableName == null)) {
            return false;
        }
        if ((m_columnName == null) != (expr.m_columnName == null)) {
            return false;
        }
        if (m_tableName != null) { // Implying both sides non-null
            if (! m_tableName.equals(expr.m_tableName)) {
                return false;
            }
        }
        if (m_tableAlias != null && expr.m_tableAlias != null) {
            // Implying both sides non-null
            // If one of the table aliases is NULL it is considered to be a wild card
            // matching any alias.
            if (! m_tableAlias.equals(expr.m_tableAlias)) {
                return false;
            }
        }
        if (m_columnName != null) { // Implying both sides non-null
            return m_columnName.equals(expr.m_columnName);
        }
        // NOTE: m_ColumnIndex was ignored in comparison, because it might not get properly set
        // till end of planning, when query is made across several tables.
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
        return result + super.hashCode();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        stringer.keySymbolValuePair(Members.COLUMN_IDX, m_columnIndex);
        if (m_tableIdx > 0) {
            stringer.keySymbolValuePair(Members.TABLE_IDX, m_tableIdx);
        }
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj, StmtTableScan tableScan)
            throws JSONException {
        m_columnIndex = obj.getInt(Members.COLUMN_IDX);
        if (obj.has(Members.TABLE_IDX)) {
            m_tableIdx = obj.getInt(Members.TABLE_IDX);
        }
        if (tableScan != null) {
            m_tableAlias = tableScan.getTableAlias();
            m_tableName = tableScan.getTableName();
            m_columnName = tableScan.getColumnName(m_columnIndex);
        }
    }

    /**
     * Resolve a TVE in the context of the given table.  Since
     * this is a TVE, it is a leaf node in the expression tree.
     * We just look up the metadata from the table and copy it
     * here, to this object.
     */
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
        setTypeSizeAndInBytes(column);
    }

    /**
     * Given an input schema, resolve this TVE expression.
     */
    public int setColumnIndexUsingSchema(NodeSchema inputSchema) {
        int index = inputSchema.getIndexOfTve(this);
        if (index < 0) {
            //* enable to debug*/ System.out.println("DEBUG: setColumnIndex miss: " + this);
            //* enable to debug*/ System.out.println("DEBUG: setColumnIndex candidates: " + inputSchema);
            return index;
        }

        setColumnIndex(index);
        if (getValueType() == null) {
            // In case of sub-queries the TVE may not have its
            // value type and size resolved yet. Try to resolve it now
            SchemaColumn inputColumn = inputSchema.getColumn(index);
            setTypeSizeAndInBytes(inputColumn);
        }
        return index;
    }

    // Even though this function applies generally to expressions and tables
    // and not just to TVEs as such, it is somewhat TVE-related because TVEs
    // represent the points where expression trees depend on tables.
    public static AbstractExpression getOtherTableExpression(
            AbstractExpression expr, String tableAlias) {
        assert(expr != null);
        AbstractExpression retval = expr.getLeft();
        if (isOperandDependentOnTable(retval, tableAlias)) {
            retval = expr.getRight();
            assert( ! isOperandDependentOnTable(retval, tableAlias));
        }
        return retval;
    }

    // Even though this function applies generally to expressions and tables
    // and not just to TVEs as such, it is somewhat TVE-related because TVEs
    // represent the points where expression trees depend on tables.
    public static boolean isOperandDependentOnTable(AbstractExpression expr,
            String tableAlias) {
        assert(tableAlias != null);
        for (TupleValueExpression tve :
            ExpressionUtil.getTupleValueExpressions(expr)) {
            if (tableAlias.equals(tve.getTableAlias())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String explain(String impliedTableName) {
        String tableName = (m_tableAlias != null) ? m_tableAlias : m_tableName;
        String columnName = m_columnName;
        if (columnName == null || columnName.equals("")) {
            columnName = "column#" + m_columnIndex;
        }
        if (m_verboseExplainForDebugging) {
            columnName += " (as JSON: ";
            JSONStringer stringer = new JSONStringer();
            try {
                stringer.object();
                toJSONString(stringer);
                stringer.endObject();
                columnName += stringer.toString();
            } catch (Exception e) {
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
        } else if ( ! tableName.equals(impliedTableName)) {
            return tableName + "." + columnName;
        } else if (m_verboseExplainForDebugging) {
            // In verbose mode, always show an "implied' tableName that would normally be left off.
            return "{" + tableName + "}." + columnName;
        }
        return columnName;
    }

    private String chooseFromTwoNames(String name, String alias) {
        if (name == null) {
            if (alias == null) {
                return "<none>";
            }
            return "(" + alias + ")";
        }

        if (alias == null || name.equals(alias)) {
            return name;
        } else {
            return name + "(" + alias + ")";
        }
    }

    @Override
    protected String getExpressionNodeNameForToString() {
        return String.format("%s: %s.%s(index:%d, diff'tor:%d)",
                             super.getExpressionNodeNameForToString(),
                             chooseFromTwoNames(m_tableName, m_tableAlias),
                             chooseFromTwoNames(m_columnName, m_columnAlias),
                             m_columnIndex, m_differentiator);
    }

}
