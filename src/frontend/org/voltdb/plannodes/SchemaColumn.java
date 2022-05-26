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
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;

/**
 * This class encapsulates the data and operations needed to track columns
 * in the planner.
 *
 */
public class SchemaColumn {
    private static class Members {
        static final String COLUMN_NAME = "COLUMN_NAME";
        static final String EXPRESSION = "EXPRESSION";
    }

    private String m_tableName;
    private String m_tableAlias;
    private String m_columnName;
    private String m_columnAlias;
    private AbstractExpression m_expression;
    private int m_differentiator = -1;

    /**
     * Create a new SchemaColumn
     * @param tableName  The name of the table where this column originated,
     *        if any.  Currently, internally created columns will be assigned
     *        the table name AbstractParsedStmt.TEMP_TABLE_NAME for disambiguation.
     * @param tableAlias  The alias assigned to this table, if any
     * @param columnName  The name of this column, if any
     * @param columnAlias  The alias assigned to this column, if any
     * @param expression  The input expression which generates this
     *        column.  SchemaColumn needs to have exclusive ownership
     *        so that it can adjust the index of any TupleValueExpressions
     *        without affecting other nodes/columns/plan iterations, so
     *        it clones this expression.
     *
     * Some callers seem to provide an empty string instead of a null.  We change the
     * empty string to null to simplify the comparison functions.
     */
    SchemaColumn(String tableName, String tableAlias,
            String columnName, String columnAlias) {
        m_tableName = tableName == null || tableName.equals("") ? null : tableName;
        m_tableAlias = tableAlias == null || tableAlias.equals("") ? null : tableAlias;
        m_columnName = columnName == null || columnName.equals("") ? null : columnName;
        m_columnAlias = columnAlias == null || columnAlias.equals("") ? null : columnAlias;
    }

    public SchemaColumn(String tableName, String tableAlias,
            String columnName, String columnAlias,
            AbstractExpression expression) {
        this(tableName, tableAlias, columnName, columnAlias);
        if (expression != null) {
            m_expression = expression.clone();
        }
    }

    public SchemaColumn(String tableName, String tableAlias,
            String columnName, String columnAlias,
            AbstractExpression expression, int differentiator) {
        this(tableName, tableAlias, columnName, columnAlias, expression);
        m_differentiator = differentiator;
    }

    /**
     * Clone a schema column
     */
    @Override
    protected SchemaColumn clone() {
        return new SchemaColumn(m_tableName, m_tableAlias,
                m_columnName, m_columnAlias,
                m_expression, m_differentiator);
    }

    /**
     * Determine if this object is equal to another based on a comparison
     * of the table and column names or aliases.  See compareNames below
     * for details.
     */
    @Override
    public boolean equals (Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (obj instanceof SchemaColumn == false) {
            return false;
        }

        SchemaColumn sc = (SchemaColumn) obj;
        if (compareNames(sc) != 0) {
            return false;
        }

        return getDifferentiator() == sc.getDifferentiator();
    }

    private int nullSafeStringCompareTo(String str1, String str2) {
        if (str1 == null ^ str2 == null) {
            return str1 == null ? -1 : 1;
        }

        if (str1 == null && str2 == null) {
            return 0;
        }

        return str1.compareTo(str2);
    }

    /**
     * Compare this schema column to the input.
     *
     * Two SchemaColumns are compared thus:
     *
     * -  Compare the table aliases or names, preferring to compare aliases if
     *    not null for both sides.
     * -  Compare the column names or aliases, preferring to compare names if
     *    not null for both sides.
     */
    public int compareNames(SchemaColumn that) {

        String thatTbl;
        String thisTbl;
        if (m_tableAlias != null && that.m_tableAlias != null) {
            thisTbl = m_tableAlias;
            thatTbl = that.m_tableAlias;
        }
        else {
            thisTbl = m_tableName;
            thatTbl = that.m_tableName;
        }

        int tblCmp = nullSafeStringCompareTo(thisTbl, thatTbl);
        if (tblCmp != 0) {
            return tblCmp;
        }

        String thisCol;
        String thatCol;
        if (m_columnName != null && that.m_columnName != null) {
            thisCol = m_columnName;
            thatCol = that.m_columnName;
        }
        else {
            thisCol = m_columnAlias;
            thatCol = that.m_columnAlias;
        }

        int colCmp = nullSafeStringCompareTo(thisCol, thatCol);
        return colCmp;
    }

    @Override
    public int hashCode () {
        // based on implementation of equals
        int result = m_tableAlias != null ?
                m_tableAlias.hashCode() :
                m_tableName.hashCode();
        if (m_columnName != null && !m_columnName.equals("")) {
            result += m_columnName.hashCode();
        }
        else if (m_columnAlias != null && !m_columnAlias.equals("")) {
            result += m_columnAlias.hashCode();
        }

        result += m_differentiator;
        return result;
    }


    /**
     * Return a copy of this SchemaColumn, but with the input expression
     * replaced by an appropriate TupleValueExpression.
     * @param colIndex
     */
    public SchemaColumn copyAndReplaceWithTVE(int colIndex) {
        TupleValueExpression newTve;
        if (m_expression instanceof TupleValueExpression) {
            newTve = (TupleValueExpression) m_expression.clone();
            newTve.setColumnIndex(colIndex);
        }
        else {
            newTve = new TupleValueExpression(m_tableName, m_tableAlias,
                    m_columnName, m_columnAlias,
                    m_expression, colIndex);
        }
        return new SchemaColumn(m_tableName, m_tableAlias,
                m_columnName, m_columnAlias,
                newTve, m_differentiator);
    }

    public String getTableName() { return m_tableName; }

    public String getTableAlias() { return m_tableAlias; }

    public String getColumnName() { return m_columnName; }

    public String getColumnAlias() { return m_columnAlias; }

    public void reset(String tbName, String tbAlias,
            String colName, String colAlias) {
        m_tableName = tbName;
        m_tableAlias = tbAlias;
        m_columnName = colName;
        m_columnAlias = colAlias;

        if (m_expression instanceof TupleValueExpression) {
            TupleValueExpression tve = (TupleValueExpression) m_expression;
            tve.setTableName(m_tableName);
            tve.setTableAlias(m_tableAlias);
            tve.setColumnName(m_columnName);
            tve.setColumnAlias(m_columnAlias);
        }
    }

    public AbstractExpression getExpression() { return m_expression; }

    public VoltType getValueType() { return m_expression.getValueType(); }

    public void setValueType(VoltType type) { m_expression.setValueType(type); }

    public int getValueSize() { return m_expression.getValueSize(); }

    public void setValueSize(int size) { m_expression.setValueSize(size); }

    public boolean getInBytes() { return m_expression.getInBytes(); }

    public void setInBytes(boolean inBytes) { m_expression.setInBytes(inBytes); }

    /**
     * Return the differentiator that can distinguish columns with the same name.
     * This value is just the ordinal position of the SchemaColumn within its NodeSchema.
     * @return  differentiator for this schema column
     */
    public int getDifferentiator() { return m_differentiator; }

    /**
     * Set the differentiator value for this SchemaColumn.
     * @param differentiator
     */
    public void setDifferentiator(int differentiator) {
        m_differentiator = differentiator;
    }

    public void toJSONString(JSONStringer stringer, boolean finalOutput, int colNo)
            throws JSONException {
        stringer.object();
        // Tell the EE that the column name is either a valid column
        // alias or the original column name if no alias exists.  This is a
        // bit hacky, but it's the easiest way for the EE to generate
        // a result set that has all the aliases that may have been specified
        // by the user (thanks to chains of setOutputTable(getInputTable))
        if (finalOutput) {
            String columnName = getColumnAlias();
            if (columnName == null || columnName.equals("")) {
                columnName = getColumnName();
                if (columnName == null) {
                    columnName = "C" + colNo;
                }
            }
            stringer.keySymbolValuePair(Members.COLUMN_NAME, columnName);
        }

        if (m_expression != null) {
            stringer.key(Members.EXPRESSION).object();
            m_expression.toJSONString(stringer);
            stringer.endObject();
        }

        stringer.endObject();
    }

    public static SchemaColumn fromJSONObject(JSONObject jobj)
            throws JSONException {
        String tableName = null;
        String tableAlias = null;
        String columnName = null;
        String columnAlias = null;
        AbstractExpression expression = null;
        if ( ! jobj.isNull(Members.COLUMN_NAME)){
            columnName = jobj.getString(Members.COLUMN_NAME);
        }
        expression = AbstractExpression.fromJSONChild(jobj, Members.EXPRESSION);
        return new SchemaColumn(tableName, tableAlias,
                columnName, columnAlias, expression);
    }

    /**
     * Generates a string that can appear in "explain plan" output
     * when detailed debug output is enabled.
     * @return a string that represents this SchemaColumn
     */
    @Override
    public String toString() {
        String str = "";
        String table = getTableAlias();
        if (table == null) {
            table = getTableName();
        }
        if (table == null) {
            table = "<null>";
        }
        str += table;

        str += ".";
        if (getColumnName() != null) {
            str += getColumnName();
        }
        else if (getColumnAlias() != null) {
            str += getColumnAlias();
        }
        else {
            str += "<null>";
        }

        if (m_expression != null) {
            str += " expr: (";
            if (m_expression.getValueType() != null) {
                VoltType vt = m_expression.getValueType();
                String typeStr = vt.toSQLString();
                if (vt.isVariableLength()) {
                    boolean inBytes = m_expression.getInBytes();
                    typeStr += "(" + m_expression.getValueSize()
                    + (inBytes ? " bytes" : " chars") + ")";
                }
                str += "[" + typeStr + "] ";
            }
            else {
                str += "[!!! value type:NULL !!!] ";
            }

            str += m_expression.explain(table) + ")";

            if (m_expression instanceof TupleValueExpression) {
                int tveIndex = ((TupleValueExpression)m_expression).getColumnIndex();
                str += " index: " + tveIndex;
            }
        }

        return str;
    }
}
