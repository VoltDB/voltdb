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
public class SchemaColumn
{
    public enum Members {
        COLUMN_NAME,
        EXPRESSION,
    }

    /**
     * Create a new SchemaColumn
     * @param tableName  The name of the table where this column originated,
     *        if any.  Currently, internally created columns will be assigned
     *        the table name "VOLT_TEMP_TABLE" for disambiguation.
     * @param tableAlias  The alias assigned to this table, if any
     * @param columnName  The name of this column, if any
     * @param columnAlias  The alias assigned to this column, if any
     * @param expression  The input expression which generates this
     *        column.  SchemaColumn needs to have exclusive ownership
     *        so that it can adjust the index of any TupleValueExpressions
     *        without affecting other nodes/columns/plan iterations, so
     *        it clones this expression.
     */
    SchemaColumn(String tableName, String tableAlias, String columnName, String columnAlias) {
        m_tableName = tableName;
        m_tableAlias = tableAlias;
        m_columnName = columnName;
        m_columnAlias = columnAlias;
    }

    public SchemaColumn(String tableName, String tableAlias, String columnName,
                        String columnAlias, AbstractExpression expression)
    {
        m_tableName = tableName;
        m_tableAlias = tableAlias;
        m_columnName = columnName;
        m_columnAlias = columnAlias;
        if (expression != null) {
            m_expression = (AbstractExpression) expression.clone();
        }
    }

    /**
     * Clone a schema column
     */
    @Override
    protected SchemaColumn clone()
    {
        return new SchemaColumn(m_tableName, m_tableAlias, m_columnName, m_columnAlias,
                                m_expression);
    }

    /**
     * Check if this SchemaColumn provides the column specified by the input
     * arguments.  An equal match is defined as matching both the table name and
     * the column name if it is provided, otherwise matching the provided alias.
     */
    @Override
    public boolean equals (Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (obj instanceof SchemaColumn == false) return false;

        SchemaColumn sc = (SchemaColumn) obj;
        String tableName = sc.getTableName();
        String tableAlias = sc.getTableAlias();
        boolean sameTable = false;

        if (tableAlias != null) {
            if (tableAlias.equals(m_tableAlias)) {
                sameTable = true;
            }
        } else if (m_tableName.equals(tableName)) {
            sameTable = true;
        }

        if (! sameTable) {
            return false;
        }

        String columnName = sc.getColumnName();
        String columnAlias = sc.getColumnAlias();

        if (columnName != null && !columnName.equals("")) {
            if (columnName.equals(m_columnName)) {
                // Next line is not true according to current VoltDB's logic
                //assert(m_columnAlias.equals(columnAlias));
                return true;
            }
        }
        else if (columnAlias != null && !columnAlias.equals("")) {
            if (columnAlias.equals(m_columnAlias)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public int hashCode () {
        // based on implementation of equals
        int result = m_tableAlias != null ? m_tableAlias.hashCode() : m_tableName.hashCode();
        if (m_columnName != null && !m_columnName.equals("")) {
            result += m_columnName.hashCode();
        } else if (m_columnAlias != null && !m_columnAlias.equals("")) {
            result += m_columnAlias.hashCode();
        }
        return result;
    }


    /**
     * Return a copy of this SchemaColumn, but with the input expression
     * replaced by an appropriate TupleValueExpression.
     */
    public SchemaColumn copyAndReplaceWithTVE()
    {
        TupleValueExpression new_exp = null;
        if (m_expression instanceof TupleValueExpression)
        {
            new_exp = (TupleValueExpression) m_expression.clone();
        }
        else
        {
            new_exp = new TupleValueExpression(m_tableName, m_tableAlias, m_columnName, m_columnAlias);
            // XXX not sure this is right
            new_exp.setTypeSizeBytes(m_expression.getValueType(), m_expression.getValueSize(),
                    m_expression.getInBytes());
        }
        return new SchemaColumn(m_tableName, m_tableAlias, m_columnName, m_columnAlias,
                                new_exp);
    }

    public String getTableName()
    {
        return m_tableName;
    }

    public String getTableAlias()
    {
        return m_tableAlias;
    }

    public String getColumnName()
    {
        return m_columnName;
    }

    public String getColumnAlias()
    {
        return m_columnAlias;
    }

    public void reset(String tbName, String tbAlias, String colName, String colAlias) {
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

    public AbstractExpression getExpression()
    {
        return m_expression;
    }

    public VoltType getType()
    {
        return m_expression.getValueType();
    }

    public int getSize()
    {
        return m_expression.getValueSize();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SchemaColumn:\n");
        sb.append("\tTable Name: ").append(m_tableName).append("\n");
        sb.append("\tTable Alias: ").append(m_tableAlias).append("\n");
        sb.append("\tColumn Name: ").append(m_columnName).append("\n");
        sb.append("\tColumn Alias: ").append(m_columnAlias).append("\n");
        sb.append("\tColumn Type: ").append(getType()).append("\n");
        sb.append("\tColumn Size: ").append(getSize()).append("\n");
        sb.append("\tExpression: ").append(m_expression.toString()).append("\n");
        return sb.toString();
    }

    public void toJSONString(JSONStringer stringer, boolean finalOutput) throws JSONException
    {
        stringer.object();
        // Tell the EE that the column name is either a valid column
        // alias or the original column name if no alias exists.  This is a
        // bit hacky, but it's the easiest way for the EE to generate
        // a result set that has all the aliases that may have been specified
        // by the user (thanks to chains of setOutputTable(getInputTable))
        if (finalOutput) {
            if (getColumnAlias() != null && !getColumnAlias().equals(""))
            {
                stringer.key(Members.COLUMN_NAME.name()).value(getColumnAlias());
            }
            else if (getColumnName() != null) {
                stringer.key(Members.COLUMN_NAME.name()).value(getColumnName());
            }
        }

        if (m_expression != null) {
            stringer.key(Members.EXPRESSION.name());
            stringer.object();
            m_expression.toJSONString(stringer);
            stringer.endObject();
        }

        stringer.endObject();
    }

    public static SchemaColumn fromJSONObject(JSONObject jobj) throws JSONException
    {
        String tableName = "";
        String tableAlias = "";
        String columnName = "";
        String columnAlias = "";
        AbstractExpression expression = null;
        if( !jobj.isNull( Members.COLUMN_NAME.name() ) ){
            columnName = jobj.getString( Members.COLUMN_NAME.name() );
        }
        expression = AbstractExpression.fromJSONChild(jobj, Members.EXPRESSION.name());
        return new SchemaColumn( tableName, tableAlias, columnName, columnAlias, expression );
    }

    private String m_tableName;
    private String m_tableAlias;
    private String m_columnName;
    private String m_columnAlias;
    private AbstractExpression m_expression;
}
