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

package org.voltdb.plannodes;

import org.json_voltpatches.JSONException;
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
        TABLE_NAME,
        COLUMN_NAME,
        COLUMN_ALIAS,
        EXPRESSION,
        TYPE,
        SIZE;
    }

    /**
     * Create a new SchemaColumn
     * @param tableName  The name of the table where this column originated,
     *        if any.  Currently, internally created columns will be assigned
     *        the table name "VOLT_TEMP_TABLE" for disambiguation.
     * @param columnName  The name of this column, if any
     * @param columnAlias  The alias assigned to this column, if any
     * @param expression  The input expression which generates this
     *        column.  SchemaColumn needs to have exclusive ownership
     *        so that it can adjust the index of any TupleValueExpressions
     *        without affecting other nodes/columns/plan iterations, so
     *        it clones this expression.
     */
    public SchemaColumn(String tableName, String columnName,
                        String columnAlias, AbstractExpression expression)
    {
        m_tableName = tableName;
        m_columnName = columnName;
        m_columnAlias = columnAlias;
        try
        {
            m_expression = (AbstractExpression) expression.clone();
        }
        catch (CloneNotSupportedException e)
        {
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * Clone a schema column
     */
    protected SchemaColumn clone()
    {
        return new SchemaColumn(m_tableName, m_columnName, m_columnAlias,
                                m_expression);
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
            try
            {
                new_exp = (TupleValueExpression) m_expression.clone();
            }
            catch (CloneNotSupportedException e)
            {
                throw new RuntimeException(e.getMessage());
            }
        }
        else
        {
            new_exp = new TupleValueExpression();
            // XXX not sure this is right
            new_exp.setTableName(m_tableName);
            new_exp.setColumnName(m_columnName);
            new_exp.setColumnAlias(m_columnAlias);
            new_exp.setValueType(m_expression.getValueType());
            new_exp.setValueSize(m_expression.getValueSize());
        }
        return new SchemaColumn(m_tableName, m_columnName, m_columnAlias,
                                new_exp);
    }

    public String getTableName()
    {
        return m_tableName;
    }

    public String getColumnName()
    {
        return m_columnName;
    }

    public String getColumnAlias()
    {
        return m_columnAlias;
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

    /**
     * Check if this SchemaColumn provides the column specified by the input
     * arguments.  A match is defined as matching both the table name and
     * the column name if it is provided, otherwise matching the provided alias.
     * @param tableName
     * @param columnName
     * @param columnAlias
     * @return
     */
    public boolean matches(String tableName, String columnName,
                           String columnAlias)
    {
        boolean retval = false;
        if (tableName.equals(m_tableName))
        {
            if (columnName != null && !columnName.equals(""))
            {
                if (columnName.equals(m_columnName))
                {
                    retval = true;
                }
            }
            else if (columnAlias != null && !columnAlias.equals(""))
            {
                if (columnAlias.equals(m_columnAlias))
                {
                    retval = true;
                }
            }
            else
            {
                throw new RuntimeException("Attempted to match a SchemaColumn " +
                                           "but provided no name or alias.");
            }
        }
        return retval;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SchemaColumn:\n");
        sb.append("\tTable Name: ").append(m_tableName).append("\n");
        sb.append("\tColumn Name: ").append(m_columnName).append("\n");
        sb.append("\tColumn Alias: ").append(m_columnAlias).append("\n");
        sb.append("\tColumn Type: ").append(getType()).append("\n");
        sb.append("\tColumn Size: ").append(getSize()).append("\n");
        sb.append("\tExpression: ").append(m_expression.toString()).append("\n");
        return sb.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        stringer.object();
        if (getTableName() != null) {
            stringer.key(Members.TABLE_NAME.name()).value(getTableName());
        }
        else
        {
            stringer.key(Members.TABLE_NAME.name()).value("");
        }

        // Tell the EE that the column name is either a valid column
        // alias or the original column name if no alias exists.  This is a
        // bit hacky, but it's the easiest way for the EE to generate
        // a result set that has all the aliases that may have been specified
        // by the user (thanks to chains of setOutputTable(getInputTable))
        if (getColumnAlias() != null && !getColumnAlias().equals(""))
        {
            stringer.key(Members.COLUMN_NAME.name()).value(getColumnAlias());
        }
        else if (getColumnName() != null) {
            stringer.key(Members.COLUMN_NAME.name()).value(getColumnName());
        }
        else
        {
            stringer.key(Members.COLUMN_NAME.name()).value("");
        }

        if (getColumnAlias() != null) {
            stringer.key(Members.COLUMN_ALIAS.name()).value(getColumnAlias());
        }
        else
        {
            stringer.key(Members.COLUMN_ALIAS.name()).value("");
        }

        if (m_expression != null) {
            stringer.key(Members.EXPRESSION.name());
            stringer.object();
            m_expression.toJSONString(stringer);
            stringer.endObject();
        }
        else
        {
            stringer.key(Members.EXPRESSION.name()).value("");
        }
        stringer.key(Members.TYPE.name()).value(getType().name());
        stringer.key(Members.SIZE.name()).value(getSize());

        stringer.endObject();
    }

    private String m_tableName;
    private String m_columnName;
    private String m_columnAlias;
    private AbstractExpression m_expression;
}
