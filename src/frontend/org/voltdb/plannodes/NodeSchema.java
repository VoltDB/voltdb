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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.voltdb.expressions.TupleValueExpression;

/**
 * This class encapsulates the representation and common operations for
 * a PlanNode's output schema.
 */
public class NodeSchema
{
    public NodeSchema()
    {
        m_columns = new ArrayList<SchemaColumn>();
    }

    /**
     * Add a column to this schema.
     *
     * Unless actively modified, updated, or sorted, the column order is
     * implicitly the order in which columns are added using this call
     */
    public void addColumn(SchemaColumn column)
    {
        m_columns.add(column);
    }

    /**
     * @return a list of the columns in this schema.  These columns will be
     * in the order in which they will appear at the output of this node.
     */
    public ArrayList<SchemaColumn> getColumns()
    {
        return m_columns;
    }

    public int size()
    {
        return m_columns.size();
    }

    /**
     * Retrieve the SchemaColumn that matches the provided arguments.
     * @param tableName
     * @param columnName
     * @param columnAlias
     * @return The matching SchemaColumn.  Returns null if the column wasn't
     *         found.
     */
    public SchemaColumn find(String tableName, String columnName, String columnAlias)
    {
        SchemaColumn retval = null;
        for (SchemaColumn col : m_columns)
        {
            if (col.matches(tableName, columnName, columnAlias))
            {
                retval = col;
                break;
            }
        }
        return retval;
    }

    /**
     * Get the offset into the schema of the column specified by the provided
     * arguments.
     * @param tableName
     * @param columnName
     * @param columnAlias
     * @return The offset of the specified column.  Returns -1 if the column
     *         wasn't found.
     */
    int getIndexOf(String tableName, String columnName, String columnAlias)
    {
        for (int i = 0; i < m_columns.size(); i++)
        {
            SchemaColumn col = m_columns.get(i);
            if (col.matches(tableName, columnName, columnAlias))
            {
                return i;
            }
        }
        return -1;
    }

    /** Convenience method for looking up the column offset for a TVE using
     *  getIndexOf().  This is a common operation because every TVE in every
     *  AbstractExpression in a plan node needs to have its column_idx updated
     *  during the column index resolution phase.
     */
    int getIndexOfTve(TupleValueExpression tve)
    {
        return getIndexOf(tve.getTableName(), tve.getColumnName(),
                          tve.getColumnAlias());
    }

    /** Convenience method to sort the SchemaColumns.  Only applies if they
     *  all are tuple value expressions.  Modification is made in-place.
     */
    void sortByTveIndex()
    {
        class TveColCompare implements Comparator<SchemaColumn>
        {
            @Override
            public int compare(SchemaColumn col1, SchemaColumn col2)
            {
                if (!(col1.getExpression() instanceof TupleValueExpression) ||
                    !(col2.getExpression() instanceof TupleValueExpression))
                {
                    throw new ClassCastException();
                }
                TupleValueExpression tve1 =
                    (TupleValueExpression) col1.getExpression();
                TupleValueExpression tve2 =
                    (TupleValueExpression) col2.getExpression();
                if (tve1.getColumnIndex() < tve2.getColumnIndex())
                {
                    return -1;
                }
                else if (tve1.getColumnIndex() > tve2.getColumnIndex())
                {
                    return 1;
                }
                return 0;
            }
        }

        Collections.sort(m_columns, new TveColCompare());
    }

    public NodeSchema clone()
    {
        NodeSchema copy = new NodeSchema();
        for (int i = 0; i < m_columns.size(); ++i)
        {
            copy.addColumn((SchemaColumn)m_columns.get(i).clone());
        }
        return copy;
    }

    /**
     * Returns a copy of this NodeSchema but with all non-TVE expressions
     * replaced with an appropriate TVE.  This is used primarily when generating
     * a node's output schema based on its childrens' schema; we want to
     * carry the columns across but leave any non-TVE expressions behind.
     */
    NodeSchema copyAndReplaceWithTVE()
    {
        NodeSchema copy = new NodeSchema();
        for (int i = 0; i < m_columns.size(); ++i)
        {
            copy.addColumn(m_columns.get(i).copyAndReplaceWithTVE());
        }
        return copy;
    }

    /**
     * Combine the provided schema to this schema and return the result
     * as a new schema.
     */
    NodeSchema join(NodeSchema schema)
    {
        NodeSchema copy = null;
        copy = schema.clone();
        for (int i = 0; i < m_columns.size(); ++i)
        {
            copy.addColumn((SchemaColumn)m_columns.get(i).clone());
        }
        return copy;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("NodeSchema:\n");
        for (int i = 0; i < m_columns.size(); ++i)
        {
            sb.append("Column " + i + ":\n");
            sb.append(m_columns.get(i).toString()).append("\n");
        }
        return sb.toString();
    }

    private ArrayList<SchemaColumn> m_columns;
}
