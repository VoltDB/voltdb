/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanningErrorException;

/**
 * This class encapsulates the representation and common operations for
 * a PlanNode's output schema.
 */
public class NodeSchema
{
    // Sometimes there are columns with identical names within a given table
    // and its schema.  We want to be able to differentiate these columns so that
    // m_columnsMapHelper can produce the right offset for columns that have the same
    // name but are physically different.  We use the "differentiator" (an attribute
    // of a TVE) to do this.
    private static class SchemaColumnComparator implements Comparator<SchemaColumn> {

        @Override
        public int compare(SchemaColumn col1, SchemaColumn col2) {
            int nameCompare = col1.compareNames(col2);
            if (nameCompare != 0) {
                return nameCompare;

            }

            return col1.getDifferentiator() - col2.getDifferentiator();
        }
    }

    // The list of columns produced by a plan node, in storage order.
    private ArrayList<SchemaColumn> m_columns;

    // A helpful map that goes from a schema column to the columns index in the list.
    private TreeMap<SchemaColumn, Integer> m_columnsMapHelper;

    public NodeSchema()
    {
        m_columns = new ArrayList<SchemaColumn>();
        m_columnsMapHelper = new TreeMap<>(new SchemaColumnComparator());
    }

    /**
     * Add a column to this schema.
     *
     * Unless actively modified, updated, or sorted, the column order is
     * implicitly the order in which columns are added using this call.
     *
     * Note that it's possible to add the same column to a schema more than once.
     * In this case we replace the old entry for the column in the map (so it will
     * stay the same size), the column list will grow by one, and the updated map entry
     * will point the the second instance of the column in the list.
     */
    public void addColumn(SchemaColumn column)
    {
        int size = m_columns.size();
        m_columnsMapHelper.put(column, size);
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
     * @param tableAlias
     * @param columnName
     * @param columnAlias
     * @return The matching SchemaColumn.  Returns null if the column wasn't
     *         found.
     */
    public SchemaColumn find(String tableName, String tableAlias, String columnName, String columnAlias)
    {
        SchemaColumn col = new SchemaColumn(tableName, tableAlias, columnName, columnAlias);
        int index = findIndexOfColumn(col);
        if (index != -1) {
            return m_columns.get(index);
        }
        return null;
    }

    /**
     * A subclass of SchemaColumn that always returns -1 for the differentiator,
     * meaning that it will sort lowest among schema columns with the same names.
     */
    private static class SchemaColumnFloor extends SchemaColumn {
        SchemaColumnFloor(SchemaColumn col) {
            super(col.getTableName(), col.getTableAlias(), col.getColumnName(), col.getColumnAlias(), col.getExpression());
        }

        @Override
        public int getDifferentiator() {
            return -1;
        }
    }

    private int findIndexOfColumn(SchemaColumn col) {
        SchemaColumn floorSchemaColumn = new SchemaColumnFloor(col);
        SortedMap<SchemaColumn, Integer> submap = m_columnsMapHelper.tailMap(floorSchemaColumn);
        int index = -1;
        int numMatchesFound = 0;
        for (Map.Entry<SchemaColumn, Integer> entry : submap.entrySet()) {
            if (entry.getKey().compareNames(col) == 0) {
                ++numMatchesFound;
                index = entry.getValue();
            }
            else {
                break;
            }
        }

        if (numMatchesFound > 1) {
            // Subqueries with joins can produce intermediate tables containing
            // columns with the same names.  Referred to explicitly, an "ambiguous
            // column" error will be produced.  But it's still possible to reference them
            // with "SELECT * ...".  This is standard SQL but problematic for VoltDB, since
            // column resolution is complex and happens based on names.
            throw new PlanningErrorException("This combination of \"SELECT * ...\" "
                    + "and subqueries is not supported.");
        }

        return index;
    }

    /** Convenience method for looking up the column offset for a TVE using
     *  getIndexOf().  This is a common operation because every TVE in every
     *  AbstractExpression in a plan node needs to have its column_idx updated
     *  during the column index resolution phase.
     */
    public int getIndexOfTve(TupleValueExpression tve)
    {
        SchemaColumn col = new SchemaColumn(tve.getTableName(), tve.getTableAlias(),
                tve.getColumnName(), tve.getColumnAlias(), tve);

        return findIndexOfColumn(col);
    }

    /**
     * Sort schema columns by TVE index.  All elements
     * must be TupleValueExpressions.  Modification is made in-place.
     */
    void sortByTveIndex() {
        sortByTveIndex(0, size());
    }

    /**
     * Sort a sub-range of the schema columns by TVE index.  All elements
     * must be TupleValueExpressions.  Modification is made in-place.
     * @param fromIndex   lower bound of range to be sorted, inclusive
     * @param toIndex     upper bound of range to be sorted, exclusive
     */
    void sortByTveIndex(int fromIndex, int toIndex)
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

        if (fromIndex == 0 && toIndex == size()) {
            Collections.sort(m_columns, new TveColCompare());
        }
        else {
            Collections.sort(m_columns.subList(fromIndex, toIndex), new TveColCompare());
        }
    }

    @Override
    public NodeSchema clone()
    {
        NodeSchema copy = new NodeSchema();
        for (int i = 0; i < m_columns.size(); ++i)
        {
            copy.addColumn(m_columns.get(i).clone());
        }
        return copy;
    }

    public NodeSchema replaceTableClone(String tableAlias) {
        NodeSchema copy = new NodeSchema();
        for (int i = 0; i < m_columns.size(); ++i)
        {
            SchemaColumn col = m_columns.get(i);
            String colAlias = col.getColumnAlias();

            TupleValueExpression tve = new TupleValueExpression(tableAlias, tableAlias, colAlias, colAlias, i);
            tve.setDifferentiator(col.getDifferentiator());
            tve.setTypeSizeBytes(col.getType(), col.getSize(), col.getExpression().getInBytes());
            SchemaColumn sc = new SchemaColumn(tableAlias, tableAlias, colAlias, colAlias, tve);
            copy.addColumn(sc);
        }

        return copy;
    }

    @Override
    public boolean equals (Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (obj instanceof NodeSchema == false) return false;

        NodeSchema schema = (NodeSchema) obj;

        if (schema.size() != size()) return false;

        ArrayList<SchemaColumn> columns = schema.getColumns();

        for (int i =0; i < size(); i++ ) {
            SchemaColumn col1 = columns.get(i);
            if (!col1.equals(m_columns.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode () {
        int result = 0;
        for (SchemaColumn col: m_columns) {
            result += col.hashCode();
        }
        return result;
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
     * Append the provided schema to this schema and return the result
     * as a new schema. Columns order: [this][provided schema columns].
     */
    NodeSchema join(NodeSchema schema)
    {
        NodeSchema copy = this.clone();
        for (SchemaColumn col: schema.getColumns())
        {
            copy.addColumn(col.clone());
        }
        return copy;
    }

    @Override
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

    public String toExplainPlanString() {
        StringBuffer sb = new StringBuffer();

        String separator = "schema: {";
        for (SchemaColumn col : m_columns) {
            sb.append(separator);
            sb.append(col.toExplainPlanString());

            separator = ", ";
        }
        sb.append("}");

        return sb.toString();
    }
}

