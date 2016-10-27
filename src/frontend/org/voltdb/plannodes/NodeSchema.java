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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;

/**
 * This class encapsulates the representation and common operations for
 * a PlanNode's output schema.
 */
public class NodeSchema {
    // Sometimes there are columns with identical names within a given table
    // and its schema.  We want to be able to differentiate these columns so that
    // m_columnsMapHelper can produce the right offset for columns that have the same
    // name but are physically different.  We use the "differentiator" (an attribute
    // of a TVE) to do this.
    private static final Comparator<SchemaColumn> BY_NAME =
            new Comparator<SchemaColumn>() {
        @Override
        public int compare(SchemaColumn col1, SchemaColumn col2) {
            int nameCompare = col1.compareNames(col2);
            if (nameCompare != 0) {
                return nameCompare;
            }

            return col1.getDifferentiator() - col2.getDifferentiator();
        }
    };

    // The list of columns produced by a plan node, in storage order.
    private final ArrayList<SchemaColumn> m_columns;

    // A helpful map that goes from a schema column to the columns index in the list.
    private final TreeMap<SchemaColumn, Integer> m_columnsMapHelper;

    public NodeSchema() {
        m_columns = new ArrayList<SchemaColumn>();
        m_columnsMapHelper = new TreeMap<>(BY_NAME);
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
    public void addColumn(SchemaColumn column) {
        int size = m_columns.size();
        m_columnsMapHelper.put(column, size);
        m_columns.add(column);
    }

    public void addColumn(String tableName, String tableAlias,
            String columnName, String columnAlias,
            AbstractExpression expression) {
        SchemaColumn scol = new SchemaColumn(
                tableName, tableAlias,
                columnName, columnAlias,
                expression);
        addColumn(scol);
    }

    public void addColumn(String tableName, String tableAlias,
            String columnName, String columnAlias,
            AbstractExpression expression,
            int differentiator) {
        SchemaColumn scol = new SchemaColumn(
                tableName, tableAlias,
                columnName, columnAlias,
                expression, differentiator);
        addColumn(scol);
    }


    /**
     * @return a list of the columns in this schema.  These columns will be
     * in the order in which they will appear at the output of this node.
     */
    public ArrayList<SchemaColumn> getColumns() { return m_columns; }

    public int size() { return m_columns.size(); }

    /**
     * Retrieve the SchemaColumn that matches the provided arguments.
     * @param tableName
     * @param tableAlias
     * @param columnName
     * @param columnAlias
     * @return The matching SchemaColumn.  Returns null if the column wasn't
     *         found.
     */
    public SchemaColumn find(String tableName, String tableAlias,
            String columnName, String columnAlias) {
        SchemaColumn col = new SchemaColumn(tableName, tableAlias,
                columnName, columnAlias);
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
        SchemaColumnFloor(SchemaColumn column) {
            super(column.getTableName(), column.getTableAlias(),
                    column.getColumnName(), column.getColumnAlias(),
                    column.getExpression());
        }

        @Override
        public int getDifferentiator() { return -1; }
    }

    private int findIndexOfColumn(SchemaColumn column) {
        SchemaColumn floorSchemaColumn = new SchemaColumnFloor(column);
        SortedMap<SchemaColumn, Integer> submap =
                m_columnsMapHelper.tailMap(floorSchemaColumn);
        int index = -1;

        // If more than one column in this NodeSchema has the same name of the column
        // we're looking for, then we "break the tie" and prefer the one with a matching
        // differentiator field.
        for (Map.Entry<SchemaColumn, Integer> entry : submap.entrySet()) {
            SchemaColumn key = entry.getKey();
            if (key.compareNames(column) != 0) {
                break;
            }

            index = entry.getValue();
            if (column.getDifferentiator() == key.getDifferentiator()) {
                // An exact match
                break;
            }
        }

        return index;
    }

    /** Convenience method for looking up the column offset for a TVE using
     *  getIndexOf().  This is a common operation because every TVE in every
     *  AbstractExpression in a plan node needs to have its column_idx updated
     *  during the column index resolution phase.
     */
    public int getIndexOfTve(TupleValueExpression tve) {
        SchemaColumn column = new SchemaColumn(
                tve.getTableName(), tve.getTableAlias(),
                tve.getColumnName(), tve.getColumnAlias(),
                tve, tve.getDifferentiator());
        return findIndexOfColumn(column);
    }

    private static final Comparator<SchemaColumn> TVE_IDX_COMPARE =
            new Comparator<SchemaColumn>() {
        @Override
        public int compare(SchemaColumn col1, SchemaColumn col2) {
            TupleValueExpression tve1 =
                (TupleValueExpression) col1.getExpression();
            TupleValueExpression tve2 =
                (TupleValueExpression) col2.getExpression();

            int colIndex1 = tve1.getColumnIndex();
            int colIndex2 = tve2.getColumnIndex();

            return (colIndex1 < colIndex2) ? -1 :
                (colIndex1 > colIndex2) ? 1 : 0;
        }
    };

    /**
     * Sort schema columns by TVE index.  All elements
     * must be TupleValueExpressions.  Modification is made in-place.
     */
    void sortByTveIndex() { Collections.sort(m_columns, TVE_IDX_COMPARE); }

    /**
     * Sort a sub-range of the schema columns by TVE index.  All elements
     * must be TupleValueExpressions.  Modification is made in-place.
     * @param fromIndex   lower bound of range to be sorted, inclusive
     * @param toIndex     upper bound of range to be sorted, exclusive
     */
    void sortByTveIndex(int fromIndex, int toIndex) {
        Collections.sort(m_columns.subList(fromIndex, toIndex), TVE_IDX_COMPARE);
    }

    @Override
    public NodeSchema clone() {
        NodeSchema copy = new NodeSchema();
        for (SchemaColumn column : m_columns) {
            copy.addColumn(column.clone());
        }
        return copy;
    }

    public NodeSchema replaceTableClone(String tableAlias) {
        NodeSchema copy = new NodeSchema();
        for (int colIndex = 0; colIndex < m_columns.size(); ++colIndex) {
            SchemaColumn column = m_columns.get(colIndex);
            String colAlias = column.getColumnAlias();
            int differentiator = column.getDifferentiator();

            TupleValueExpression tve = new TupleValueExpression(
                    tableAlias, tableAlias, colAlias, colAlias,
                    colIndex, differentiator);
            tve.setTypeSizeAndInBytes(column);
            copy.addColumn(tableAlias, tableAlias,
                    colAlias, colAlias,
                    tve, differentiator);
        }

        return copy;
    }

    @Override
    public boolean equals (Object obj) {
        if (obj == null) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        if (obj instanceof NodeSchema == false) {
            return false;
        }

        NodeSchema schema = (NodeSchema) obj;

        if (schema.size() != size()) {
            return false;
        }

        ArrayList<SchemaColumn> columns = schema.getColumns();

        for (int colIndex = 0; colIndex < size(); colIndex++ ) {
            SchemaColumn col1 = columns.get(colIndex);
            if ( ! col1.equals(m_columns.get(colIndex))) {
                return false;
            }
        }
        return true;
    }

    // Similar to the equals method above, but consider SchemaColumn objects as equal if their
    // names are the same.  Don't worry about the differentiator field.
    public boolean equalsOnlyNames(NodeSchema otherSchema) {
        if (otherSchema == null) {
            return false;
        }

        if (otherSchema.size() != size()) {
            return false;
        }

        ArrayList<SchemaColumn> columns = otherSchema.getColumns();
        for (int colIndex = 0; colIndex < size(); colIndex++ ) {
            SchemaColumn col1 = columns.get(colIndex);
            SchemaColumn col2 = m_columns.get(colIndex);
            if (col1.compareNames(col2) != 0) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (SchemaColumn column : m_columns) {
            result += column.hashCode();
        }
        return result;
    }

    /**
     * Returns a copy of this NodeSchema but with all non-TVE expressions
     * replaced with an appropriate TVE.  This is used primarily when generating
     * a node's output schema based on its childrens' schema; we want to
     * carry the columns across but leave any non-TVE expressions behind.
     */
    NodeSchema copyAndReplaceWithTVE() {
        NodeSchema copy = new NodeSchema();
        int colIndex = 0;
        for (SchemaColumn column : m_columns) {
            copy.addColumn(column.copyAndReplaceWithTVE(colIndex));
            ++colIndex;
        }
        return copy;
    }

    /**
     * Append the provided schema to this schema and return the result
     * as a new schema. Columns order: [this][provided schema columns].
     */
    NodeSchema join(NodeSchema schema) {
        NodeSchema copy = this.clone();
        for (SchemaColumn column: schema.getColumns()) {
            copy.addColumn(column.clone());
        }
        return copy;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("NodeSchema:\n");
        for (int colIndex = 0; colIndex < m_columns.size(); ++colIndex) {
            sb.append("Column " + colIndex + ":\n");
            sb.append(m_columns.get(colIndex).toString()).append("\n");
        }
        return sb.toString();
    }

    public String toExplainPlanString() {
        StringBuilder sb = new StringBuilder();
        String separator = "schema: {";
        for (SchemaColumn column : m_columns) {
            String colAsString = column.toString();
            sb.append(separator).append(colAsString);
            separator = ", ";
        }
        sb.append("}");
        return sb.toString();
    }

    public void addAllSubexpressionsOfClassFromNodeSchema(
            Set<AbstractExpression> exprs,
            Class<? extends AbstractExpression> aeClass) {
        for (SchemaColumn column : getColumns()) {
            AbstractExpression colExpr = column.getExpression();
            if (colExpr == null) {
                continue;
            }
            Collection<AbstractExpression> found =
                    colExpr.findAllSubexpressionsOfClass(aeClass);
            exprs.addAll(found);
        }
    }

}
