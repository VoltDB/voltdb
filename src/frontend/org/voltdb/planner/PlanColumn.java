/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.planner;

import org.json.JSONException;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;

/**
 * A PlanColumn organizes all of the schema and meta-data about a column that is
 * required by the planner. Each plan node has an output column list of
 * PlanColumns specifying its output schema. Column value types are stored in
 * the column's associated expression.
 *
 * Once the AST objects generated from the parsed HSQL are processed into plan
 * nodes, all column references should be managed as PlanColumn GUIDs.
 *
 * Eventually, AbstractExpressions should reference column GUIDs allowing
 * walking the expression should lead to the GUIDs for the expression input
 * columns - those inputs are sometimes referred to as origin columns in
 * comments and variable names.
 */
public class PlanColumn
{

    public enum Members {
        GUID,
        TYPE,
        SIZE,
        NAME,
        INPUT_COLUMN_NAME, //For output columns, what was the name of the column in the input that it maps to
        INPUT_TABLE_NAME,
        EXPRESSION;
    }

    public enum SortOrder {
        kAscending,
        kDescending,
        kUnsorted
    };

    public enum Storage {
        kPartitioned,
        kReplicated,
        kTemporary,   // column in an intermediate table
        kUnknown      // TODO: eliminate this and make all columns known
    };

    /**
     * Globally unique id identifying this column
     */
    final int m_guid;

    /**
     * Columns may be derived from other columns by expressions. For example,
     * c = a + b. Or c = a + 1. Or c = a where (a < b). The creating expression,
     * if any, is given. Those expressions in turn contain information (in
     * TupleValueExpression nodes, e.g.) about "origin columns", from which
     * this column may be derived.
     */
    final AbstractExpression m_expression;

    /**
     * The sort order. This sort order may have been established by an ORDER BY
     * statement or as a result of the column's construction (scan of sorted
     * table column, for example).
     */
    final SortOrder m_sortOrder;

    /**
     * Partitioned, replicated or intermediate table column
     */
    final Storage m_storage;

    /**
     * Column's display name. If an output column, this will be the alias if an
     * alias was present in the SQL expression.
     */
    final String m_displayName;

    //
    // Accessors: all return copies or immutable values
    //

    public String displayName() {
        return new String(m_displayName);
    }

    public String originTableName() {
        TupleValueExpression tve = null;
        if ((m_expression instanceof TupleValueExpression) == true)
            tve = (TupleValueExpression)m_expression;
        else
            return null;

        if (tve.getTableName() != null)
            return new String(tve.getTableName());
        else
            return null;
    }

    public String originColumnName() {
        TupleValueExpression tve = null;
        if ((m_expression instanceof TupleValueExpression) == true)
            tve = (TupleValueExpression)m_expression;
        else
            return null;

        if (tve.getColumnAlias() != null)
            return new String(tve.getColumnAlias());
        else
            return null;
    }

    public int guid() {
        return m_guid;
    }

    public VoltType type() {
        return m_expression.getValueType();
    }

    public int width() {
        return m_expression.getValueSize();
    }

    public AbstractExpression getExpression()
    {
        return m_expression;
    }

    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        stringer.object();
        stringer.key(Members.GUID.name()).value(guid());
        stringer.key(Members.NAME.name()).value(displayName());
        stringer.key(Members.TYPE.name()).value(type().name());
        stringer.key(Members.SIZE.name()).value(width());
        if (originTableName() != null) {
            stringer.key(Members.INPUT_TABLE_NAME.name()).value(originTableName());
        }
        else
        {
            stringer.key(Members.INPUT_TABLE_NAME.name()).value("");
        }
        if (originColumnName() != null) {
            stringer.key(Members.INPUT_COLUMN_NAME.name()).value(originColumnName());
        }
        else
        {
            stringer.key(Members.INPUT_COLUMN_NAME.name()).value("");
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

        stringer.endObject();
    }

    //
    // Constructors
    //

    PlanColumn(
            int guid,
            AbstractExpression expression,
            String columnName,
            SortOrder sortOrder,
            Storage storage)
    {
        // all members are final and immutable (by implementation)
        m_guid = guid;
        m_expression = expression;
        m_displayName = columnName;
        m_sortOrder = sortOrder;
        m_storage = storage;

        /* Breaks for adhoc deser code..
        if (expression instanceof TupleValueExpression) {
            assert(((TupleValueExpression)expression).getColumnAlias() != null);
            assert(((TupleValueExpression)expression).getColumnName() != null);
        } */
    }

}
