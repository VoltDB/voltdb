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

package org.voltdb.planner.parseinfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a table or a sub-query
 * within the statement scope.  These are the items we scan over in a FROM clause.
 * For example, given this SQL statement:
 *   select * from a, b as bb join b as cc on bb.id = cc.id;
 * there are three table scans.  One is for a, with alias a.
 * There is a second for bb with alias b, and a third for
 * b again, but with alias cc.  These two occurrences of b
 * are separate scans of the same table.
 *
 * In a certain sense, these scans are similar to a <em>scope<em>
 * in other programming languages.  Given a table reference, T.C,
 * we look up the name T in parsed statement to find which StmtTableScan
 * it denotes, and then we look up C in the scan.
 *
 * There are separate subclasses of this class for persistent
 * tables and for ephemeral tables, whose values don't persist
 * past a single query.
 *
 * There is some complexity here with names.  At the core of a
 * StmtTableScan object there is a NodeSchema.  This is a list
 * of SchemaColumn objects.  Each column has a name, but columns
 * may not be uniquely determined by their name.  For example,
 * in a derived table which joins two other derived tables there
 * may be some name duplication.  In order to keep these all
 * straight we introduce another integer index, thie differentiator.
 * Two schema columns must have different <name, differentiator>
 * pairs.
 */
public abstract class StmtTableScan {
    public static final int NULL_ALIAS_INDEX = -1;

    // The statement id this table belongs to
    protected int m_stmtId = 0;

    // table alias
    protected String m_tableAlias = null;

    // See getScanColumns() for an explanation of what
    // this is good for.  In particular, this is not
    // a schema.
    protected final List<SchemaColumn> m_scanColumnsList = new ArrayList<>();
    // This is used only in resolveLeafTve.  We may see multiple
    // references to a <columnname, differentiator> pair in an
    // expression.  We want to add all the columns in the expressions
    // in this scan to the m_scanColumnsList.
    private final Set<Pair<String, Integer>> m_scanColumnNameSet = new HashSet<>();

    // Partitioning column info
    protected List<SchemaColumn> m_partitioningColumns = null;

    protected StmtTableScan(String tableAlias, int stmtId) {
        m_tableAlias = tableAlias;
        m_stmtId = stmtId;
    }

    /**
     * Make a leaf join node.  Each kind of scan makes its own
     * class of leaf join node.
     * @param id Unique node id.
     * @param joinExpr All the join expressions on this table in one expression.
     * @param whereExpr All the where expressions using this table.
     */
    public abstract JoinNode makeLeafNode(int nodeId, AbstractExpression joinExpr, AbstractExpression whereExpr);

    public String getTableAlias() {
        return m_tableAlias;
    }

    /**
     * This scan is from the "from" list of a select statement,
     * and references a persistent or derived table or else a
     * common table, defined in a "with" clause.  It's useful
     * to know which input columns have actually be referenced
     * by some expression in this scan, so that we know which
     * input columns can be projected out.
     *
     * @return a list of all columns in this table which have been referenced
     * in some expression in the statement which contains this
     * scan.
     */
    public List<SchemaColumn> getScanColumns() {
        return m_scanColumnsList;
    }

    public List<SchemaColumn> getPartitioningColumns() {
        return m_partitioningColumns;
    }

    abstract public String getTableName();

    abstract public boolean getIsReplicated();

    protected static final List<Index> noIndexesSupportedOnSubqueryScansOrCommonTables = new ArrayList<>();

    abstract public List<Index> getIndexes();

    public int getStatementId() {
        return m_stmtId;
    }

    abstract public String getColumnName(int columnIndex);

    /**
     * Look up the column named columnName in this table scan
     * and transfer the information from the SchemaColumn to the
     * expr.  This information is the type,
     * the size and whether the size is in bytes.  This needs to be done
     * differently for derived tables, which we call subqueries,
     * persistent tables and common tables, defined using a with clause.
     *
     * @param expr
     * @param columnName
     * @return
     */
    abstract public AbstractExpression processTVE(TupleValueExpression expr, String columnName);

    /**
     * The parameter tve is a column reference, obtained
     * by parsing a column ref VoltXML element.  We need to
     * find out to which column in the current table scan the
     * name of the TVE refers, and transfer metadata from the
     * schema's column to the tve.  The function processTVE
     * does the transfer.
     *
     * In some cases the tve may actually be replaced by some
     * other expression.  In this case we do the resolution
     * for all the tves in the new expression.
     * @param tve
     * @return
     */
    public AbstractExpression resolveTVE(TupleValueExpression tve) {
        AbstractExpression resolvedExpr = processTVE(tve, tve.getColumnName());

        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(resolvedExpr);
        for (TupleValueExpression subqTve : tves) {
            resolveLeafTve(subqTve);
        }
        return resolvedExpr;
    }

    private void resolveLeafTve(TupleValueExpression subqTve) {
        String columnName = subqTve.getColumnName();
        subqTve.setOrigStmtId(m_stmtId);
        Pair<String, Integer> setItem =
                Pair.of(columnName, subqTve.getDifferentiator());
        if (m_scanColumnNameSet.add(setItem)) {
            SchemaColumn scol = new SchemaColumn(getTableName(), m_tableAlias,
                    columnName, columnName, subqTve);
            m_scanColumnsList.add(scol);
        }
    }
}
