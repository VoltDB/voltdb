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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;

/**
 *
 *
 */
public class ParsedDeleteStmt extends AbstractParsedStmt {

    /** Columns in the statements ORDER BY clause, if any */
    private final List<ParsedColInfo> m_orderColumns = new ArrayList<>();

    /** Limit plan node for this statement */
    private LimitPlanNode m_limitPlanNode = null;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedDeleteStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    /** Given XML for ORDER BY, add each column to m_orderColumns */
    private void parseOrderColumns(VoltXMLElement orderColumnsXml) {
        assert(m_orderColumns.size() == 0);
        if (orderColumnsXml == null)
            return;

        for (VoltXMLElement orderColXml : orderColumnsXml.children) {
            m_orderColumns.add(ParsedColInfo.fromOrderByXml(this, orderColXml));
        }
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        assert(m_tableList.size() == 1);

        VoltXMLElement limitXml = null;
        VoltXMLElement offsetXml = null;
        for (VoltXMLElement elem : stmtNode.children) {
            if (elem.name.equalsIgnoreCase("ordercolumns")) {
                parseOrderColumns(elem);
            }
            else if (elem.name.equalsIgnoreCase("limit")) {
                limitXml = elem;
            }
            else if(elem.name.equalsIgnoreCase("offset")) {
                offsetXml = elem;
            }
        }

        m_limitPlanNode = limitPlanNodeFromXml(limitXml, offsetXml);
    }

    /** Returns TRUE if this statement had an ORDER BY clause */
    @Override
    public boolean hasOrderByColumns() {
        return m_orderColumns.size() > 0;
    }

    /** Returns items in ORDER BY clause as a list of ParsedColInfo */
    @Override
    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(m_orderColumns);
    }

    /** Returns true if this statement has a LIMIT or OFFSET clause */
    @Override
    public boolean hasLimitOrOffset() {
        return m_limitPlanNode != null;
    }

    /** Returns a copy of the limit node for this statement if any.
     * The returned object is cloned, so it's suitable for connecting
     * to an existing plan. */
    public LimitPlanNode limitPlanNode() {
        assert(m_limitPlanNode != null);
        return new LimitPlanNode(m_limitPlanNode);
    }

    /**
     * Returns true if the ORDER BY clause on this
     * statement contains all the columns in the target table.
     * Used to determine if rows are ordered deterministically.
     * @return
     */
    private boolean orderByCoversAllColumns() {
        // SELECT statements do a check that is somewhat similar
        // But we are restricted here to a single table with no
        // table or column aliases allowed.
        //
        // Just build a set of all column names

        // There could be non-trivial expressions in the order by clause
        Set<String> allCols = new HashSet<>();
        Table t = m_tableList.get(0);
        for (Column c : t.getColumns()) {
            allCols.add(c.getName());
        }

        for (ParsedColInfo col : orderByColumns()) {
            AbstractExpression e = col.expression;
            if (!(e instanceof TupleValueExpression)) {
                continue;
            }
            TupleValueExpression tve = (TupleValueExpression)e;
            allCols.remove(tve.getColumnName());
        }

        return allCols.isEmpty();
    }

    /** Returns true if the set of rows deleted by this statement
     * is deterministic.
     */
    public boolean sideEffectsAreDeterministic() {

        if (! hasLimitOrOffset()) {
            return true;
        }

        // Syntax requires LIMIT or OFFSET to have an ORDER BY
        assert(hasOrderByColumns());

        if (orderByColumnsCoverUniqueKeys()) {
            return true;
        }

        if (orderByCoversAllColumns()) {
            return true;
        }

        return false;
    }
}
