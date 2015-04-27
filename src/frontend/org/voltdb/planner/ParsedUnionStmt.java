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

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.planner.ParsedSelectStmt.LimitOffset;
import org.voltdb.plannodes.LimitPlanNode;

public class ParsedUnionStmt extends AbstractParsedStmt {

    public enum UnionType {
        NOUNION,
        UNION,
        UNION_ALL,
        INTERSECT,
        INTERSECT_ALL,
        EXCEPT_ALL,
        EXCEPT
    };

    // Limit plan node information.
    LimitOffset m_limitOffset = new LimitOffset();

    public ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    public UnionType m_unionType = UnionType.NOUNION;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedUnionStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        m_unionType = UnionType.valueOf(type);

        int selectCounter = 0;
        VoltXMLElement limitElement = null, offsetElement = null, orderbyElement = null;
        for (VoltXMLElement child : stmtNode.children) {
            if (SELECT_NODE_NAME.equals(child.name) ||
                    UNION_NODE_NAME.equals(child.name)) {
                assert(selectCounter < m_children.size());
                AbstractParsedStmt nextStmt = m_children.get(selectCounter++);
                nextStmt.parse(child);
            } else if (child.name.equalsIgnoreCase("limit")) {
                limitElement = child;
            } else if (child.name.equalsIgnoreCase("offset")) {
                offsetElement = child;
            } else if (child.name.equalsIgnoreCase("ordercolumns")) {
                orderbyElement = child;
            }

        }
        // Parse LIMIT/OFFSET
        ParsedSelectStmt.parseLimitAndOffset(limitElement, offsetElement, m_limitOffset);
//        // Parse ORDER BY
//        ArrayList<ParsedColInfo> orderColumns = null;
//        if (orderbyElement != null) {
//            // Create a fake ParsedSelectStmt to parse the order by columns
//            ParsedSelectStmt parsedSelectStmt = new ParsedSelectStmt(m_paramValues, m_db);
//            parsedSelectStmt.parseOrderColumns(orderbyElement, false);
//            // Copy order by columns back to the union statement
//            orderColumns.addAll(parsedSelectStmt.orderByColumns());
//        }

        // prepare the limit plan node if it needs one.
        if (hasLimitOrOffset()) {
            ParsedSelectStmt.prepareLimitPlanNode(this, m_limitOffset);
        }
    }

    /**Parse tables and parameters
     *
     * @param root
     * @param db
     */
    @Override
    void parseTablesAndParams(VoltXMLElement stmtNode) {
        m_tableList.clear();
        // Parse parameters first to satisfy a dependency of expression parsing
        // which happens during table scan parsing.
        parseParameters(stmtNode);

        assert(stmtNode.children.size() > 1);
        AbstractParsedStmt childStmt = null;
        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                childStmt = new ParsedSelectStmt(m_paramValues, m_db);
            } else if (child.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                childStmt = new ParsedUnionStmt(m_paramValues, m_db);
            } else {
                // skip Order By, Limit/Offset. They will be processed later
                // by the 'parse' method
                continue;
            }
            childStmt.m_paramsById.putAll(m_paramsById);
            childStmt.parseTablesAndParams(child);
            promoteUnionParametersFromChild(childStmt);
            m_children.add(childStmt);
            // Add statement's tables to the consolidated list
            m_tableList.addAll(childStmt.m_tableList);
        }
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param joinOrder
     */
    @Override
    void postParse(String sql, String joinOrder) {
        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, joinOrder);
        }

        m_sql = sql;
        m_joinOrder = joinOrder;
    }

    @Override
    public boolean isOrderDeterministic() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( ! childStmt.isOrderDeterministic()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasLimitOrOffset() {
        return m_limitOffset.hasLimitOrOffset();
    }

    @Override
    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( ! childStmt.isOrderDeterministicInSpiteOfUnorderedSubqueries()) {
                return false;
            }
        }
        return true;
    }

    public LimitPlanNode getLimitNodeTop() {
        return m_limitOffset.getLimitNodeTop();
    }
}
