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
import java.util.List;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ParameterValueExpression;
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
    private LimitOffset m_limitOffset = new LimitOffset();
    // Order by
    private ArrayList<ParsedColInfo> m_orderColumns = new ArrayList<ParsedColInfo>();

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
        // Parse ORDER BY
        if (orderbyElement != null) {
            parseOrderColumns(orderbyElement);
        }

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
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                childStmt = new ParsedSelectStmt(m_paramValues, m_db);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                childStmt = new ParsedUnionStmt(m_paramValues, m_db);
            } else {
                // skip Order By, Limit/Offset. They will be processed later
                // by the 'parse' method
                continue;
            }
            childStmt.m_paramsById.putAll(m_paramsById);
            childStmt.parseTablesAndParams(childSQL);
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

    private void parseOrderColumns(VoltXMLElement columnsNode) {
        ParsedSelectStmt firstSelectChild = getFirstSelectStmt();
        for (VoltXMLElement child : columnsNode.children) {
            parseOrderColumn(child, firstSelectChild);
        }
    }

    /**
     * This is a stripped down version of the ParsedSelectStmt.parseOrderColumn. Since the SET ops
     * are not allowed to have aggregate expressions (HAVING, GROUP BY) (except the individual SELECTS)
     * all the logic handling the aggregates is omitted here
     * @param orderByNode
     * @param firstSelectChild
     */
    private void parseOrderColumn(VoltXMLElement orderByNode, ParsedSelectStmt firstSelectChild) {

        ParsedColInfo.ExpressionAdjuster adjuster = new ParsedColInfo.ExpressionAdjuster() {
            @Override
            public AbstractExpression adjust(AbstractExpression expr) {
                // Union itself can't have aggregate expression
                return expr;
            }
        };
        // Get the display columns from the first child
        List<ParsedColInfo> displayColumns = firstSelectChild.orderByColumns();
        ParsedColInfo order_col = ParsedColInfo.fromOrderByXml(firstSelectChild, orderByNode, adjuster);

        AbstractExpression order_exp = order_col.expression;
        assert(order_exp != null);
        // Mark the order by column if it is in displayColumns
        // The ORDER BY column MAY be identical to a simple display column, in which case,
        // tagging the actual display column as being also an order by column
        // helps later when trying to determine ORDER BY coverage (for determinism).
        for (ParsedColInfo col : displayColumns) {
            if (col.alias.equals(order_col.alias) || col.expression.equals(order_exp)) {
                col.orderBy = true;
                col.ascending = order_col.ascending;

                order_col.alias = col.alias;
                order_col.columnName = col.columnName;
                order_col.tableName = col.tableName;
                break;
            }
        }
        assert( ! (order_exp instanceof ConstantValueExpression));
        assert( ! (order_exp instanceof ParameterValueExpression));

        m_orderColumns.add(order_col);
    }

    /**
     * Return the first child SELECT statement
     * @return ParsedSelectStmt
     */
    private ParsedSelectStmt getFirstSelectStmt() {
        assert (!m_children.isEmpty());
        AbstractParsedStmt firstChild = m_children.get(0);
        if (firstChild instanceof ParsedSelectStmt) {
            return (ParsedSelectStmt) firstChild;
        } else {
            assert(firstChild instanceof ParsedUnionStmt);
            return ((ParsedUnionStmt)firstChild).getFirstSelectStmt();
        }
    }

    @Override
    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(m_orderColumns);
    }

    @Override
    public boolean hasOrderByColumns() {
        return ! m_orderColumns.isEmpty();
    }

}
