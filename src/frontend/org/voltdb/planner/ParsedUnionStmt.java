/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.expressions.AbstractExpression;

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

        assert(stmtNode.children.size() == m_children.size());
        int i = 0;
        for (VoltXMLElement selectSQL : stmtNode.children) {
            AbstractParsedStmt nextSelectStmt = m_children.get(i++);
            nextSelectStmt.parse(selectSQL);
        }
    }

    /**Parse tables and parameters
     *
     * @param root
     * @param db
     */
    @Override
    void parseTablesAndParams(VoltXMLElement stmtNode) {

        assert(stmtNode.children.size() > 1);
        m_tableList.clear();
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedSelectStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);

                // Add statement's tables to the consolidated list
                m_tableList.addAll(childStmt.m_tableList);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                ParsedUnionStmt childStmt = new ParsedUnionStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);
                // Add statement's tables to the consolidated list
                m_tableList.addAll(childStmt.m_tableList);
            } else {
                throw new PlanningErrorException("Unexpected Element in UNION statement: " + childSQL.name);
            }
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

    /**
    * Converts each child from the IN union subquery into the equivalent EXISTS
    *
    * @param unionStmt set operations subquery from the IN expression
    * @param inListExpr TVE for the columns from the IN list
    * @return modified subquery
    */
    protected static void rewriteInSubqueryAsExists(ParsedUnionStmt unionStmt, AbstractExpression inListExpr) {
        if (unionStmt.m_unionType == UnionType.UNION || unionStmt.m_unionType == UnionType.UNION_ALL) {
            // rewrite for all children
            for (AbstractParsedStmt childStmt : unionStmt.m_children) {
                if (childStmt instanceof ParsedSelectStmt) {
                    ParsedSelectStmt.rewriteInSubqueryAsExists((ParsedSelectStmt) childStmt, inListExpr);
                } else {
                    assert(childStmt instanceof ParsedUnionStmt);
                    ParsedUnionStmt.rewriteInSubqueryAsExists((ParsedUnionStmt) childStmt, inListExpr);
                }
            }
        } else {
            // for everything else it's enough to rewirite the left operand only
            assert(!unionStmt.m_children.isEmpty());
            AbstractParsedStmt childStmt = unionStmt.m_children.get(0);
            if (childStmt instanceof ParsedSelectStmt) {
                ParsedSelectStmt.rewriteInSubqueryAsExists((ParsedSelectStmt) childStmt, inListExpr);
            } else {
                assert(childStmt instanceof ParsedUnionStmt);
                ParsedUnionStmt.rewriteInSubqueryAsExists((ParsedUnionStmt) childStmt, inListExpr);
            }
        }
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
        for (AbstractParsedStmt childStmt : m_children) {
            if ( childStmt.hasLimitOrOffset()) {
                return true;
            }
        }
        return false;
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
}
