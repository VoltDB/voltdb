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

    ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    UnionType m_unionType = UnionType.NOUNION;

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
        tableList.clear();
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedSelectStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);

                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                ParsedUnionStmt childStmt = new ParsedUnionStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);
                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
            } else {
                throw new PlanningErrorException("Unexpected Element in UNION statement: " + childSQL.name);
            }
        }
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param db
     * @param joinOrder
     */
    @Override
    void postParse(String sql, String joinOrder)
    {
        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, joinOrder);
        }

        // these just shouldn't happen right?
        assert(noTableSelectionList.size() == 0);

        this.sql = sql;
        this.joinOrder = joinOrder;
    }

    public boolean isOrderDeterministic() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( ! childStmt.isOrderDeterministic()) {
                return false;
            }
        }
        return true;
    }

    public boolean hasLimitOrOffset() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( childStmt.hasLimitOrOffset()) {
                return true;
            }
        }
        return false;
    }
}
