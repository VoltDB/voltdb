/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

public class ParsedUnionStmt extends AbstractParsedStmt {

    public enum UnionType {
        NOUNION,
        UNION,
        UNION_ALL,
        INTERSECT,
        INTERSECT_ALL,
        EXCEPT_ALL,
        EXCEPT,
        UNION_TERM
    };

    public ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    public UnionType m_unionType = UnionType.NOUNION;

    @Override
    void parse(VoltXMLElement stmtNode, Database db) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        m_unionType = UnionType.valueOf(type);

        assert(stmtNode.children.size() == m_children.size());
        int i = 0;
        for (VoltXMLElement selectSQL : stmtNode.children) {
            AbstractParsedStmt nextSelectStmt = m_children.get(i++);
            nextSelectStmt.parse(selectSQL, db);
        }
    }

    /**Parse tables and parameters
     *
     * @param root
     * @param db
     */
    void parseTablesAndParams(VoltXMLElement stmtNode, Database db) {

        assert(stmtNode.children.size() > 1);
        tableList.clear();
        for (VoltXMLElement childSQL : stmtNode.children) {
            // @TODO MIKE
            //if (!childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
            //    throw new PlanningErrorException("Unexpected Element in UNION statement: " + childSQL.name);
            //}
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedSelectStmt();
                childStmt.parseTablesAndParams(childSQL, db);
                m_children.add(childStmt);


                // So far T UNION T (as well as T JOIN T) are not handled properly
                // by the fragmentizer. Need to give an error if any table mentioned
                // in a UNION's (UNION TREE's) children occurs more than once.
                HashSet<String> uniqueTables = new HashSet<String>();

                if (childStmt.scanColumns != null)
                {
                    Set<String> tableNames = childStmt.scanColumns.keySet();

                    Iterator<Table> it = childStmt.tableList.iterator();
                    // When HSQLInterface.getXMLCompiledStatement() parses the union statement
                    // it adds ALL tables across the entire union to each child statement (table sources)
                    // SCAN columns though contains right set of tables related to this particular
                    // sub-select only. Filter out tables which are not from this statement
                    while (it.hasNext()) {
                        String tableName = it.next().getTypeName();
                        if (!tableNames.contains(tableName)) {
                            it.remove();
                        } else if (uniqueTables.contains(tableName)) {
                            // Is this table 'unique' across the union?
                            throw new PlanningErrorException("Table " + tableName +
                                    " appears more than once in the union statement");
                        } else {
                            uniqueTables.add(tableName);
                        }
                    }
                } else {
                    throw new PlanningErrorException("Select * is not supported within the UNION statement");
                }
                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedUnionStmt();
                childStmt.parseTablesAndParams(childSQL, db);
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
    void postParse(String sql, Database db, String joinOrder) {

        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, db, joinOrder);
        }
        // these just shouldn't happen right?
        assert(this.multiTableSelectionList.size() == 0);
        assert(this.noTableSelectionList.size() == 0);

        this.sql = sql;
        this.joinOrder = joinOrder;
    }


}
