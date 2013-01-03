/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
        EXCEPT
    };
    /** Hash Set to enforce table uniqueness across ALL the children statements */
    public HashSet<String> m_uniqueTables = new HashSet<String>();
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
    void parseTablesAndParams(VoltXMLElement stmtNode) {

        assert(stmtNode.children.size() > 1);
        tableList.clear();
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedSelectStmt(this.m_paramValues, this.m_db);
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);

                // So far T UNION T (as well as T JOIN T) are not handled properly
                // by the fragmentizer. Need to give an error if any table is mentioned
                // in the UNION TREE more than once.
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
                        } else if (m_uniqueTables.contains(tableName)) {
                            // The table is not 'unique' across the union
                            throw new PlanningErrorException("Table " + tableName +
                                    " appears more than once in the union statement");
                        } else {
                            m_uniqueTables.add(tableName);
                        }
                    }
                } else {
                    throw new PlanningErrorException("Scan columns are NULL the UNION statement");
                }
                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                ParsedUnionStmt childStmt = new ParsedUnionStmt(this.m_paramValues, this.m_db);
                // Pass already accumulated unique tables to the child union
                childStmt.m_uniqueTables = m_uniqueTables;
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);
                // Add statement's tables to the consolidated list
                tableList.addAll(childStmt.tableList);
                // Child's unique tables now contains the consolidated list
                m_uniqueTables = childStmt.m_uniqueTables;
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
    void postParse(String sql, String joinOrder) {

        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, joinOrder);
            // Propagate parsing results to the parent union
            this.whereSelectionList.addAll(selectStmt.whereSelectionList);
        }
        // Analyze children's where expressions together to identify possible identically
        // partitioned tables
        this.analyzeWhereExpression(this.whereSelectionList);

        // these just shouldn't happen right?
        assert(this.multiTableSelectionList.size() == 0);
        assert(this.noTableSelectionList.size() == 0);

        this.sql = sql;
        this.joinOrder = joinOrder;
    }


}
