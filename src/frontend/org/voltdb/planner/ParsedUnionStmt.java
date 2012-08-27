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
import org.voltdb.plannodes.SchemaColumn;

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
        HashSet<Table> tableSet = new HashSet<Table>();
        int i = 0;
        for (VoltXMLElement selectSQL : stmtNode.children) {
            AbstractParsedStmt nextSelectStmt = m_children.get(i++);
            nextSelectStmt.parse(selectSQL, db);
            // @TODO MIKE
            // When HSQLInterface.getXMLCompiledStatement() parses the union statement
            // it adds ALL tables across the entire union to each child statement (table sources)
            // SCAN columns though contains right set of tables related to this particular
            // sub-select only
            // What to do if nextSelectStmt.scanColumns is NULL?
            if (nextSelectStmt.scanColumns != null)
            {
                Set<String> tableNames = nextSelectStmt.scanColumns.keySet();
                Iterator<Table> it = nextSelectStmt.tableList.iterator();

                while (it.hasNext()) {
                    Table next = it.next();
                    if (!tableNames.contains(next.getTypeName())) {
                        it.remove();
                    }
                }
                tableSet.addAll(nextSelectStmt.tableList);
            } else {
                throw new RuntimeException("Select * is not supported within the UNION statement");
            }
        }
        // List of all tables across the union
        tableList.clear();
        tableList.addAll(tableSet);
    }

    /**Parse tables and parameters
     * .
     * @param root
     * @param db
     */
    void parseTablesAndParams(VoltXMLElement stmtNode, Database db) {

        assert(stmtNode.children.size() > 1);
        for (VoltXMLElement selectSQL : stmtNode.children) {
            if (!selectSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                throw new RuntimeException("Unexpected Element in UNION statement: " + selectSQL.name);
            }
            AbstractParsedStmt selectStmt = new ParsedSelectStmt();
            selectStmt.parseTablesAndParams(selectSQL, db);
            m_children.add(selectStmt);
            //tableList.addAll(selectStmt.tableList);
        }
        // MIKE. This is I don't understand why all fragments have the same table list
        // all tables across all selects in the union
        tableList.addAll(m_children.get(0).tableList);
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
