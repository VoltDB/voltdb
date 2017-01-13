/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

/**
 *
 *
 */
public class ParsedSwapStmt extends AbstractParsedStmt {
    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedSwapStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    /**
     * Parse the arguments to a SWAP TABLE statement.
     * SWAP TABLE statements use simple String attributes
     * as the "VoltXML" representation for their target tables.
     * They don't need all the complexity of the child table nodes
     * used in other kinds of statements.
     * SWAP TABLE statements don't bother to populate most of the
     * detailed AbstractParsedTable members related to tables.
     * The m_tableList is sufficient for SWAP TABLE's minimal
     * validation and planning requirements.
     */
    @Override
    void parse(VoltXMLElement stmtNode) {
        // parseTablesAndParameters may have been called on this
        // SWAP TABLE statement, but the simplified VoltXML representation
        // for SWAP TABLE gave that method nothing to do.
        assert(stmtNode.children.isEmpty());
        assert(m_tableList.isEmpty());

        String theName = stmtNode.attributes.get("thetable");
        assert(theName != null);
        Table theTable = getTableFromDB(theName);
        if (theTable == null) {
            throw new PlanningErrorException("TABLE object not found: " + theName);
        }
        m_tableList.add(theTable);

        String otherName = stmtNode.attributes.get("othertable");
        assert(otherName != null);
        Table otherTable = getTableFromDB(otherName);
        if (otherTable == null) {
            throw new PlanningErrorException("TABLE object not found: " + otherName);
        }
        m_tableList.add(otherTable);
    }

    @Override
    public boolean isDML() { return true; }

    @Override
    public String calculateContentDeterminismMessage() { return null; }
}
