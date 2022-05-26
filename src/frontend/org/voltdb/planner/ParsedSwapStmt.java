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

package org.voltdb.planner;

import java.util.Collection;
import java.util.HashSet;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;

/**
 * Subclass of AbstractParsedStmt for a SWAP TABLES statement.
 * Note that currently SWAP TABLES is not valid DML in VoltDB SQL,
 * it must be invoked from the @SwapTables system procedure.  Under
 * the hood this procedure just constructs an instance of this class.
 * (At some point in the future a SWAP TABLES DML statement may be
 * supported.)
 */
public class ParsedSwapStmt extends AbstractParsedStmt {
    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedSwapStmt(AbstractParsedStmt parent, String[] paramValues, Database db) {
        super(parent, paramValues, db);
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

        addTabletoList(stmtNode, "thetable");
        addTabletoList(stmtNode, "othertable");
    }

    private void addTabletoList(VoltXMLElement stmtNode, String attrName) {

        String name = stmtNode.attributes.get(attrName);
        assert(name != null);
        Table table = getTableFromDB(name);
        if (table == null) {
            throw new PlanningErrorException("TABLE object not found: " + name);
        }
        m_tableList.add(table);
    }

    @Override
    public boolean isDML() { return true; }

    @Override
    public String calculateContentDeterminismMessage() { return null; }

    @Override
    /**
     * There are no UDF dependences in swap statements.
     */
    public Collection<String> calculateUDFDependees() {
        return new HashSet<>();
    }

    @Override
    protected void parseCommonTableExpressions(VoltXMLElement root) {
        // No with statements here.
    }
}
