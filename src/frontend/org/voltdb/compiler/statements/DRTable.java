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

package org.voltdb.compiler.statements;

import java.util.regex.Matcher;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process DR TABLE table-name [DISABLE]
 */
public class DRTable extends StatementProcessor {

    public DRTable(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // matches if it is DR TABLE <table-name> [DISABLE]
        // group 1 -- table name
        // group 2 -- NULL: enable dr
        //            NOT NULL: disable dr
        // TODO: maybe I should write one fit all regex for this.
        Matcher statementMatcher = SQLParser.matchDRTable(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        String tableName;
        if (statementMatcher.group(1).equalsIgnoreCase("*")) {
            tableName = "*";
        } else {
            tableName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
        }

        //System.out.println("\n\n" + m_schema.toString());

        VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
        if (tableXML != null) {
            if (tableXML.attributes.containsKey("export")) {
                throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid DR statement: table %s is an export table", tableName));
            }
            else {
                if ((statementMatcher.group(2) != null)) {
                    tableXML.attributes.put("drTable", "DISABLE");
                }
                else {
                    tableXML.attributes.put("drTable", "ENABLE");
                }
            }
        }
        else {
            throw m_compiler.new VoltCompilerException(String.format(
                    "While configuring dr, table %s was not present in the catalog.", tableName));
        }
        return true;
    }

}
