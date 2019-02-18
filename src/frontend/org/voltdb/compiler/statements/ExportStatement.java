/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

public class ExportStatement extends StatementProcessor {

    public ExportStatement(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    private static VoltXMLElement findRegularTable(VoltXMLElement m_schema, String name) {
        for (VoltXMLElement element : m_schema.children) {
            if (element.name.equals("table")
                    && element.attributes.get("name").equalsIgnoreCase(name)) {

                if (! (element.attributes.containsKey("stream")
                        && element.attributes.get("stream").equalsIgnoreCase("true")) ) {
                    return element;
                }
            }
        }
        return null;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher statementMatcher = SQLParser.matchExportTable(ddlStatement.statement);
        if (!statementMatcher.matches()) {
            return false;
        }
        String tableName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
        String targetName = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);

        VoltXMLElement tableXML = findRegularTable(m_schema, tableName.toUpperCase());
        if (tableXML != null) {
            tableXML.attributes.put("export", targetName);
        } else {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid EXPORT TABLE statement: could not find a table named %s", tableName));
        }
        //System.out.println("XXX Exporting table " + tableName + ", to target " + targetName);
        return true;
    }

}
