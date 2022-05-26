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
 * Process DROP STREAM stream-name [IF EXISTS] [CASCADE]
 */
public class DropStream extends StatementProcessor {

    public DropStream(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    private void validateTable(String name) throws VoltCompilerException {
        for (VoltXMLElement element : m_schema.children) {
            if (element.name.equals("table")
                    && element.attributes.get("name").equalsIgnoreCase(name)) {
                if (!Boolean.parseBoolean(element.attributes.get("stream"))) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid DROP STREAM statement: %s is not a stream.",
                            name));
                }
                break;
            }
        }
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // matches if it is DROP STREAM
        // group 1 is stream name
        // guard against drop regular table
        Matcher statementMatcher = SQLParser.matchDropStream(ddlStatement.statement);
        if (statementMatcher.matches()) {
            String streamName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
            validateTable(streamName);
            m_returnAfterThis = true;
        }
        return false;
    }
}
