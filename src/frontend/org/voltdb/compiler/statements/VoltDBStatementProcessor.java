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

import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Check if a statement is VoltDB-specific.
 * If it is, then hand it over to the subsequent processors to handle.
 * If not, end the chain now and give it to HSQL for handling.
 */
public class VoltDBStatementProcessor extends StatementProcessor {

    public VoltDBStatementProcessor(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    private String m_commandPrefix;

    public String getCommandPrefix() {
        return m_commandPrefix;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        if (ddlStatement.statement == null || ddlStatement.statement.trim().isEmpty()) {
            m_returnAfterThis = true;
            return false;
        }
        ddlStatement.statement = ddlStatement.statement.trim();

        // Matches if it is the beginning of a VoltDB statement
        Matcher statementMatcher = SQLParser.matchAllVoltDBStatementPreambles(ddlStatement.statement);
        if ( ! statementMatcher.find()) {
            m_returnAfterThis = true;
            return false;
        }
        // Either PROCEDURE, FUNCTION, REPLICATE, PARTITION, ROLE, EXPORT, DR OR AGGREGATE
        m_commandPrefix = statementMatcher.group(1).toUpperCase();
        // TODO: fix this for compound etc
        return false;
    }

}
