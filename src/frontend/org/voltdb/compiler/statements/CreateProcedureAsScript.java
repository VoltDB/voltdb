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
 * Process
 * CREATE PROCEDURE procedure-name
 * [PARTITION ON TABLE table-name COLUMN column-name [PARAMETER position]]
 * [ALLOW role-name [,...]]
 * AS sql-statement
 */
public class CreateProcedureAsScript extends StatementProcessor {

    public CreateProcedureAsScript(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // Matches if it is CREATE PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS
        // ### <code-block> ### LANGUAGE <language-name>
        // We used to support Groovy in pre-5.x, but now we don't
        Matcher statementMatcher = SQLParser.matchCreateProcedureAsScript(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        throw m_compiler.new VoltCompilerException("VoltDB doesn't support inline procedure creation.");
    }

}
