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

import java.util.ArrayList;
import java.util.regex.Matcher;

import org.voltdb.ProcedurePartitionData;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

public class CreateProcedureAsSQL extends CreateProcedure {

    public CreateProcedureAsSQL(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {

        /* first check if it matches a multi statement procedure
         * if not, then check if it is a single statement procedure
         */
        // Matches if it is CREATE PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS BEGIN <select-or-dml-statement>
        Matcher statementMatcher = SQLParser.matchCreateMultiStmtProcedureAsSQL(ddlStatement.statement);

        if (! statementMatcher.matches()) {
         // Matches if it is CREATE PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS <select-or-dml-statement>
            statementMatcher = SQLParser.matchCreateProcedureAsSQL(ddlStatement.statement);
            if (! statementMatcher.matches()) {
                return false;
            }
        }
        String clazz = checkProcedureIdentifier(statementMatcher.group(1), ddlStatement.statement);
        String sqlStatement = statementMatcher.group(3) + ";";

        ProcedureDescriptor descriptor = m_compiler.new ProcedureDescriptor(
                new ArrayList<String>(), clazz, sqlStatement, null, null, false, null);

        // Parse the ALLOW and PARTITION clauses.
        // Populate descriptor roles and returned partition data as needed.
        ProcedurePartitionData partitionData =
                parseCreateProcedureClauses(descriptor, statementMatcher.group(2));

        m_tracker.add(descriptor);

        // add partitioning if specified
        addProcedurePartitionInfo(clazz, partitionData, ddlStatement.statement);

        return true;
    }

    /**
     * Check whether or not a procedure name is acceptible.
     * @param identifier the identifier to check
     * @param statement the statement where the identifier is
     * @return the given identifier unmodified
     * @throws VoltCompilerException
     */
    private String checkProcedureIdentifier(
            final String identifier, final String statement
            ) throws VoltCompilerException {
        String retIdent = checkIdentifierStart(identifier, statement);
        if (retIdent.contains(".")) {
            String msg = String.format(
                "Invalid procedure name containing dots \"%s\" in DDL: \"%s\"",
                identifier, statement.substring(0,statement.length()-1));
            throw m_compiler.new VoltCompilerException(msg);
        }
        return retIdent;
    }

}
