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

import java.util.ArrayList;
import java.util.regex.Matcher;

import org.voltdb.ProcedurePartitionData;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.VoltCompiler;
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

        // Matches if it is CREATE <type> PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS BEGIN <select-or-dml-statement>
        Matcher statementMatcher = SQLParser.matchCreateProcedureAsMultiStmtSQL(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            // Matches if it is CREATE <type> PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS <select-or-dml-statement>
            statementMatcher = SQLParser.matchCreateProcedureAsSQL(ddlStatement.statement);
            if (! statementMatcher.matches()) {
                return false;
            }
        }

        // Capture groups:
        //  (1) Optional type modifier, DIRECTED or COMPOUND
        //  (2) Procedure name
        //  (3) ALLOW/PARTITION clauses full text - needs further parsing
        //  (4) SELECT or DML statement

        String typeModifier = statementMatcher.group(1);
        String clazz = checkProcedureIdentifier(statementMatcher.group(2), ddlStatement.statement);
        String otherClauses =  statementMatcher.group(3);
        String sqlStatement = statementMatcher.group(4) + ";";

        ProcedureDescriptor descriptor = new VoltCompiler.ProcedureDescriptor(
                new ArrayList<String>(), clazz, sqlStatement, null, null, false, null);

        // Parse the ALLOW and PARTITION clauses.
        // Populate descriptor roles and returned partition data as needed.
        ProcedurePartitionData partitionData = parseCreateProcedureClauses(descriptor, typeModifier, otherClauses);

        // The syntax allows 'compound', but it is not meaningful here
        if (partitionData != null && partitionData.isCompoundProcedure()) {
            throw m_compiler.new VoltCompilerException("COMPOUND is only allowed with CREATE PROCEDURE FROM CLASS");
        }

        // track the defined procedure
        m_tracker.add(descriptor);

        // add partitioning if specified
        addProcedurePartitionInfo(clazz, partitionData, ddlStatement.statement);

        return true;
    }

    /**
     * Check whether or not a procedure name is acceptable.
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
