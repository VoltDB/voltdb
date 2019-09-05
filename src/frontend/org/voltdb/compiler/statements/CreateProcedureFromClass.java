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

import org.voltcore.utils.CoreUtils;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process
 * CREATE PROCEDURE
 * [PARTITION ON TABLE table-name COLUMN column-name [PARAMETER position]]
 * [ALLOW role-name [,...]]
 * FROM CLASS class-name
 */
public class CreateProcedureFromClass extends CreateProcedure {

    public CreateProcedureFromClass(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(
            DDLStatement ddlStatement,
            Database db,
            DdlProceduresToLoad whichProcs) throws VoltCompilerException {
        // Matches if it is CREATE PROCEDURE [ALLOW <role> ...] [PARTITION ON ...] FROM CLASS <class-name>;
        Matcher statementMatcher = SQLParser.matchCreateProcedureFromClass(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        if (whichProcs != DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
            return true;
        }
        String className = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true, m_classLoader);
        }
        catch (Throwable cause) {
            // We are here because either the class was not found or the class was found and
            // the initializer of the class threw an error we can't anticipate. So we will
            // wrap the error with a runtime exception that we can trap in our code.
            if (CoreUtils.isStoredProcThrowableFatalToServer(cause)) {
                throw (Error)cause;
            }
            else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Cannot load class for procedure: %s",
                        className), cause);
            }
        }

        ProcedureDescriptor descriptor = new VoltCompiler.ProcedureDescriptor(
                new ArrayList<String>(), null, clazz);

        // Parse the ALLOW and PARTITION clauses.
        // Populate descriptor roles and returned partition data as needed.
        ProcedurePartitionData partitionData =
                parseCreateProcedureClauses(descriptor, statementMatcher.group(1));

        // track the defined procedure
        String procName = m_tracker.add(descriptor);

        // add partitioning if specified
        addProcedurePartitionInfo(procName, partitionData, ddlStatement.statement);

        return true;
    }
}
