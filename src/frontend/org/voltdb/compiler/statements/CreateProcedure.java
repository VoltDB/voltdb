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

package org.voltdb.compiler.statements;

import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * This class serves as the base class of two CREATE PROCEDURE processors,
 * it has some functions that are shared by the two processors.
 */
public abstract class CreateProcedure extends StatementProcessor {

    public CreateProcedure(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    protected static class CreateProcedurePartitionData {
        String tableName = null;
        String columnName = null;
        String parameterNo = null;
    }

    /**
     * Parse and validate the substring containing ALLOW and PARTITION
     * clauses for CREATE PROCEDURE.
     * @param clauses  the substring to parse
     * @param descriptor  procedure descriptor populated with role names from ALLOW clause
     * @return  parsed and validated partition data or null if there was no PARTITION clause
     * @throws VoltCompilerException
     */
    protected CreateProcedurePartitionData parseCreateProcedureClauses(
              ProcedureDescriptor descriptor,
              String clauses) throws VoltCompilerException {

        // Nothing to do if there were no clauses.
        // Null means there's no partition data to return.
        // There's also no roles to add.
        if (clauses == null || clauses.isEmpty()) {
            return null;
        }
        CreateProcedurePartitionData data = null;

        Matcher matcher = SQLParser.matchAnyCreateProcedureStatementClause(clauses);
        int start = 0;
        while (matcher.find(start)) {
            start = matcher.end();

            if (matcher.group(1) != null) {
                // Add roles if it's an ALLOW clause. More that one ALLOW clause is okay.
                for (String roleName : StringUtils.split(matcher.group(1), ',')) {
                    // Don't put the same role in the list more than once.
                   String roleNameFixed = roleName.trim().toLowerCase();
                    if (!descriptor.m_authGroups.contains(roleNameFixed)) {
                        descriptor.m_authGroups.add(roleNameFixed);
                    }
                }
            }
            else {
                // Add partition info if it's a PARTITION clause. Only one is allowed.
                if (data != null) {
                    throw m_compiler.new VoltCompilerException(
                        "Only one PARTITION clause is allowed for CREATE PROCEDURE.");
                }
                data = new CreateProcedurePartitionData();
                data.tableName = matcher.group(2);
                data.columnName = matcher.group(3);
                data.parameterNo = matcher.group(4);
            }
        }

        return data;
    }

    protected void addProcedurePartitionInfo(
              String procName,
              CreateProcedurePartitionData data,
              String statement) throws VoltCompilerException {

        assert(procName != null);

        // Will be null when there is no optional partition clause.
        if (data == null) {
            return;
        }

        assert(data.tableName != null);
        assert(data.columnName != null);

        // Check the identifiers.
        checkIdentifierStart(procName, statement);
        checkIdentifierStart(data.tableName, statement);
        checkIdentifierStart(data.columnName, statement);

        // if not specified default parameter index to 0
        if (data.parameterNo == null) {
            data.parameterNo = "0";
        }

        String partitionInfo = String.format("%s.%s: %s", data.tableName, data.columnName, data.parameterNo);

        m_tracker.addProcedurePartitionInfoTo(procName, partitionInfo);
    }
}
