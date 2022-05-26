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

import org.apache.commons.lang3.StringUtils;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * This class serves as the base class for two processors,
 * for CREATE PROCEDURE AS and CREATE PROCEDURE FROM CLASS.
 *
 * It has some functions that are shared by both processors.
 */
public abstract class CreateProcedure extends StatementProcessor {

    public CreateProcedure(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    /**
     * Parse and validate the substring containing ALLOW and PARTITION
     * clauses for CREATE PROCEDURE.
     *
     * Handles DIRECTED and COMPOUND modifiers: PARTITION is not supported
     * for these cases.
     *
     * The type information (DIRECTED or COMPOUND keywords) should appear
     * between CREATE and PROCEDURE. For back-compatibility we allow
     * DIRECTED to be specified in the ALLOW/PARTITION part.
     *
     * @param descriptor  procedure descriptor populated with role names from ALLOW clause
     * @param typeModifier optional DIRECTED or COMPOUND
     * @param clauses  the substring to parse
     * @return  parsed and validated partition data, null for multipartition proc
     * @throws VoltCompilerException
     */
    protected ProcedurePartitionData parseCreateProcedureClauses(ProcedureDescriptor descriptor,
                                                                 String typeModifier,
                                                                 String clauses) throws VoltCompilerException {
        ProcedurePartitionData data = null;
        String procType = null;

        if (typeModifier != null && !typeModifier.isEmpty()) {
            data = new ProcedurePartitionData(convertTypeModifier(typeModifier));
            procType = typeModifier.toUpperCase();
        }

        if (clauses == null || clauses.isEmpty()) {
            return data; // null if no typeModifier; implies multipartition procedure
        }

        Matcher matcher = SQLParser.matchAnyCreateProcedureStatementClause(clauses);
        int start = 0;

        while (matcher.find(start)) {
            start = matcher.end();

            if (matcher.group(1) != null) {
                // Add roles if it's an ALLOW clause. More than one ALLOW clause is okay.
                for (String roleName : StringUtils.split(matcher.group(1), ',')) {
                    // Don't put the same role in the list more than once.
                   String roleNameFixed = roleName.trim().toLowerCase();
                    if (!descriptor.m_authGroups.contains(roleNameFixed)) {
                        descriptor.m_authGroups.add(roleNameFixed);
                    }
                }
            } else {
                ProcedurePartitionData thisData = null;
                String thisType = null;
                String thisModifier = matcher.group(8);
                if (thisModifier != null) {
                    thisData = new ProcedurePartitionData(convertTypeModifier(thisModifier));
                    thisType = thisModifier.toUpperCase();
                }
                else {
                    // (2) PARTITION clause: table name
                    // (3) PARTITION clause: column name
                    // (4) PARTITION clause: parameter number
                    // (5) PARTITION clause: table name 2 (or null)
                    // (6) PARTITION clause: column name 2 (or null)
                    // (7) PARTITION clause: parameter number 2 (or null)
                    thisData = new ProcedurePartitionData(matcher.group(2), matcher.group(3), matcher.group(4),
                                                          matcher.group(5), matcher.group(6), matcher.group(7));
                    thisType = "PARTITION";
                }
                // Can't mix and match types; and no repetition of clauses
                if (procType != null) {
                    String msg;
                    if (procType.equals(thisType)) {
                        msg = String.format("Only one %s clause is allowed for CREATE PROCEDURE.", procType);
                    } else {
                        msg = String.format("Cannot combine %s and %s clauses for CREATE PROCEDURE.", procType, thisType);
                    }
                    throw m_compiler.new VoltCompilerException(msg);
                }
                data = thisData;
                procType = thisType;
            }
        }

        return data;
    }

    /*
     * Maps a "type modifier" string to the corresponding
     * partition-data type.
     */
    private ProcedurePartitionData.Type convertTypeModifier(String str) throws VoltCompilerException {
        if (str.equalsIgnoreCase("DIRECTED")) {
            return ProcedurePartitionData.Type.DIRECTED;
        }
        else if (str.equalsIgnoreCase("COMPOUND")) {
            return ProcedurePartitionData.Type.COMPOUND;
        }
        else {
            String msg = String.format("Parser error: unknown procedure type modifier '%s'", str);
            throw m_compiler.new VoltCompilerException(msg);
        }
    }

    /*
     * Adds partition data to the procedure for tracking
     */
    protected void addProcedurePartitionInfo(String procName,
                                             ProcedurePartitionData data,
                                             String statement) throws VoltCompilerException {

        // Will be null when there is no optional partition clause
        if (data == null) {
            return;
        }

        // Check the identifiers.
        checkIdentifierStart(procName, statement);
        if (data.m_tableName != null) {
            checkIdentifierStart(data.m_tableName, statement);
            checkIdentifierStart(data.m_columnName, statement);

            // two partition procedure
            if (data.m_tableName2 != null) {
                checkIdentifierStart(data.m_tableName2, statement);
                checkIdentifierStart(data.m_columnName2, statement);

                if (data.m_paramIndex2 == null) {
                    String exceptionMsg = String.format("Two partition parameter must specify index for  "
                            + "second partitioning parameter if the first partitioning parameter index is non-zero.");
                    throw m_compiler.new VoltCompilerException(exceptionMsg);
                }
            }
        }

        m_tracker.addProcedurePartitionInfoTo(procName, data);
    }
}
