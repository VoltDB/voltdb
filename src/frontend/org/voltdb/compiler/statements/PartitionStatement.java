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
import org.voltdb.ProcedurePartitionData;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process both PARTITION TABLE and PARTITION PROCEDURE statements.
 */
public class PartitionStatement extends StatementProcessor {

    public PartitionStatement(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // Matches if it is the beginning of a partition statement
        Matcher statementMatcher = SQLParser.matchPartitionStatementPreamble(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        // either TABLE or PROCEDURE
        String partitionee = statementMatcher.group(1).toUpperCase();
        if (TABLE.equals(partitionee)) {
            return processPartitionTable(ddlStatement.statement);
        }
        else if (PROCEDURE.equals(partitionee)) {
            return processPartitionProcedure(ddlStatement, whichProcs);
        }
        // can't get here as regex only matches for PROCEDURE or TABLE
        return false;
    }

    private boolean processPartitionTable(String statement) throws VoltCompilerException {
        // matches if it is PARTITION TABLE <table> ON COLUMN <column>
        Matcher statementMatcher = SQLParser.matchPartitionTable(statement);

        if ( ! statementMatcher.matches()) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid PARTITION statement: \"%s\"" +
                    "\nexpected syntax: PARTITION TABLE table ON COLUMN column",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }
        // group(1) -> table, group(2) -> column
        String tableName = checkIdentifierStart(statementMatcher.group(1), statement);
        String columnName = checkIdentifierStart(statementMatcher.group(2), statement);
        VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
        if (tableXML != null) {
            tableXML.attributes.put("partitioncolumn", columnName.toUpperCase());
            // Column validity check done by VoltCompiler in post-processing

            // mark the table as dirty for the purposes of caching sql statements
            m_compiler.markTableAsDirty(tableName);
        }
        else {
            throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid PARTITION statement: table %s does not exist", tableName));
        }
        return true;
    }

    private boolean processPartitionProcedure(DDLStatement ddlStatement, DdlProceduresToLoad whichProcs) throws VoltCompilerException {
        if (whichProcs != DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
            return true;
        }

        // matches if it is
        //   PARTITION PROCEDURE <procedure>
        //      ON  TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]
        Matcher statementMatcher = SQLParser.matchPartitionProcedure(ddlStatement.statement);

        if ( ! statementMatcher.matches()) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid PARTITION statement: \"%s\"" +
                    "\nexpected syntax: PARTITION PROCEDURE procedure ON" +
                    " TABLE table COLUMN column [PARAMETER parameter-index-no]",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        // check the table portion of the partition info
        String tableName = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);

        // check the column portion of the partition info
        String columnName = checkIdentifierStart(statementMatcher.group(3), ddlStatement.statement);

        // if not specified default parameter index to 0
        String parameterNo = statementMatcher.group(4);
        if (parameterNo == null) {
            parameterNo = "0";
        }

        ProcedurePartitionData partitionData = new ProcedurePartitionData(tableName, columnName, parameterNo);
        // procedureName -> group(1), partitionInfo -> group(2)
        m_tracker.addProcedurePartitionInfoTo(
                checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement), partitionData
                );

        // this command is now deprecated as of VoltDB 7.0
        m_compiler.addWarn("The standalone \"PARTITION PROCEDURE ...\" statement is deprecated. " +
                           "Please use the combined statement \"CREATE PROCEDURE PARTITION ON ...\" " +
                           "instead. See the documentation of \"CREATE PROCEDURE\" for more information.",
                           ddlStatement.lineNo);

        return true;
    }

}
