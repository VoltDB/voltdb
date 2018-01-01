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

import org.hsqldb_voltpatches.VoltXMLElement;
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

    public static final String PARTITION_PROCEDURE_STATEMENT_ERROR_MESSAGE = "Deprecated PARTITION PROCEDURE statement found."
            + " Please use the PARTITION ON clause of the CREATE PARTITION statement to"
            + " declare and partition the procedure in a single combined statement.";

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
            throw m_compiler.new VoltCompilerException(PARTITION_PROCEDURE_STATEMENT_ERROR_MESSAGE);
        }
        // can't get here as regex only matches for PROCEDURE or TABLE
        return false;
    }

    private boolean processPartitionTable(String statement) throws VoltCompilerException {
        // matches if it is PARTITION TABLE <table> ON COLUMN <column>
        Matcher statementMatcher = SQLParser.matchPartitionTable(statement);

        if ( ! statementMatcher.matches()) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid PARTITION statement: \"%s\", " +
                    "expected syntax: PARTITION TABLE <table> ON COLUMN <column>",
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
}
