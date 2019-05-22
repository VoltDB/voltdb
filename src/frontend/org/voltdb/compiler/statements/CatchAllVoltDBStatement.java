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

import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;

/**
 * If a statement gets to this processor, it means that the statement was identified as
 * a VoltDB-specific statement, but could have syntax errors.
 * We will throw exceptions here with the correct syntax information.
 */
public class CatchAllVoltDBStatement extends StatementProcessor {

    private VoltDBStatementProcessor m_firstProcessor;

    public CatchAllVoltDBStatement(DDLCompiler ddlCompiler, VoltDBStatementProcessor firstProcessor) {
        super(ddlCompiler);
        m_firstProcessor = firstProcessor;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        /*
         * if no correct syntax regex matched above then at this juncture
         * the statement is syntax incorrect
         */
        if (PARTITION.equals(m_firstProcessor.getCommandPrefix())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid PARTITION statement: \"%s\", " +
                    "expected syntax: \"PARTITION TABLE <table> ON COLUMN <column>\" or " +
                    "\"PARTITION PROCEDURE <procedure> ON " +
                    "TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]\"",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        if (REPLICATE.equals(m_firstProcessor.getCommandPrefix())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid REPLICATE statement: \"%s\", " +
                    "expected syntax: REPLICATE TABLE <table>",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        if (PROCEDURE.equals(m_firstProcessor.getCommandPrefix())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE PROCEDURE statement: \"%s\", " +
                    "expected syntax: \"CREATE PROCEDURE [ALLOW <role> [, <role> ...] FROM CLASS <class-name>\" " +
                    "or: \"CREATE PROCEDURE <name> [ALLOW <role> [, <role> ...] AS <single-select-or-dml-statement>\"",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        if (FUNCTION.equals(m_firstProcessor.getCommandPrefix())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE FUNCTION statement: \"%s\", " +
                    "expected syntax: \"CREATE FUNCTION <name> FROM METHOD <class-name>.<method-name>\"",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        if (ROLE.equals(m_firstProcessor.getCommandPrefix())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE ROLE statement: \"%s\", " +
                    "expected syntax: CREATE ROLE <role>",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        if (DR.equals(m_firstProcessor.getCommandPrefix())) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid DR TABLE statement: \"%s\", " +
                    "expected syntax: DR TABLE <table> [DISABLE]",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }
        
        if (AGGREGATE.equals(m_firstProcessor.getCommandPrefix())) {
        	throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE AGGREGATE FUNCTION statement: \"%s\", " +
                    "expected syntax: \"CREATE AGGREGATE FUNCTION <name> FROM CLASS <class-name>\"",
                    ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1))); // remove trailing semicolon
        }

        // Not a VoltDB-specific DDL statement.
        return false;
    }
}
