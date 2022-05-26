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

import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process DROP FUNCTION <function-name>
 */
public class DropFunction extends StatementProcessor {
    private static final VoltLogger m_logger = new VoltLogger("UDF");

    public DropFunction(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    /**
     * Remove the function with the given name from the VoltXMLElement schema
     * if it is there already.
     *
     * @param functionName
     * @return Return true iff the function is removed.
     *         Return false if the function does not exist in the schema.
     */
    private boolean removeUDFInSchema(String functionName) {
        for (int idx = 0; idx < m_schema.children.size(); idx++) {
            VoltXMLElement func = m_schema.children.get(idx);
            if ("ud_function".equals(func.name)) {
                String fnm = func.attributes.get("name");
                if (fnm != null && functionName.equals(fnm)) {
                    m_schema.children.remove(idx);
                    m_tracker.addDroppedFunction(functionName);
                    m_logger.debug(String.format("Removed XML for"
                            + " function named %s", functionName));
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // Matches if it is DROP FUNCTION <name>
        Matcher statementMatcher = SQLParser.matchDropFunction(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        String functionName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement).toLowerCase();
        boolean ifExists = statementMatcher.group(2) != null;
        if ( ! removeUDFInSchema(functionName)) {
            if ( ! ifExists ) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Function name \"%s\" in DROP FUNCTION statement does not exist.",
                        functionName));
            }
        }
        FunctionForVoltDB.deregisterUserDefinedFunction(functionName);
        return true;
    }
}
