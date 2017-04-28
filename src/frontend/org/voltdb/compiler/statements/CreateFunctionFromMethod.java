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

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process CREATE FUNCTION <function-name> FROM METHOD <class-name>.<method-name>
 */
public class CreateFunctionFromMethod extends StatementProcessor {

    public CreateFunctionFromMethod(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // Matches if it is CREATE FUNCTION <name> FROM METHOD <class-name>.<method-name>
        Matcher statementMatcher = SQLParser.matchCreateFunctionFromMethod(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        // Before we complete the user-defined function feature, executing UDF-related DDLs will
        // generate compiler warnings.
        m_compiler.addWarn("User-defined functions are not implemented yet.");

        String functionName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
        String className = checkIdentifierStart(statementMatcher.group(2), ddlStatement.statement);
        String methodName = checkIdentifierStart(statementMatcher.group(3), ddlStatement.statement);
        CatalogMap<Function> functions = db.getFunctions();
        if (functions.get(functionName) != null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Function name \"%s\" in CREATE FUNCTION statement already exists.",
                    functionName));
        }
        Function func = functions.add(functionName);
        func.setFunctionname(functionName);
        func.setClassname(className);
        func.setMethodname(methodName);
        return true;
    }

}
