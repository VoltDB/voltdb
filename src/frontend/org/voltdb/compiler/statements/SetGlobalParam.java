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

import org.voltdb.catalog.Database;
import org.voltdb.catalog.DatabaseConfiguration;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process SET <PARAMETER NAME> <PARAMETER VALUE>
 */
public class SetGlobalParam extends StatementProcessor {

    public SetGlobalParam(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher statementMatcher = SQLParser.matchSetGlobalParam(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        String name = statementMatcher.group(1).toUpperCase();
        switch (name) {
            case DatabaseConfiguration.DR_MODE_NAME:
                m_compiler.addWarn("Setting DR mode in the DDL is deprecated. Please use the role attribute of the <dr> tag in the deployment file.");
                break;
            default:
                throw m_compiler.new VoltCompilerException(String.format(
                    "Unknown global parameter: %s. Candidate parameters are %s", name, DatabaseConfiguration.allNames));
        }
        return true;
    }
}
