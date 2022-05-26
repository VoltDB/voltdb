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
import org.voltdb.catalog.Database;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process REPLICATE TABLE <table-name>
 */
public class ReplicateTable extends StatementProcessor {

    public ReplicateTable(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // matches if it is REPLICATE TABLE <table-name>
        Matcher statementMatcher = SQLParser.matchReplicateTable(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        // group(1) -> table
        String tableName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);
        VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
        if (tableXML != null) {
            tableXML.attributes.remove("partitioncolumn");

            // mark the table as dirty for the purposes of caching sql statements
            m_compiler.markTableAsDirty(tableName);
        }
        else {
            throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid REPLICATE statement: table %s does not exist", tableName));
        }
        return true;
    }

}
