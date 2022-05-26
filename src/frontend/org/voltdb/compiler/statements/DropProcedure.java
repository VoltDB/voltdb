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

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Task;
import org.voltdb.catalog.TaskParameter;
import org.voltdb.catalog.Topic;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process DROP PROCEDURE procedure-name [IF EXISTS]
 */
public class DropProcedure extends StatementProcessor {

    public DropProcedure(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // Matches if it is DROP PROCEDURE <proc-name or classname>
        Matcher statementMatcher = SQLParser.matchDropProcedure(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }

        String classOrProcName = checkIdentifierStart(statementMatcher.group(1), ddlStatement.statement);

        // DROP PROCEDURE can use the fully-qualified class name, similar to the CREATE PROCEDURE FROM CLASS statement
        // for all other purposes the simple name is used, so we need the simple name for all comparisons
        String simpleProcName = classOrProcName;
        if (classOrProcName.contains(".")) {
            simpleProcName = classOrProcName.substring(classOrProcName.lastIndexOf("."));
        }

        // check if used by any topics
        CatalogMap<Topic> topics = db.getTopics();
        for (Topic t : topics) {
            if (simpleProcName.equalsIgnoreCase(t.getProcedurename())) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid DROP PROCEDURE statement: %s is used by topic %s.",
                        classOrProcName, t.getTypeName()));
            }
        }

        // check if used by any Tasks
        CatalogMap<Task> tasks = db.getTasks();
        for (Task t : tasks) {
            String actionGenerator = t.getActiongeneratorclass();
            if (actionGenerator.equals(CreateTask.DEFAULT_ACTION_GENERATOR)) {
                for (TaskParameter tp : t.getActiongeneratorparameters()) {
                    String param = tp.getParameter();
                    if (param.equalsIgnoreCase(simpleProcName)) {
                        throw m_compiler.new VoltCompilerException(
                            String.format(
                                "Invalid DROP PROCEDURE statement: %s is used by task %s.",
                                classOrProcName, t.getName()));
                    }
                }
            }
        }

        // Extract the ifExists bool from group 2
        m_tracker.removeProcedure(classOrProcName, (statementMatcher.group(2) != null));

        return true;
    }

}
