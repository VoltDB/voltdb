/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
import org.voltdb.catalog.Task;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Handle processing ddl which matches {@link SQLParser#matchAlterTask(String)}
 */
public class AlterTask extends StatementProcessor {
    public AlterTask(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher matcher = SQLParser.matchAlterTask(ddlStatement.statement);
        if (!matcher.matches()) {
            return false;
        }

        String name = matcher.group("name");
        Task task = db.getTasks().get(name);
        if (task == null) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Schedule name \"%s\" in ALTER TASK statement does not exist.", name));
        }

        String action = matcher.group("action");
        if (action != null) {
            boolean enable = "ENABLE".equalsIgnoreCase(action);

            assert enable || "DISABLE".equalsIgnoreCase(action);

            if (enable == task.getEnabled()) {
                throw m_compiler.new VoltCompilerException(
                        String.format("Schedule \"%s\" is already %s", name, enable ? "enabled" : "disabled"));
            }
            task.setEnabled(enable);
        }

        String onError = matcher.group("onError");
        if (onError != null) {
            task.setOnerror(onError);
        }

        return true;
    }
}
