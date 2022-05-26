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
import org.voltdb.catalog.Group;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process CREATE ROLE role-name [WITH permission [,...]]
 */
public class DropRole extends StatementProcessor {

    public DropRole(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // matches if it is DROP ROLE
        // group 1 is role name
        Matcher statementMatcher = SQLParser.matchDropRole(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        String roleName = statementMatcher.group(1).toUpperCase();
        boolean ifExists = (statementMatcher.group(2) != null);
        CatalogMap<Group> groupMap = db.getGroups();
        if (groupMap.get(roleName) == null) {
            if (!ifExists) {
                throw m_compiler.new VoltCompilerException(String.format(
                            "Role name \"%s\" in DROP ROLE statement does not exist.",
                            roleName));
            }
            else {
                return true;
            }
        }
        else {
            // Hand-check against the two default roles which shall not be
            // dropped.
            if (roleName.equals("ADMINISTRATOR") || roleName.equals("USER")) {
                throw m_compiler.new VoltCompilerException(String.format(
                            "You may not drop the built-in role \"%s\".",
                            roleName));
            }
            // The constraint that there be no users with this role gets
            // checked by the deployment validation.  *HOWEVER*, right now
            // this ends up giving a confusing error message.
            groupMap.delete(roleName);
        }
        return true;
    }

}
