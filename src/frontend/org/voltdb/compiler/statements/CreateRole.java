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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.common.Permission;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;

/**
 * Process CREATE ROLE role-name [WITH permission [,...]]
 */
public class CreateRole extends StatementProcessor {

    public CreateRole(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // matches if it is CREATE ROLE [WITH <permission> [, <permission> ...]]
        // group 1 is role name
        // group 2 is comma-separated permission list or null if there is no WITH clause
        Matcher statementMatcher = SQLParser.matchCreateRole(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        String roleName = statementMatcher.group(1).toLowerCase();
        CatalogMap<Group> groupMap = db.getGroups();
        if (groupMap.get(roleName) != null) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Role name \"%s\" in CREATE ROLE statement already exists.",
                    roleName));
        }
        Group catGroup = groupMap.add(roleName);
        if (statementMatcher.group(2) != null) {
            try {
                EnumSet<Permission> permset =
                        Permission.getPermissionsFromAliases(Arrays.asList(StringUtils.split(statementMatcher.group(2), ',')));
                Permission.setPermissionsInGroup(catGroup, permset);
            } catch (IllegalArgumentException iaex) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid permission \"%s\" in CREATE ROLE statement: \"%s\", " +
                                "available permissions: %s", iaex.getMessage(),
                                ddlStatement.statement.substring(0, ddlStatement.statement.length() - 1), // remove trailing semicolon
                        Permission.toListString()));
            }
        }
        return true;
    }

}
