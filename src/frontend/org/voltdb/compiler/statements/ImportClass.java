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

import org.voltdb.catalog.Database;
import org.voltdb.compiler.ClassMatcher.ClassNameMatchStatus;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.utils.InMemoryJarfile;

/**
 * Process IMPORT CLASS <class-name>, notice that this is deprecated.
 */
public class ImportClass extends StatementProcessor {

    public ImportClass(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        // match IMPORT CLASS statements
        Matcher statementMatcher = SQLParser.matchImportClass(ddlStatement.statement);
        if (! statementMatcher.matches()) {
            return false;
        }
        if (whichProcs == DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
            // Semi-hacky way of determining if we're doing a cluster-internal compilation.
            // Command-line compilation will never have an InMemoryJarfile.
            if (!(m_classLoader instanceof InMemoryJarfile.JarLoader)) {
                // Only process the statement if this is not for the StatementPlanner
                String classNameStr = statementMatcher.group(1);

                // check that the match pattern is a valid match pattern
                checkIdentifierWithWildcard(classNameStr, ddlStatement.statement);

                ClassNameMatchStatus matchStatus = m_classMatcher.addPattern(classNameStr);
                if (matchStatus == ClassNameMatchStatus.NO_EXACT_MATCH) {
                    throw m_compiler.new VoltCompilerException(String.format(
                                "IMPORT CLASS not found: '%s'",
                                classNameStr)); // remove trailing semicolon
                }
                else if (matchStatus == ClassNameMatchStatus.NO_WILDCARD_MATCH) {
                    m_compiler.addWarn(String.format(
                                "IMPORT CLASS no match for wildcarded class: '%s'",
                                classNameStr), ddlStatement.lineNo);
                }
            }
            else {
                m_compiler.addInfo("Internal cluster recompilation ignoring IMPORT CLASS line: " +
                        ddlStatement.statement);
            }
            // Need to track the IMPORT CLASS lines even on internal compiles so that
            // we don't lose them from the DDL source.  When the @UAC path goes away,
            // we could change this.
            m_tracker.addImportLine(ddlStatement.statement);
        }

        return true;
    }

    /**
     * Checks whether or not the start of the given identifier is java (and
     * thus DDL) compliant. An identifier may start with: _ [a-zA-Z] $ *
     * and contain subsequent characters including: _ [0-9a-zA-Z] $ *
     * @param identifier the identifier to check
     * @param statement the statement where the identifier is
     * @return the given identifier unmodified
     * @throws VoltCompilerException when it is not compliant
     */
    private String checkIdentifierWithWildcard(
            final String identifier, final String statement
            ) throws VoltCompilerException {

        assert identifier != null && ! identifier.trim().isEmpty();
        assert statement != null && ! statement.trim().isEmpty();

        int loc = 0;
        do {
            if ( ! Character.isJavaIdentifierStart(identifier.charAt(loc)) && identifier.charAt(loc)!= '*') {
                String msg = "Unknown indentifier in DDL: \"" +
                        statement.substring(0,statement.length()-1) +
                        "\" contains invalid identifier \"" + identifier + "\"";
                throw m_compiler.new VoltCompilerException(msg);
            }
            loc++;
            while (loc < identifier.length() && identifier.charAt(loc) != '.') {
                if (! Character.isJavaIdentifierPart(identifier.charAt(loc)) && identifier.charAt(loc)!= '*') {
                    String msg = "Unknown indentifier in DDL: \"" +
                            statement.substring(0,statement.length()-1) +
                            "\" contains invalid identifier \"" + identifier + "\"";
                    throw m_compiler.new VoltCompilerException(msg);
                }
                loc++;
            }
            if (loc < identifier.length() && identifier.charAt(loc) == '.') {
                loc++;
                if (loc >= identifier.length()) {
                    String msg = "Unknown indentifier in DDL: \"" +
                            statement.substring(0,statement.length()-1) +
                            "\" contains invalid identifier \"" + identifier + "\"";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }
        }
        while( loc > 0 && loc < identifier.length());

        return identifier;
    }

}
