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
        String prefix = m_firstProcessor.getCommandPrefix();
        if (prefix == null) {
            return false;
        }

        // Special handling for "COMPOUND|DIRECTED procedure"
        String procTypeModifier = null;
        if (prefix.endsWith(PROCEDURE)) {
            String[] parts = prefix.split("\\s+");
            if (parts.length == 2) {
                procTypeModifier = parts[0];
                prefix = parts[1];
            }
        }

        // Format the error message.  The standard layout is
        //     Invalid WHOZIT statement: "blah blah"
        //     expected syntax: "mumble mumble"
        //     or: "rhubarb rhubarb"
        // Careful, any changes here will likely break tests in
        // TestVoltCompiler, since they care about exact messages.
        // I found this out the hard way.
        String form = null;
        switch (prefix) {

        case PARTITION:
            form = "Invalid PARTITION statement: \"%s\"" +
                "\nexpected syntax: \"PARTITION TABLE table ON COLUMN column\"" +
                "\nor: \"PARTITION PROCEDURE procedure ON TABLE table COLUMN column [PARAMETER parameter-index-no]\"";
            break;

        case REPLICATE:
            form = "Invalid REPLICATE statement: \"%s\"" +
                "\nexpected syntax: \"REPLICATE TABLE table\"";
            break;

        case PROCEDURE:
            form = "Invalid CREATE PROCEDURE statement: \"%s\"" +
                "\nexpected syntax: \"CREATE PROCEDURE [ALLOW role, ...] [PARTITION partitioning] FROM CLASS class-name\"" +
                "\nor: \"CREATE PROCEDURE name [ALLOW role, ...] [PARTITION partitioning] AS single-select-or-dml-statement\"" +
                "\nwhere: partitioning is \"ON TABLE table COLUMN column [PARAMETER parameter-index-no]\"";

            if (procTypeModifier != null) {
                switch (procTypeModifier) {
                case "COMPOUND":
                    form = "Invalid CREATE COMPOUND PROCEDURE statement: \"%s\"" +
                        "\nexpected syntax: \"CREATE COMPOUND PROCEDURE [ALLOW role, ...] FROM CLASS class-name\"";
                    break;
                case "DIRECTED":
                    form = "Invalid CREATE DIRECTED PROCEDURE statement: \"%s\"" +
                        "\nexpected syntax: \"CREATE DIRECTED PROCEDURE [ALLOW role, ...] FROM CLASS class-name\" " +
                        "\nor: \"CREATE DIRECTED PROCEDURE name [ALLOW role, ...] AS single-select-or-dml-statement\"";
                    break;
                }
            }
            break;

        case FUNCTION:
            form = "Invalid CREATE FUNCTION statement: \"%s\"" +
                "\nexpected syntax: \"CREATE FUNCTION name FROM METHOD class-name.method-name\"";
            break;

        case ROLE:
            form = "Invalid CREATE ROLE statement: \"%s\"" +
                "\nexpected syntax: \"CREATE ROLE role\"";
            break;

        case DR:
            form = "Invalid DR TABLE statement: \"%s\"" +
                "\nexpected syntax: \"DR TABLE table [DISABLE]\"";
            break;

        case TASK:
            form = "Invalid CREATE TASK statement: \"%s\"" +
                "\nexpected syntax: \"CREATE TASK name" +
                " ON SCHEDULE (CRON exp | DELAY interval unit | EVERY interval unit)" +
                " PROCEDURE procedure [WITH (arg, ...)] [options]" +
                "\nor: \"CREATE TASK name FROM CLASS class [WITH (arg, ...)] [options]" +
                "\nwhere: options are \"[ON ERROR (STOP | LOG | IGNORE)] [RUN ON (DATABASE | HOSTS | PARTITIONS)] [AS USER user] [ENABLE | DISABLE]\"";
            break;

        case AGGREGATE:
            form = "Invalid CREATE AGGREGATE FUNCTION statement: \"%s\"" +
                "\nexpected syntax: \"CREATE AGGREGATE FUNCTION name FROM CLASS class-name\"";
            break;

        case TOPIC:
            form = "Invalid CREATE TOPIC statement: \"%s\"" +
                "\nexpected syntax: \"CREATE TOPIC [USING STREAM] name [EXECUTE PROCEDURE proc]" +
                " [ALLOW role, ...] [PROFILE profile] [PROPERTIES (key1=val1, ...)]\"";
            break;

        case OPAQUE:
            form = "Invalid CREATE OPAQUE TOPIC statement: \"%s\"" +
                "expected syntax: \"CREATE OPAQUE TOPIC name [PARTITIONED]" +
                " [ALLOW [PRODUCER | CONSUMER] role, ...] [PROFILE profile]\"";
            break;

        default:
            // Not a VoltDB-specific DDL statement.
            return false;
        }

        String stmt = ddlStatement.statement;
        int n = stmt.length() - 1;
        if (n >= 0 && stmt.charAt(n) == ';') {
            stmt = stmt.substring(0, n);
        }
        throw m_compiler.new VoltCompilerException(String.format(form, stmt));
    }
}
