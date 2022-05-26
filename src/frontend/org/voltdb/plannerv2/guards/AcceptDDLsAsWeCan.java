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

package org.voltdb.plannerv2.guards;

import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.plannerv2.SqlTask;
import org.voltdb.plannerv2.SqlTaskImpl;

/**
 * Allow all DDLs if we support them.
 * @author Yiqun Zhang
 * @since 9.0
 */
public class AcceptDDLsAsWeCan extends CalciteCompatibilityCheck {

    private static String truncate(String src, int max) {
        if (src.length() <= max) {
            return src;
        } else {
            return src.substring(0, max) + "...";
        }
    }

    @Override
    protected boolean doCheck(String sql) throws SqlParseException {
        try {
            final SqlTask task = new SqlTaskImpl(sql);
            return task.isDDL() /*|| task.isDQL()*/;    // NOTE: enabling isDQL() check is the last stand to enable all DQL planning through Calcite.
        } catch (SqlParseException e) {
            if (e.getCause() instanceof StackOverflowError) {
                // Note - ethan - 12/28/2008:
                // I think this is copied from NonDdlBatchCompiler and
                // AdHocNTBase.compileAdHocSQL() by Lukai.
                // It is necessary, and the purpose is to abort planning.
                // There is a JUnit test case to check this.

                // Throwing an exception that's not a PlannerFallbackException will abort the planning.
                throw new PlanningErrorException("Encountered stack overflow error. " +
                        "Try reducing the number of predicate expressions in the query.");
            } else { // For all Calcite unsupported syntax, fall back to VoltDB implementation
                throw e;    // The fall back occurs in SqlBatchImpl's ctor's try-catch block calling CalciteSqlCheck.
            }
        }
    }
}
