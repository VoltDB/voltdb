/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

import org.hsqldb_voltpatches.Scanner;
import org.hsqldb_voltpatches.Tokens;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcedureSchedule;
import org.voltdb.catalog.SchedulerParam;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.sched.SchedulerManager;

import com.google_voltpatches.common.base.MoreObjects;

/**
 * Handle processing ddl which matches {@link SQLParser#matchCreateSchedule(String)}
 */
public class CreateSchedule extends StatementProcessor {
    public CreateSchedule(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher matcher = SQLParser.matchCreateSchedule(ddlStatement.statement);
        if (!matcher.matches()) {
            return false;
        }

        CatalogMap<ProcedureSchedule> schedules = db.getProcedureschedules();
        String name = matcher.group("name");
        if (schedules.get(name) != null) {
            throw m_compiler.new VoltCompilerException("Schedule with name already exists: " + name);
        }

        ProcedureSchedule schedule = schedules.add(name);
        configureScheduler(schedule, matcher, ddlStatement.newDdl);

        SchedulerManager.SchedulerValidationResult result = SchedulerManager.validateScheduler(schedule, m_classLoader);
        if (!result.isValid()) {
            schedules.delete(name);
            throw m_compiler.new VoltCompilerException(result.getErrorMessage());
        }
        return true;
    }

    private ProcedureSchedule configureScheduler(ProcedureSchedule schedule, Matcher matcher, boolean newDdl)
            throws VoltCompilerException {
        schedule.setName(matcher.group("name"));
        schedule.setRunlocation(
                MoreObjects.firstNonNull(matcher.group("runLocation"), SchedulerManager.RUN_LOCATION_DEFAULT));
        schedule.setSchedulerclass(matcher.group("class"));
        String user = matcher.group("asUser");
        // If no user is set and this is new DDL use the user which is creating the schedule
        schedule.setUser(user == null && newDdl ? m_compiler.getUser() : user);
        schedule.setEnabled(matcher.group("disabled") == null);
        fillOutParams(schedule.getParameters(), matcher.group("parameters"));

        return schedule;
    }

    private void fillOutParams(CatalogMap<SchedulerParam> params, String paramsCsv) throws VoltCompilerException {
        if (paramsCsv == null) {
            return;
        }

        Scanner scanner = new Scanner();
        scanner.reset(paramsCsv);
        int i = 0;
        boolean minus = false;
        while (true) {
            scanner.scanNext();
            switch (scanner.getTokenType()) {
            case Tokens.X_VALUE:
                SchedulerParam param = params.add(Integer.toString(i));
                param.setIndex(i);
                String paramString = minus ? '-' + scanner.getString() : scanner.getString();
                param.setParameter(paramString);
                ++i;
                minus = false;
                break;
            case Tokens.MINUS:
                minus = true;
                break;
            case 664: /** Same as {@link Tokens#COMMA} but that isn't visible */
                if (minus) {
                    throw m_compiler.new VoltCompilerException("Minus sign encountered by itself");
                }
                break;
            case Tokens.X_ENDPARSE:
                return;
            default:
                throw m_compiler.new VoltCompilerException(
                        "Unsupported token found in parameters: " + scanner.getString());
            }
        }
    }
}
