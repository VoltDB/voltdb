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

import org.hsqldb_voltpatches.Scanner;
import org.hsqldb_voltpatches.Tokens;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Task;
import org.voltdb.catalog.TaskParameter;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DDLCompiler.DDLStatement;
import org.voltdb.compiler.DDLCompiler.StatementProcessor;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.parser.SQLParser;
import org.voltdb.task.CronIntervalGenerator;
import org.voltdb.task.DelayIntervalGenerator;
import org.voltdb.task.IntervalIntervalGenerator;
import org.voltdb.task.SingleProcGenerator;
import org.voltdb.task.TaskScope;

import com.google_voltpatches.common.base.MoreObjects;

/**
 * Handle processing ddl which matches {@link SQLParser#matchCreateTask(String)}
 */
public class CreateTask extends StatementProcessor {
    public CreateTask(DDLCompiler ddlCompiler) {
        super(ddlCompiler);
    }

    public static final String DEFAULT_ACTION_GENERATOR = SingleProcGenerator.class.getName();

    @Override
    protected boolean processStatement(DDLStatement ddlStatement, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Matcher matcher = SQLParser.matchCreateTask(ddlStatement.statement);
        if (!matcher.matches()) {
            return false;
        }

        CatalogMap<Task> tasks = db.getTasks();
        String name = matcher.group("name");
        if (tasks.get(name) != null) {
            throw m_compiler.new VoltCompilerException("Schedule with name already exists: " + name);
        }

        Task task = tasks.add(name);
        configureTask(task, matcher, ddlStatement.newDdl);
        return true;
    }

    private Task configureTask(Task task, Matcher matcher, boolean newDdl)
            throws VoltCompilerException {
        task.setName(matcher.group("name"));
        String scopeString = matcher.group("scope");
        task.setScope(TaskScope.fromName(scopeString).getId());
        if (matcher.group("class") != null) {
            task.setSchedulerclass(matcher.group("class"));
            fillOutParams(task.getSchedulerparameters(), matcher.group("parameters"), 0);
            if (matcher.group("procedure") != null) {
                throw m_compiler.new VoltCompilerException(
                        "Schedule configures procdure when scheduler parameters are expected.");
            }
        } else {
            CatalogMap<TaskParameter> scheduleParams = task.getScheduleparameters();
            String scheduleClass = matcher.group("scheduleClass");

            if (scheduleClass == null) {
                if (matcher.group("cron") != null) {
                    task.setScheduleclass(CronIntervalGenerator.class.getName());
                    addParameter(scheduleParams, 0, matcher.group("cron"));
                } else {
                    String intervalSchedule = matcher.group("intervalSchedule");
                    if ("delay".equalsIgnoreCase(intervalSchedule)) {
                        task.setScheduleclass(DelayIntervalGenerator.class.getName());
                    } else if ("every".equalsIgnoreCase(intervalSchedule)) {
                        task.setScheduleclass(IntervalIntervalGenerator.class.getName());
                    } else {
                        throw m_compiler.new VoltCompilerException("Could not determine type of scheduler to use");
                    }
                    addParameter(scheduleParams, 0, matcher.group("interval"));
                    addParameter(scheduleParams, 1, matcher.group("timeUnit").toUpperCase());
                }
            } else {
                task.setScheduleclass(scheduleClass);
                fillOutParams(scheduleParams, matcher.group("scheduleParameters"), 0);
            }

            CatalogMap<TaskParameter> actionGeneratorParams = task.getActiongeneratorparameters();
            String generatorClass = matcher.group("generatorClass");

            int index = 0;

            if (generatorClass == null) {
                task.setActiongeneratorclass(DEFAULT_ACTION_GENERATOR);
                addParameter(actionGeneratorParams, index++, matcher.group("procedure"));
            } else {
                task.setActiongeneratorclass(generatorClass);
            }
            fillOutParams(actionGeneratorParams, matcher.group("parameters"), index);
        }
        String user = matcher.group("asUser");
        // If no user is set and this is new DDL use the user which is creating the schedule
        task.setUser(user == null && newDdl ? m_compiler.getUser() : user);
        task.setEnabled(matcher.group("disabled") == null);
        task.setOnerror(MoreObjects.firstNonNull(matcher.group("onError"), "STOP").toUpperCase());

        return task;
    }

    private void fillOutParams(CatalogMap<TaskParameter> params, String paramsCsv, int startIndex)
            throws VoltCompilerException {
        if (paramsCsv == null) {
            return;
        }

        Scanner scanner = new Scanner();
        scanner.reset(paramsCsv);
        int i = startIndex;
        boolean minus = false;
        while (true) {
            scanner.scanNext();
            switch (scanner.getTokenType()) {
            case Tokens.X_VALUE:
                addParameter(params, i++, minus ? '-' + scanner.getString() : scanner.getString());
                minus = false;
                break;
            case Tokens.MINUS:
                minus = true;
                break;
            case Tokens.NULL:
            case Tokens.X_NULL:
                addParameter(params, i++, null);
                //$FALL-THROUGH$
            case 664: /** Same as {@link Tokens#COMMA} but that isn't visible */
                if (minus) {
                    throw m_compiler.new VoltCompilerException("Minus sign encountered by itself");
                }
                break;
            case Tokens.X_ENDPARSE:
                return;
            default:
                throw m_compiler.new VoltCompilerException(
                        "Unsupported token found encountered: " + scanner.getString());
            }
        }
    }

    private static void addParameter(CatalogMap<TaskParameter> params, int index, String param) {
        TaskParameter taskParam = params.add(Integer.toString(index));
        taskParam.setIndex(index);
        taskParam.setParameter(param);
    }
}
