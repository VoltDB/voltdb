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
package org.voltdb.tasks.clockskew;

import org.voltdb.InternalConnectionContext;
import org.voltdb.VoltDBInterface;
import org.voltdb.client.Priority;
import org.voltdb.task.ActionResult;
import org.voltdb.task.ActionScheduler;
import org.voltdb.task.ScheduledAction;
import org.voltdb.task.TaskHelper;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.voltdb.ClockSkewCollectorAgent.COLLECT;
import static org.voltdb.ClockSkewCollectorAgent.PROCEDURE;
import static org.voltdb.task.ScheduledAction.callback;
import static org.voltdb.task.ScheduledAction.exit;

public class ClockSkewCollectorScheduler implements ActionScheduler {
    public static final Duration DEFAULT = Duration.ofHours(1);
    private static final Duration DISABLED = Duration.ZERO;
    private static final Duration LOWEST_ALLOWED = Duration.ofMinutes(1);
    private static final Duration FIRST_ACTION_DELAY = Duration.ofMillis(100);

    private static final InternalConnectionContext ICC = new InternalConnectionContext() {
        @Override
        public String getName() {
            return "GatherSkewScheduler";
        }

        @Override
        public int getPriority() {
            return Priority.SYSTEM_PRIORITY;
        }
    };

    private final ScheduledAction action;
    private final ScheduledAction firstAction;
    private final VoltDBInterface voltDb;
    private final ClockSkewStats skewStats;

    public ClockSkewCollectorScheduler(VoltDBInterface voltDb, TaskHelper taskHelper, ClockSkewStats skewStats, Duration interval) {
        this.voltDb = voltDb;
        this.skewStats = skewStats;

        taskHelper.logInfo("Initializing Clock Skew Collector with interval of " + interval);

        if (DISABLED.equals(interval)){
            action = exit("Scheduler is disabled");
            firstAction = action;
        } else {
            if (LOWEST_ALLOWED.compareTo(interval) > 0) {
                interval = DEFAULT;
            }
            firstAction = callback(FIRST_ACTION_DELAY.toMillis(), MILLISECONDS, this::gatherClockSkew);
            action = callback(interval.toMillis(), MILLISECONDS, this::gatherClockSkew);
        }
    }

    ScheduledAction getAction() {
        return action;
    }

    @Override
    public ScheduledAction getFirstScheduledAction() {
        return firstAction;
    }

    // TODO pk - how do we get notification about topo change - we should gather skews again for new hosts
    private ScheduledAction gatherClockSkew(ActionResult actionResult) {
        skewStats.clear();
        voltDb
                .getClientInterface()
                .getInternalConnectionHandler()
                .callProcedure(ICC, skewStats, response -> {}, PROCEDURE, COLLECT);

        return action;
    }
}
