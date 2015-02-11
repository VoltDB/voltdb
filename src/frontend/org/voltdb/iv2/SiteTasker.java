/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.iv2;

import java.io.IOException;

import org.voltdb.rejoin.TaskLog;

import org.voltdb.SiteProcedureConnection;

public abstract class SiteTasker {

    public static abstract class SiteTaskerRunnable extends SiteTasker {
        abstract void run();

        public void run(SiteProcedureConnection siteConnection) {
            run();
        }

        public void runForRejoin(SiteProcedureConnection siteConnection,
                TaskLog rejoinTaskLog) throws IOException {
            run();
        }
    }

    /**
     * Run executes the task. Run is called on the ExecutionSite thread
     * and has exclusive access to the ee. Tasks are not preempted.
     */
    abstract public void run(SiteProcedureConnection siteConnection);

    /**
     * Run the task on an inconsistent/rejoining EE.
     */
    abstract public void runForRejoin(SiteProcedureConnection siteConnection,
            TaskLog rejoinTaskLog) throws IOException;

}
