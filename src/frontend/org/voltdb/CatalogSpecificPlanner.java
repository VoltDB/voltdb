/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb;

import java.util.Arrays;

import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.AdHocPlannerWork;

import com.google.common.util.concurrent.ListenableFuture;

/*
 * Wrapper around a planner tied to a specific catalog version. This planner
 * is specifically configured to generate plans from within a stored procedure
 * so it will give a slightly different set of config to the planner
 * via AdHocPlannerWork
 */
public class CatalogSpecificPlanner {
    private final AsyncCompilerAgent m_agent;
    private final CatalogContext m_catalogContext;

    public CatalogSpecificPlanner(AsyncCompilerAgent agent, CatalogContext context) {
        m_agent = agent;
        m_catalogContext = context;
    }

    public ListenableFuture<AdHocPlannedStmtBatch> plan(String sql, boolean multipart) {
        /*
         * If this is multi-part, don't give the planner a partition param AND
         * tell it not to infer whether the plan is single part. Those optimizations
         * are fine for adhoc SQL planned outside a stored proc, but not when those
         * factors have already been determined by the proc.
         */
        AdHocPlannerWork work =
            new AdHocPlannerWork(
                    -1, false, 0, 0, "", false, null, //none of the params on this line are used
                    sql, Arrays.asList(new String[] { sql }), multipart ? null : 0, m_catalogContext, true, !multipart);
        return m_agent.compileAdHocPlanFuture(work);
    }
}
