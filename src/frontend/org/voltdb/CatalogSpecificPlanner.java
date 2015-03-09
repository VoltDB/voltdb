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
package org.voltdb;

import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.AdHocPlannerWork;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWork.AsyncCompilerWorkCompletionHandler;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

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

    public ListenableFuture<AdHocPlannedStmtBatch> plan(String sql,
            Object[] userParams, boolean singlePartition) {
        final SettableFuture<AdHocPlannedStmtBatch> retval = SettableFuture.create();
        AsyncCompilerWorkCompletionHandler completionHandler = new AsyncCompilerWorkCompletionHandler()
        {
            @Override
            public void onCompletion(AsyncCompilerResult result) {
                retval.set((AdHocPlannedStmtBatch)result);
            }
        };
        AdHocPlannerWork work = AdHocPlannerWork.makeStoredProcAdHocPlannerWork(-1, sql, userParams,
                singlePartition, m_catalogContext, completionHandler);
        m_agent.compileAdHocPlanForProcedure(work);
        return retval;
    }
}
