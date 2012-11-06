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

package org.voltdb.sysprocs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SQLStmtAdHocHelper;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;

/**
 * Base class for @AdHoc... system procedures.
 *
 * Provides default implementation for VoltSystemProcedure call-backs.
 */
public abstract class AdHocBase extends VoltSystemProcedure {

    /* (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#init()
     */
    @Override
    public void init() {}

    /* (non-Javadoc)
     * @see org.voltdb.VoltSystemProcedure#executePlanFragment(java.util.Map, long, org.voltdb.ParameterSet, org.voltdb.SystemProcedureExecutionContext)
     */
    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // This code should never be called.
        assert(false);
        return null;
    }

    /**
     * Call from derived class run() method for implementation.
     *
     * @param ctx
     * @param aggregatorFragments
     * @param collectorFragments
     * @param sqlStatements
     * @param replicatedTableDMLFlags
     * @return
     */
    public VoltTable[] runAdHoc(SystemProcedureExecutionContext ctx, byte[] serializedBatchData) {

        // Collections must be the same size since they all contain slices of the same data.
        assert(serializedBatchData != null);

        ByteBuffer buf = ByteBuffer.wrap(serializedBatchData);
        AdHocPlannedStatement[] statements = null;
        try {
            statements = AdHocPlannedStmtBatch.planArrayFromBuffer(buf);
        }
        catch (IOException e) {
            throw new VoltAbortException(e);
        }

        if (statements.length == 0) {
            return new VoltTable[]{};
        }

        int currentCatalogVersion = ctx.getCatalogVersion();

        for (AdHocPlannedStatement statement : statements) {
            if (currentCatalogVersion != statement.core.catalogVersion) {
                String msg = String.format("AdHoc transaction %d wasn't planned " +
                        "against the current catalog version. Statement: %s",
                        ctx.getCurrentTxnId(),
                        new String(statement.sql, VoltDB.UTF8ENCODING));
                throw new VoltAbortException(msg);
            }

            SQLStmt stmt = SQLStmtAdHocHelper.createWithPlan(
                    statement.sql,
                    statement.core.aggregatorFragment,
                    statement.core.collectorFragment,
                    statement.core.isReplicatedTableDML,
                    statement.core.parameterTypes);

            voltQueueSQL(stmt, statement.extractedParamValues.toArray());
        }

        return voltExecuteSQL(true);
    }

}
