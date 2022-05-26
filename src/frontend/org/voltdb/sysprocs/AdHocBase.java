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

package org.voltdb.sysprocs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.DependencyPair;
import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SQLStmtAdHocHelper;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.common.Constants;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.planner.ActivePlanRepository;
import com.google_voltpatches.common.base.Charsets;

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
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

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
     * Get a string containing the SQL statements and any parameters for a given
     * batch passed to an ad-hoc query. Used for debugging and logging.
     */
    public static String adHocSQLFromInvocationForDebug(StoredProcedureInvocation invocation) {
        assert(invocation.getProcName().startsWith("@AdHoc"));
        ParameterSet params = invocation.getParams();
        // the final param is the byte array we need
        byte[] serializedBatchData = (byte[]) params.getParam(params.size() - 1);

        Pair<Object[], AdHocPlannedStatement[]> data = decodeSerializedBatchData(serializedBatchData);
        Object[] userparams = data.getFirst();
        AdHocPlannedStatement[] statements = data.getSecond();

        StringBuilder sb = new StringBuilder();
        if (statements.length == 0) {
            sb.append("ADHOC INVOCATION HAS NO SQL");
        }
        else if (statements.length == 1) {
            sb.append(adHocSQLStringFromPlannedStatement(statements[0], userparams));
        }
        else { // > 1
            sb.append("BEGIN ADHOC_SQL_BATCH {\n");
            for (AdHocPlannedStatement stmt : statements) {
                sb.append(adHocSQLStringFromPlannedStatement(stmt, userparams)).append("\n");
            }
            sb.append("} END ADHOC_SQL_BATCH");
        }

        return sb.toString();
    }

    /**
     * Get a string containing a SQL statement and any parameters for a given
     * AdHocPlannedStatement. Used for debugging and logging.
     */
    public static String adHocSQLStringFromPlannedStatement(AdHocPlannedStatement statement, Object[] userparams) {
        final int MAX_PARAM_LINE_CHARS = 120;

        StringBuilder sb = new StringBuilder();
        String sql = new String(statement.sql, Charsets.UTF_8);
        sb.append(sql);

        Object[] params = paramsForStatement(statement, userparams);
        // convert params to strings of a certain max length
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];

            String paramLineStr = String.format("    Param %d: %s", i, param.toString());
            // trim param line if it's silly long
            if (paramLineStr.length() > MAX_PARAM_LINE_CHARS) {
                paramLineStr = paramLineStr.substring(0, MAX_PARAM_LINE_CHARS - 3);
                paramLineStr += "...";
            }

            sb.append('\n').append(paramLineStr);
        }

        return sb.toString();
    }

    /**
     * Decode binary data into structures needed to process adhoc queries.
     * This code was pulled out of runAdHoc so it could be shared there and with
     * adHocSQLStringFromPlannedStatement.
     */
    public static Pair<Object[], AdHocPlannedStatement[]> decodeSerializedBatchData(byte[] serializedBatchData) {
        // Collections must be the same size since they all contain slices of the same data.
        assert(serializedBatchData != null);

        ByteBuffer buf = ByteBuffer.wrap(serializedBatchData);
        AdHocPlannedStatement[] statements = null;
        Object[] userparams = null;
        try {
            userparams = AdHocPlannedStmtBatch.userParamsFromBuffer(buf);
            statements = AdHocPlannedStmtBatch.planArrayFromBuffer(buf);
        }
        catch (IOException e) {
            throw new VoltAbortException(e);
        }
        return new Pair<Object[], AdHocPlannedStatement[]>(userparams, statements);
    }

    /**
     * Get the params for a specific SQL statement within a batch.
     * Note that there is usually a batch size of one.
     */
    static Object[] paramsForStatement(AdHocPlannedStatement statement, Object[] userparams) {
        // When there are no user-provided parameters, statements may have parameterized constants.
        if (userparams.length > 0) {
            return userparams;
        } else {
            return statement.extractedParamArray();
        }
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

        Pair<Object[], AdHocPlannedStatement[]> data = decodeSerializedBatchData(serializedBatchData);
        Object[] userparams = data.getFirst();
        AdHocPlannedStatement[] statements = data.getSecond();

        if (statements.length == 0) {
            return new VoltTable[]{};
        }

        for (AdHocPlannedStatement statement : statements) {
            if (!statement.core.wasPlannedAgainstHash(ctx.getCatalogHash())) {
                @SuppressWarnings("deprecation")
                String msg = String.format("AdHoc transaction %d wasn't planned " +
                        "against the current catalog version. Statement: %s",
                        DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this),
                        new String(statement.sql, Constants.UTF8ENCODING));
                throw new VoltAbortException(msg);
            }

            // Don't cache the statement text, since ad hoc statements
            // that differ only by constants reuse the same plan, statement text may change.
            long aggFragId = ActivePlanRepository.loadOrAddRefPlanFragment(
                    statement.core.aggregatorHash, statement.core.aggregatorFragment, null);
            long collectorFragId = 0;
            if (statement.core.collectorFragment != null) {
                collectorFragId = ActivePlanRepository.loadOrAddRefPlanFragment(
                        statement.core.collectorHash, statement.core.collectorFragment, null);
            }
            SQLStmt stmt = SQLStmtAdHocHelper.createWithPlan(
                    statement.sql,
                    aggFragId,
                    statement.core.aggregatorHash,
                    true,
                    collectorFragId,
                    statement.core.collectorHash,
                    true,
                    statement.core.isReplicatedTableDML,
                    statement.core.readOnly,
                    statement.core.parameterTypes,
                    m_site);

            Object[] params = paramsForStatement(statement, userparams);
            voltQueueSQL(stmt, params);
        }

        return voltExecuteSQL(true);
    }
}
