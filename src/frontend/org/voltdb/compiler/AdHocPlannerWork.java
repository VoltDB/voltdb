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

package org.voltdb.compiler;

import org.voltcore.network.Connection;
import org.voltdb.AuthSystem;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.client.ProcedureInvocationType;


public class AdHocPlannerWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -6567283432846270119L;

    final String sqlBatchText;
    final String[] sqlStatements;
    final Object[] userParamSet;
    final CatalogContext catalogContext;
    final boolean inferPartitioning;
    // The user partition key is usually null
    // -- otherwise, it contains one element to support @AdHocSpForTest and
    // ad hoc statements queued within single-partition stored procs.
    final Object[] userPartitionKey;
    public final ExplainMode explainMode;

    public AdHocPlannerWork(long replySiteId, long clientHandle, long connectionId,
            boolean adminConnection, Connection clientConnection,
            String sqlBatchText, String[] sqlStatements,
            Object[] userParamSet, CatalogContext context, ExplainMode explainMode,
            boolean inferPartitioning, Object[] userPartitionKey,
            String invocationName, ProcedureInvocationType type,
            long originalTxnId, long originalUniqueId,
            boolean onReplica, boolean useAdhocDDL,
            AsyncCompilerWorkCompletionHandler completionHandler, AuthSystem.AuthUser user)
    {
        super(replySiteId, false, clientHandle, connectionId,
              clientConnection == null ? "" : clientConnection.getHostnameAndIPAndPort(),
              adminConnection, clientConnection, invocationName, type,
              originalTxnId, originalUniqueId, onReplica, useAdhocDDL,
              completionHandler, user);
        this.sqlBatchText = sqlBatchText;
        this.sqlStatements = sqlStatements;
        this.userParamSet = userParamSet;
        this.catalogContext = context;
        this.explainMode = explainMode;
        this.inferPartitioning = inferPartitioning;
        this.userPartitionKey = userPartitionKey;
    }

    /**
     * A mutated clone method, allowing override of completionHandler and
     * clearing of (obsolete) catalogContext
     */
    public static AdHocPlannerWork rework(AdHocPlannerWork orig,
            AsyncCompilerWorkCompletionHandler completionHandler) {
        return new AdHocPlannerWork(orig.replySiteId,
                orig.clientHandle,
                orig.connectionId,
                orig.adminConnection,
                (Connection) orig.clientData,
                orig.sqlBatchText,
                orig.sqlStatements,
                orig.userParamSet,
                null /* context */,
                orig.explainMode,
                orig.inferPartitioning,
                orig.userPartitionKey,
                orig.invocationName,
                orig.invocationType,
                orig.originalTxnId,
                orig.originalUniqueId,
                orig.onReplica,
                orig.useAdhocDDL,
                completionHandler,
                orig.user);
        }

    /**
     * Special factory of a mostly mocked up instance for calling from inside a stored proc.
     * It's also convenient for simple tests that need to mock up a quick planner request to
     * test related parts of the system.
     */
    public static AdHocPlannerWork makeStoredProcAdHocPlannerWork(long replySiteId,
            String sql, Object[] userParams, boolean singlePartition, CatalogContext context,
            AsyncCompilerWorkCompletionHandler completionHandler)
    {
        return new AdHocPlannerWork(replySiteId, 0, 0, false, null,
            sql, new String[] { sql },
            userParams, context, ExplainMode.NONE,
            // ??? The settings passed here for the single partition stored proc caller
            // denote that the partitioning has already been done so something like the planner
            // code path for @AdHocSpForTest is called for.
            // The plan is required to be single-partition regardless of its internal logic
            // -- EXCEPT that writes to replicated tables are strictly forbdden -- and there
            // should be no correlation inferred or assumed between the partitioning and the
            // statement's constants or parameters.
            false, (singlePartition ? new Object[1] /*any vector element will do, even null*/ : null),
            "@AdHoc_RW_MP", ProcedureInvocationType.ORIGINAL, 0, 0,
            false, false, // don't allow adhoc DDL in this path
            completionHandler, new AuthSystem.AuthDisabledUser());
    }

    @Override
    public String toString() {
        String retval = super.toString();
        if (userParamSet == null || (userParamSet.length == 0)) {
            retval += "\n  user params: empty";
        } else {
            int i = 0;
            for (Object param : userParamSet) {
                i++;
                retval += String.format("\n  user param[%d]: %s",
                                        i, (param == null ? "null" : param.toString()));
            }
        }
        if (userPartitionKey == null) {
            retval += "\n  user partitioning: none";
        } else {
            retval += "\n  user partitioning: " +
                      (userPartitionKey[0] == null ? "null" : userPartitionKey[0].toString());
        }
        assert(sqlStatements != null);
        if (sqlStatements.length == 0) {
            retval += "\n  sql: empty";
        } else {
            int i = 0;
            for (String sql : sqlStatements) {
                i++;
                retval += String.format("\n  sql[%d]: %s", i, sql);
            }
        }
        return retval;
    }

    public int getStatementCount()
    {
        return (this.sqlStatements != null ? this.sqlStatements.length : 0);
    }

    public int getParameterCount()
    {
        return (this.userParamSet != null ? this.userParamSet.length : 0);
    }

}
