package org.voltdb.compiler;

import java.util.List;

import org.voltdb.CatalogContext;

public class ExplainPlannerWork extends AdHocPlannerWork {

    /**
     * 
     */
    private static final long serialVersionUID = -9018666847280960264L;

    public ExplainPlannerWork( long replySiteId, boolean shouldShutdown, long clientHandle,
            long connectionId, String hostname, boolean adminConnection, Object clientData,
            String sqlBatchText, List<String> sqlStatements, Object partitionParam, CatalogContext context,
            boolean allowParameterization, final boolean inferSinglePartition,
            AsyncCompilerWorkCompletionHandler completionHandler) {
        super(replySiteId, shouldShutdown, clientHandle, connectionId, hostname, adminConnection, clientData,
                sqlBatchText, sqlStatements, partitionParam, context, allowParameterization, inferSinglePartition,
                completionHandler);
    }
}
