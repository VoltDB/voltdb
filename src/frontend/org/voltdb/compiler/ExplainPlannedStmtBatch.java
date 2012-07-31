package org.voltdb.compiler;

public class ExplainPlannedStmtBatch extends AdHocPlannedStmtBatch {
    /**
     * 
     */
    private static final long serialVersionUID = -8367826638394346984L;

    public ExplainPlannedStmtBatch (String sqlBatchText,
            Object partitionParam,
            int catalogVersion,
            long clientHandle,
            long connectionId,
            String hostname,
            boolean adminConnection,
            Object clientData) {
        super(sqlBatchText, partitionParam, catalogVersion, clientHandle, connectionId, hostname, adminConnection, clientData);
    }
}
