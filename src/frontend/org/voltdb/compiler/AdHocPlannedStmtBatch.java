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

package org.voltdb.compiler;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.planner.ParameterInfo;

/**
 * Holds a batch of planned SQL statements.
 *
 * Both AdHocPlannedStmtBatch and AdHocPlannedStatement are derived from
 * AsyncCompilerResult. So there's some data redundancy, e.g. clientData.
 */
public class AdHocPlannedStmtBatch extends AsyncCompilerResult implements Cloneable {
    private static final long serialVersionUID = -8627490621430290801L;

    public final String sqlBatchText;
    // May be reassigned if the planner infers single partition work.
    public Object partitionParam;
    public final int catalogVersion;

    // The planned statements.
    // Do not add statements directly. Use addStatement so that the readOnly flag
    // is updated
    public final List<AdHocPlannedStatement> plannedStatements = new ArrayList<AdHocPlannedStatement>();

    // Assume the batch is read-only until we see the first non-select statement.
    private boolean readOnly = true;

    /**
     * Statement batch constructor.
     *
     * IMPORTANT: sqlBatchText is not maintained or updated by this class when
     * statements are added. The caller is responsible for splitting the batch
     * text and assuring that the individual SQL statements correspond to the
     * original.
     *
     * @param sqlBatchText     Un-split SQL for the entire batch
     * @param partitionParam   Optional partition parameter or null
     * @param catalogVersion   Catalog version number
     * @param clientHandle     Client handle
     * @param connectionId     Connection ID
     * @param hostname         Host name
     * @param adminConnection  True if an admin connection
     * @param clientData       Optional client data object or null
     */
    public AdHocPlannedStmtBatch(
            String sqlBatchText,
            Object partitionParam,
            int catalogVersion,
            long clientHandle,
            long connectionId,
            String hostname,
            boolean adminConnection,
            Object clientData) {
        this.sqlBatchText = sqlBatchText;
        this.partitionParam = partitionParam;
        this.catalogVersion = catalogVersion;
        this.clientHandle = clientHandle;
        this.connectionId = connectionId;
        this.hostname = hostname;
        this.adminConnection = adminConnection;
        this.clientData = clientData;
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "\n  partition param: " + ((partitionParam != null) ? partitionParam.toString() : "null");
        retval += "\n  sql: " + ((sqlBatchText != null) ? sqlBatchText : "null");
        return retval;
    }

    @Override
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve all the SQL statement text as a list of strings.
     *
     * @return list of SQL statement strings
     */
    public List<String> getSQLStatements() {
        List<String> sqlStatements = new ArrayList<String>(plannedStatements.size());
        for (AdHocPlannedStatement plannedStatement : plannedStatements) {
            sqlStatements.add(plannedStatement.sql);
        }
        return sqlStatements;
    }

    /**
     * Retrieve all the aggregator fragments as a list of strings.
     *
     * @return list of SQL aggregator fragment strings
     */
    public List<byte[]> getAggregatorFragments() {
        List<byte[]> fragments = new ArrayList<byte[]>(plannedStatements.size());
        for (AdHocPlannedStatement plannedStatement : plannedStatements) {
            fragments.add(plannedStatement.aggregatorFragment);
        }
        return fragments;
    }

    /**
     * Retrieve all the collector fragments as a list of strings.
     *
     * @return list of SQL collector fragment strings
     */
    public List<byte[]> getCollectorFragments() {
        List<byte[]> fragments = new ArrayList<byte[]>(plannedStatements.size());
        for (AdHocPlannedStatement plannedStatement : plannedStatements) {
            fragments.add(plannedStatement.collectorFragment);
        }
        return fragments;
    }

    /**
     * Retrieve all the replicated table DML flags as a list of integers.
     *
     * @return list of table replication DML flags (1=true, 0=false)
     */
    public List<Integer> getReplicatedTableDMLFlags() {
        List<Integer> flags = new ArrayList<Integer>(plannedStatements.size());
        for (AdHocPlannedStatement plannedStatement : plannedStatements) {
            flags.add(plannedStatement.isReplicatedTableDML ? 1 : 0);
        }
        return flags;
    }

    /**
     * Convenience method to create a new AdHocPlannedStatement. Copies redundant
     * data from AsyncCompilerResult base class to new object because it is also
     * derived from that class.
     *
     * IMPORTANT: This does not update sqlBatchText. The caller is responsible for
     * splitting the batch text and assuring that the individual SQL statements
     * correspond to the original.
     *
     * @param sqlStatement          SQL statement text (must be single statement)
     * @param aggregatorFragment    aggregator fragment
     * @param collectorFragment     collector fragment
     * @param isReplicatedTableDML  replicated table DML flag
     * @param isNonDeterministic    non-deterministic SQL flag
     * @return                      statement object
     */
    public void addStatement(String sqlStatement, byte[] aggregatorFragment,
                             byte[] collectorFragment, boolean isReplicatedTableDML,
                             boolean isNonDeterministic, List<ParameterInfo> params) {
        AdHocPlannedStatement plannedStmt = new AdHocPlannedStatement(
                                                        sqlStatement,
                                                        aggregatorFragment,
                                                        collectorFragment,
                                                        isReplicatedTableDML,
                                                        isNonDeterministic,
                                                        partitionParam,
                                                        catalogVersion);
        // The first non-select statement makes it not read-only.
        if (readOnly && !sqlStatement.trim().toLowerCase().startsWith("select")) {
            readOnly = false;
        }
        plannedStmt.clientHandle = clientHandle;
        plannedStmt.connectionId = connectionId;

        plannedStmt.hostname = hostname;
        plannedStmt.adminConnection = adminConnection;
        plannedStmt.clientData = clientData;
        plannedStmt.params = params;
        plannedStatements.add(plannedStmt);
    }

    /**
     * Detect if batch is compatible with single partition optimizations
     * @return true if nothing is replicated and nothing has a collector.
     */
    public boolean isSinglePartitionCompatible() {
        for (AdHocPlannedStatement plannedStmt : plannedStatements) {
            if (plannedStmt.isReplicatedTableDML || plannedStmt.collectorFragment != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Get the number of planned statements.
     *
     * @return planned statement count
     */
    public int getPlannedStatementCount() {
        return plannedStatements.size();
    }

    /**
     * Get a particular planned statement by index.
     * The index is not validated here.
     *
     * @param index
     * @return planned statement
     */
    public AdHocPlannedStatement getPlannedStatement(int index) {
        return plannedStatements.get(index);
    }

    /**
     * Read-only flag accessor
     *
     * @return true if read-only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

}
