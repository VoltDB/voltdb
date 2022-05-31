/* This file is part of VoltDB.
 * Copyright (C) 2020-2022 Volt Active Data Inc.
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

import static org.voltdb.iv2.TransactionTask.execLog;
import static org.voltdb.iv2.TransactionTask.hostLog;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.voltcore.messaging.Mailbox;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExpectedProcedureException;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.base.Supplier;

/**
 * Really just a container for batch procedure tasks. Since both SP and MP tasks need to have the same re-implementation
 * of {@link ProcedureTask#processInitiateTask(Iv2InitiateTaskMessage, SiteProcedureConnection)} this class provides a
 * container for a common shared method that both implementations can use.
 */
abstract class BatchProcedureTask {
    private BatchProcedureTask() {}

    static class SpBatch extends SpProcedureTask {
        SpBatch(Mailbox initiator, String procName, TransactionTaskQueue queue, Iv2InitiateTaskMessage msg) {
            super(initiator, procName, queue, msg, "runBatchSpTask");
        }

        @Override
        protected InitiateResponseMessage processInitiateTask(Iv2InitiateTaskMessage task,
                SiteProcedureConnection siteConnection) {
            return processBatchTask(m_procName, m_txnState, task, siteConnection);
        }
    }

    static class MpBatch extends MpProcedureTask {
        MpBatch(Mailbox mailbox, String procName, TransactionTaskQueue queue, Iv2InitiateTaskMessage msg,
                List<Long> pInitiators, Map<Integer, Long> partitionMasters, Supplier<Long> buddyHSId, boolean isRestart,
                int leaderId, boolean nPartTxn) {
            super(mailbox, procName, queue, msg, pInitiators, partitionMasters, buddyHSId, isRestart, leaderId,
                    nPartTxn);
        }

        @Override
        protected InitiateResponseMessage processInitiateTask(Iv2InitiateTaskMessage task,
                SiteProcedureConnection siteConnection) {
            return processBatchTask(m_procName, m_txnState, task, siteConnection);
        }
    }

    /**
     * Based upon the implementation of
     * {@link ProcedureTask#processInitiateTask(Iv2InitiateTaskMessage, SiteProcedureConnection)} except a single
     * procedure is called multiple times with each set of parameters retrieved from a single VoltTable parameter
     */
    static InitiateResponseMessage processBatchTask(String procName, TransactionState txnState,
            Iv2InitiateTaskMessage task, SiteProcedureConnection siteConnection) {
        final InitiateResponseMessage response = new InitiateResponseMessage(task);

        try {
            Object[] callerParams = null;
            /*
             * Parameters are lazily deserialized. We may not find out until now that the parameter set is corrupt
             */
            try {
                callerParams = task.getParameters();
            } catch (RuntimeException e) {
                Writer result = new StringWriter();
                PrintWriter pw = new PrintWriter(result);
                e.printStackTrace(pw);
                response.setResults(new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, new VoltTable[] {},
                        "Exception while deserializing procedure params, procedure=" + procName + "\n"
                                + result.toString()));
            }

            ClientResponseImpl cr = null;
            ProcedureRunner runner = siteConnection.getProcedureRunner(procName);
            if (runner == null) {
                String error = "Procedure " + procName + " is not present in the catalog. "
                        + "This can happen if a catalog update removing the procedure occurred "
                        + "after the procedure was submitted " + "but before the procedure was executed.";
                hostLog.rateLimitedWarn(60, error + " %s", "This log message is rate limited to once every 60 seconds.");
                response.setResults(
                        new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[] {}, error));
                return response;
            }

            if (callerParams.length != 1) {
                response.setResults(new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE, new VoltTable[0],
                        "Batch procedure calls should only have one paramater of type VoltTable: "
                                + Arrays.toString(callerParams)));
                return response;
            }

            VoltTable paramsTable;
            if (callerParams[0] instanceof VoltTable) {
                paramsTable = (VoltTable) callerParams[0];
            } else if (callerParams[0] instanceof byte[]) {
                paramsTable = PrivateVoltTableFactory
                        .createVoltTableFromBuffer(ByteBuffer.wrap((byte[]) callerParams[0]), true);
            } else {
                response.setResults(new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, new VoltTable[0],
                        "Unsupported parameter type only VoltTable and VARBINARY are supported: "
                                + callerParams[0] == null ? "NULL" : callerParams.getClass().getSimpleName()));
                return response;
            }

            TheHashinator hashinator = siteConnection.getCurrentHashinator();

            /**
             * Total of all results returned by procedure calls if positive<br>
             * -1: Use procedures invoked count<br>
             * -2: Use procedures invoked count because the procedure returned and incompatible result
             */
            long totalResult = 0;
            boolean clearHash = true;
            while (paramsTable.advanceRow()) {
                // Check partitioning of single-partition and n-partition transactions.
                if (runner.checkPartition(txnState, hashinator, paramsTable::get)) {

                    // execute the procedure
                    runner.setupTransaction(txnState);
                    cr = runner.call(clearHash, paramsTable.getRowObjects());
                    if (cr.getStatus() != ClientResponse.SUCCESS) {
                        break;
                    }
                    if (totalResult >= 0) {
                        VoltTable[] results = cr.getResults();
                        if (results.length == 0) {
                            totalResult = -1;
                        } else {
                            try {
                                for (VoltTable resultTable : results) {
                                    long result = resultTable.asScalarLong();
                                    if (resultTable.wasNull()) {
                                        totalResult = -2;
                                    } else {
                                        totalResult += result;
                                    }
                                }
                            } catch (IllegalStateException e) {
                                totalResult = -2;
                            }

                            if (totalResult == -2) {
                                hostLog.rateLimitedWarn(60, "During batch processing, procedure %s is returning results that are ignored. "
                                                + "Batch procedures should either be void or return tables which can be interpreted as a scalar long. "
                                                + "See org.voltdb.VoltTable.asScalarLong()",
                                        procName);
                            }
                        }
                    }
                } else {
                    response.setResults(
                            new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, ClientResponse.TXN_MISPARTITIONED,
                                    "Row should not be processed by this partition: "
                                            + Arrays.toString(paramsTable.getRowObjects()),
                                    new VoltTable[0],
                                    "Row in batch procedure call of " + procName + " does not match partition", -1));
                    return response;
                }
                clearHash = false;
            }

            // pass in the first value in the hashes array if it's not null
            Integer hash = null;
            int[] hashes = cr.getHashes();
            if (hashes != null && hashes.length > 0) {
                hash = hashes[0];
            }

            txnState.setHash(hash);

            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("count", VoltType.BIGINT));
            result.addRow(totalResult < 0 ? paramsTable.getRowCount() : totalResult);
            cr.setResultTables(new VoltTable[] { result });

            response.setResults(cr);

        } catch (final ExpectedProcedureException e) {
            execLog.trace("Procedure threw an expected procedure exception", e);
            response.setResults(
                    new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE, new VoltTable[] {}, e.toString()));
        } catch (final Exception e) {
            // Should not be able to reach here. VoltProcedure.call caught all invocation target exceptions
            // and converted them to error responses. Java errors are re-thrown, and not caught by this
            // exception clause. A truly unexpected exception reached this point. Crash. It's a defect.
            hostLog.error("Unexpected exception while executing procedure wrapper", e);
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        return response;
    }
}
