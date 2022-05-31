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

package org.voltdb.iv2;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.concurrent.TimeUnit;

import org.voltcore.messaging.Mailbox;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ExpectedProcedureException;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

abstract public class ProcedureTask extends TransactionTask
{
    final Mailbox m_initiator;
    final String m_procName;
    ProcedureTask(Mailbox initiator, String procName, TransactionState txn,
                  TransactionTaskQueue queue)
    {
        super(txn, queue);
        m_initiator = initiator;
        m_procName = procName;
    }

    /** Run is invoked by a run-loop to execute this transaction. */
    @Override
    abstract public void run(SiteProcedureConnection siteConnection);

    /** procedure tasks must complete their txnstates */
    abstract void completeInitiateTask(SiteProcedureConnection siteConnection);

    /** Mostly copy-paste of old ExecutionSite.processInitiateTask() */
    protected InitiateResponseMessage processInitiateTask(Iv2InitiateTaskMessage taskMessage,
            SiteProcedureConnection siteConnection)
    {
        final InitiateResponseMessage response = new InitiateResponseMessage(taskMessage);

        try {
            Object[] callerParams = null;
            /*
             * Parameters are lazily deserialized. We may not find out until now
             * that the parameter set is corrupt
             */
            try {
                callerParams = taskMessage.getParameters();
            } catch (RuntimeException e) {
                Writer result = new StringWriter();
                PrintWriter pw = new PrintWriter(result);
                e.printStackTrace(pw);
                response.setResults(
                        new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                            new VoltTable[] {},
                                "Exception while deserializing procedure params, procedure="
                                + m_procName + "\n"
                                + result.toString()));
            }
            if (callerParams == null) {
                return response;
            }

            ClientResponseImpl cr = null;
            ProcedureRunner runner = siteConnection.getProcedureRunner(m_procName);
            if (runner == null) {
                String error =
                        "Procedure " + m_procName + " is not present in the catalog. "  +
                                "This can happen if a catalog update removing the procedure occurred " +
                                "after the procedure was submitted " +
                                "but before the procedure was executed.";
                hostLog.rateLimitedWarn(60, error + " %s", "This log message is rate limited to once every 60 seconds.");
                response.setResults(
                        new ClientResponseImpl(
                                ClientResponse.UNEXPECTED_FAILURE,
                                new VoltTable[]{},
                                error));
                return response;
            }

            // Check partitioning of single-partition and n-partition transactions.
            if (runner.checkPartition(m_txnState, siteConnection.getCurrentHashinator())) {
                runner.setupTransaction(m_txnState);

                // execute the procedure
                SystemSettingsType.Procedure procedureSetting = VoltDB.instance().getCatalogContext().getDeployment().getSystemsettings().getProcedure();
                boolean keepImmutable = taskMessage.getStoredProcedureInvocation().getKeepParamsImmutable() &&
                                        ( procedureSetting == null || procedureSetting.isCopyparameters() );
                cr = runner.call(callerParams, (taskMessage.shouldReturnResultTables() || taskMessage.isEveryPartition()),
                                 keepImmutable);

                // pass in the first value in the hashes array if it's not null
                Integer hash = null;
                int[] hashes = cr.getHashes();
                if (hashes != null && hashes.length > 0) {
                    hash = hashes[0];
                }
                m_txnState.setHash(hash);
                response.setResults(cr);
            } else {
                // mis-partitioned invocation, reject it and let the ClientInterface restart it
                response.setMispartitioned(true, taskMessage.getStoredProcedureInvocation(),
                        TheHashinator.getCurrentVersionedConfig());
            }
        }
        catch (final ExpectedProcedureException e) {
            execLog.trace("Procedure threw an expected procedure exception", e);
            response.setResults(
                    new ClientResponseImpl(
                        ClientResponse.GRACEFUL_FAILURE,
                        new VoltTable[]{},
                        e.toString()));
        }
        catch (final Exception e) {
            // Should not be able to reach here. VoltProcedure.call caught all invocation target exceptions
            // and converted them to error responses. Java errors are re-thrown, and not caught by this
            // exception clause. A truly unexpected exception reached this point. Crash. It's a defect.
            hostLog.error("Unexpected exception while executing procedure wrapper", e);
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        return response;
    }
}
