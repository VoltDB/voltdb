/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package xdcrSelfCheck.scenarios;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import xdcrSelfCheck.ClientPayloadProcessor;
import xdcrSelfCheck.ClientThread;
import xdcrSelfCheck.resolves.XdcrConflict.ACTION_TYPE;
import xdcrSelfCheck.resolves.XdcrConflict.CONFLICT_TYPE;
import xdcrSelfCheck.resolves.XdcrConflict.DECISION;
import xdcrSelfCheck.resolves.XdcrConflict.DIVERGENCE;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

class DDMRConflictRunner extends ClientConflictRunner {

    DDMRConflictRunner(ClientThread clientThread) {
        super(clientThread);
    }

    @Override
    public void runScenario(final String tableName) throws Throwable {
        final String insertProcName = getInsertProcCall(tableName);
        final String deleteProcName = getDeleteProcCall(tableName);
        final ClientPayloadProcessor.Pair seedPayload = processor.generateForStore();

        VoltTable referenceData;
        try {
            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(primaryClient, rid, insertProcName, seedPayload);
                            waitForApplyBinaryLog(cid, rid, tableName, primaryClient, 1, secondaryClient, 1);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            primary.join();
            VoltTable[] primaryResult = primary.get();
            referenceData = primaryResult[2];
        } catch (CompletionException ce) {
            throw ce.getCause();
        }

        try {
            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(primaryClient, rid, deleteProcName,
                                    null, seedPayload, "DDMRConflictRunner:74");
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            CompletableFuture<VoltTable[]> secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(secondaryClient, rid, deleteProcName,
                                    null, seedPayload, "DDMRConflictRunner:85");
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            CompletableFuture.allOf(primary, secondary).join();

            VoltTable[] primaryResult = primary.get();
            VoltTable primaryData = primaryResult[1];
            if (primaryData.getRowCount() != 0) {
                throw new VoltAbortException(
                        "The primary partitioned table has more rows than expected: actual " + primaryData.getRowCount() + ", expected 0");
            }

            VoltTable[] secondaryResult = secondary.get();
            VoltTable secondaryData = secondaryResult[1];
            if (secondaryData.getRowCount() != 0) {
                throw new VoltAbortException(
                        "The secondary partitioned table has more rows than expected: actual " + secondaryData.getRowCount() + ", expected 0");
            }

            logXdcrConflict(primaryClient, tableName, referenceData, 0, rid,
                    ACTION_TYPE.D, CONFLICT_TYPE.MISS, DECISION.R, DIVERGENCE.C);

            logXdcrConflict(secondaryClient, tableName, referenceData, 0, rid,
                    ACTION_TYPE.D, CONFLICT_TYPE.MISS, DECISION.R, DIVERGENCE.C);

        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof ProcCallException) {
                ProcCallException pe = (ProcCallException) cause;
                ClientResponseImpl cri = (ClientResponseImpl) pe.getClientResponse();
                if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE)) { // no xdcr conflict
                    LOG.warn("Received DDMR exceptions for cid " + cid + ", rid " + rid);
                    logXdcrNoConflict(primaryClient, tableName, cid, rid, rid,
                            ACTION_TYPE.D, CONFLICT_TYPE.DDMR, DECISION.A, DIVERGENCE.C);
                    logXdcrNoConflict(secondaryClient, tableName, cid, rid, rid,
                            ACTION_TYPE.D, CONFLICT_TYPE.DDMR, DECISION.A, DIVERGENCE.C);
                    return;
                }
            }
        }
    }
}
