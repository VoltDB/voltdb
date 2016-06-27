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

class UUCVConflictRunner extends ClientConflictRunner {

    UUCVConflictRunner(ClientThread clientThread) {
        super(clientThread);
    }

    @Override
    public void runScenario(final String tableName) throws Throwable {
        final long newRid = clientThread.getAndIncrementRid();
        final String insertProcName = getInsertProcCall(tableName);
        final ClientPayloadProcessor.Pair primaryPayload = processor.generateForStore();
        final ClientPayloadProcessor.Pair secondaryPayload = processor.generateForStore();
        final ClientPayloadProcessor.Pair sharedPayload = processor.generateForStore();

        try {
            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(primaryClient, rid, insertProcName, primaryPayload);
                            waitForApplyBinaryLog(cid, rid, tableName, primaryClient, 1, secondaryClient, 1);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            primary.join();

            CompletableFuture<VoltTable[]> secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            VoltTable[] result = clientThread.callStoreProcedure(secondaryClient, newRid, insertProcName, secondaryPayload);
                            waitForApplyBinaryLog(cid, newRid, tableName, secondaryClient, 1, primaryClient, 1);
                            return result;
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });
            secondary.join();
        } catch (CompletionException ce) {
            throw ce.getCause();
        }

        final String updateProcName = getUpdateProcCall(tableName);
        try {
            CompletableFuture<VoltTable[]> primary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(primaryClient, rid, updateProcName,
                                    sharedPayload, primaryPayload, "UUCVConflictRunner:84");
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });

            CompletableFuture<VoltTable[]> secondary = CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return clientThread.callStoreProcedure(secondaryClient, newRid, updateProcName,
                                    sharedPayload, secondaryPayload, "UUCVConflictRunner:94");
                        } catch (Exception e) {
                            throw new CompletionException(e);
                        }
                    });


            CompletableFuture.allOf(primary, secondary).join();
            VoltTable[] primaryResult = primary.get();
            VoltTable primaryData = primaryResult[1];
            verifyTableData("primary", tableName, rid, primaryData, 1, 0, sharedPayload);

            VoltTable[] secondaryResult = secondary.get();
            VoltTable secondaryData = secondaryResult[1];
            verifyTableData("secondary", tableName, newRid, secondaryData, 1, 0, sharedPayload);

            logXdcrConflict(primaryClient, tableName, primaryData, 0, newRid,
                    ACTION_TYPE.U, CONFLICT_TYPE.CNST, DECISION.R, DIVERGENCE.D);

            logXdcrConflict(secondaryClient, tableName, secondaryData, 0, rid,
                    ACTION_TYPE.U, CONFLICT_TYPE.CNST, DECISION.R, DIVERGENCE.D);
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof ProcCallException) {
                ProcCallException pe = (ProcCallException) cause;
                ClientResponseImpl cri = (ClientResponseImpl) pe.getClientResponse();
                if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE)) { // no xdcr conflict
                    LOG.warn("Received UUCV exceptions for cid " + cid + ", rid " + rid + ", next rid " + newRid);
                    logXdcrNoConflict(primaryClient, tableName, cid, rid, newRid,
                            ACTION_TYPE.U, CONFLICT_TYPE.UUCV, DECISION.A, DIVERGENCE.C);
                    logXdcrNoConflict(secondaryClient, tableName, cid, rid, newRid,
                            ACTION_TYPE.U, CONFLICT_TYPE.UUCV, DECISION.A, DIVERGENCE.C);
                    return;
                }
            }

            throw ce;
        }
    }
}
