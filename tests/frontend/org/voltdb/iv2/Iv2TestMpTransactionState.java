/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.iv2;

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.ParameterSet;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.exceptions.EEException;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class Iv2TestMpTransactionState extends TestCase
{
    static class MpTestPlan
    {
        FragmentTaskMessage remoteWork;
        FragmentTaskMessage localWork;
        List<FragmentResponseMessage> generatedResponses =
            new ArrayList<FragmentResponseMessage>();
        int[] depsToResume;
    }

    ByteBuffer createDummyParameterSet() throws IOException
    {
        ParameterSet blah = new ParameterSet();
        blah.setParameters(new Long(4321), new Long(5678));
        FastSerializer fs = new FastSerializer();
        fs.writeObject(blah);
        ByteBuffer params = fs.getBuffer();
        return params;
    }

    // Currently emulates the code in ProcedureRunner.slowPath()
    // So any change to how that stuff is build will need to
    // be reflected here
    MpTestPlan createTestPlan(int batchSize, boolean readOnly,
                              boolean replicatedTable, boolean rollback,
                              long[] remoteHSIds) throws IOException
    {
        boolean single_frag = readOnly && replicatedTable;
        MpTestPlan plan = new MpTestPlan();

        List<Integer> distributedOutputDepIds = new ArrayList<Integer>();
        List<Integer> depsToResumeList = new ArrayList<Integer>();
        List<Integer> depsForLocalTask = new ArrayList<Integer>();
        for (int i = 0; i < batchSize; i++)
        {
            // each SQL statement in the batch gets an output dep ID
            // which corresponds to a local fragment ID
            depsToResumeList.add(i);
            // each local fragment ID needs an input dep.  If this is
            // not replicated read only, generate a new value and add it to
            // the distributed output deps
            if (!single_frag) {
                // take the dep and add 1000
                depsForLocalTask.add(i + 1000);
                distributedOutputDepIds.add(i + 1000);
            } else {
                depsForLocalTask.add(-1);
            }
        }

        // Convert collections to arrays for message
        long[] distributedFragIdArray = new long[distributedOutputDepIds.size()];
        int[] distributedOutputDepIdArray = new int[distributedOutputDepIds.size()];
        ByteBuffer[] distributedParamsArray = new ByteBuffer[distributedOutputDepIds.size()];
        for (int i = 0; i < distributedOutputDepIds.size(); i++) {
            distributedFragIdArray[i] = Long.MIN_VALUE; // Don't care?
            distributedOutputDepIdArray[i] = distributedOutputDepIds.get(i);
            distributedParamsArray[i] = createDummyParameterSet();
        }

        // generate remote task with output IDs, fill in lists appropriately
        plan.remoteWork = new FragmentTaskMessage(Long.MIN_VALUE, // try not to care?
                                                  Long.MIN_VALUE, // try not to care
                                                  1234l, // magic, change if it matters
                                                  readOnly,
                                                  distributedFragIdArray,
                                                  distributedOutputDepIdArray,
                                                  distributedParamsArray,
                                                  false);  // IV2 doesn't use final task (yet)
        System.out.println("REMOTE TASK: " + plan.remoteWork.toString());

        if (!single_frag) {
            // generate a remote fragment response for each remote message
            for (int i = 0; i < remoteHSIds.length; i++) {
                FragmentResponseMessage resp =
                    new FragmentResponseMessage(plan.remoteWork, remoteHSIds[i]);
                if (rollback && i == (remoteHSIds.length - 1)) {
                    resp.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR,
                                   new EEException(1234));
                }
                else {
                    resp.setStatus(FragmentResponseMessage.SUCCESS, null);
                    for (int j = 0; j < distributedOutputDepIdArray.length; j++) {
                        resp.addDependency(distributedOutputDepIdArray[j],
                                           new VoltTable(new VoltTable.ColumnInfo("BOGO",
                                                                                  VoltType.BIGINT)));
                    }
                }
                System.out.println("RESPONSE: " + resp);
                plan.generatedResponses.add(resp);
            }
        }

        ByteBuffer[] localParams = new ByteBuffer[batchSize];
        for (int i = 0; i < batchSize; i++) {
            localParams[i] = createDummyParameterSet();
        }
        int[] depsToResume = new int[depsToResumeList.size()];
        for (int i = 0; i < depsToResumeList.size(); i++) {
            depsToResume[i] = depsToResumeList.get(i);
        }
        plan.depsToResume = depsToResume;
        long[] localFragIds = new long[batchSize];
        // generate local task with new output IDs, use above outputs as inputs,
        // if any
        plan.localWork = new FragmentTaskMessage(Long.MIN_VALUE, // try not to care
                                                 Long.MIN_VALUE,
                                                 1234l,
                                                 readOnly,
                                                 localFragIds,
                                                 depsToResume,
                                                 localParams,
                                                 false);
       for (int i = 0; i < depsForLocalTask.size(); i++) {
           if (depsForLocalTask.get(i) < 0) continue;
           plan.localWork.addInputDepId(i, depsForLocalTask.get(i));
       }
       // create the FragmentResponse for the BorrowTask
       FragmentResponseMessage resp =
           new FragmentResponseMessage(plan.remoteWork, remoteHSIds[0]);
       resp.setStatus(FragmentResponseMessage.SUCCESS, null);
       for (int j = 0; j < depsToResume.length; j++) {
           resp.addDependency(depsToResume[j],
                              new VoltTable(new VoltTable.ColumnInfo("BOGO",
                                                                     VoltType.BIGINT)));
       }
       System.out.println("BORROW RESPONSE: " + resp);
       plan.generatedResponses.add(resp);

       System.out.println("LOCAL TASK: " + plan.localWork.toString());

       return plan;
    }

    List<Long> allHsids;
    long buddyHSId;

    @Override
    protected void setUp()
    {
        allHsids = new ArrayList<Long>();
    }

    private long[] configureHSIds(int count)
    {
        long[] non_local = new long[count];
        int index = 0;
        for (long i = 0; i < count; i++)
        {
            allHsids.add(i);
            non_local[index] = i;
            ++index;
        }
        System.out.println(allHsids);
        return non_local;
    }

    @Test
    public void testOneSitePartitionedRead() throws IOException, MessagingException
    {
        long txnId = 1234l;
        int batch_size = 3;
        Iv2InitiateTaskMessage taskmsg =
            new Iv2InitiateTaskMessage(0, -1, txnId, true, false, null, 0);
        int hsids = 1;
        buddyHSId = 0;
        long[] non_local = configureHSIds(hsids);

        MpTestPlan plan = createTestPlan(batch_size, true, false, false, non_local);

        Mailbox mailbox = mock(Mailbox.class);
        SiteProcedureConnection siteConnection = mock(SiteProcedureConnection.class);

        MpTransactionState dut =
            new MpTransactionState(mailbox, txnId, taskmsg, allHsids, buddyHSId);

        // emulate ProcedureRunner's use for a single local fragment
        dut.setupProcedureResume(true, plan.depsToResume);
        dut.createLocalFragmentWork(plan.localWork, false);

        // This will be passed a FragmentTaskMessage with no deps
        dut.createAllParticipatingFragmentWork(plan.remoteWork);
        // we should send one message
        verify(mailbox).send(eq(new long[] {0}), (VoltMessage)any());

        // to simplify, offer messages first
        // offer all the necessary fragment responses to satisfy deps
        for (FragmentResponseMessage msg : plan.generatedResponses) {
            System.out.println("Offering response: " + msg);
            dut.offerReceivedFragmentResponse(msg);
        }

        // if we've satisfied everything, this should run to completion
        Map<Integer, List<VoltTable>> results = dut.recursableRun(siteConnection);
        // Verify we send a BorrowTask
        verify(mailbox).send(eq(buddyHSId), (BorrowTaskMessage)any());

        // verify returned deps/tables
        assertEquals(batch_size, results.size());
        System.out.println(results);
    }

    @Test
    public void testMultiSitePartitionedRead() throws IOException, MessagingException
    {
        long txnId = 1234l;
        int batch_size = 3;
        Iv2InitiateTaskMessage taskmsg =
            new Iv2InitiateTaskMessage(0, -1, txnId, true, false, null, 0);
        int hsids = 6;
        buddyHSId = 0;
        long[] non_local = configureHSIds(hsids);

        MpTestPlan plan = createTestPlan(batch_size, true, false, false, non_local);

        Mailbox mailbox = mock(Mailbox.class);
        SiteProcedureConnection siteConnection = mock(SiteProcedureConnection.class);

        MpTransactionState dut =
            new MpTransactionState(mailbox, txnId, taskmsg, allHsids, buddyHSId);

        // emulate ProcedureRunner's use for a single local fragment
        dut.setupProcedureResume(true, plan.depsToResume);
        dut.createLocalFragmentWork(plan.localWork, false);

        // This will be passed a FragmentTaskMessage with no deps
        dut.createAllParticipatingFragmentWork(plan.remoteWork);
        // we should send 6 messages
        verify(mailbox).send(eq(new long[] {0,1,2,3,4,5}), (VoltMessage)any());

        // to simplify, offer messages first
        // offer all the necessary fragment responses to satisfy deps
        for (FragmentResponseMessage msg : plan.generatedResponses) {
            dut.offerReceivedFragmentResponse(msg);
        }

        // if we've satisfied everything, this should run to completion
        Map<Integer, List<VoltTable>> results = dut.recursableRun(siteConnection);
        verify(mailbox).send(eq(buddyHSId), (BorrowTaskMessage)any());
        // verify returned deps/tables
        assertEquals(batch_size, results.size());
        System.out.println(results);
    }

    @Test
    public void testSingleReplicatedReadFragment() throws IOException, MessagingException
    {
        long txnId = 1234l;
        int batch_size = 3;
        Iv2InitiateTaskMessage taskmsg =
            new Iv2InitiateTaskMessage(3, 4, txnId, true, false, null, 0);
        int hsids = 6;
        buddyHSId = 3;
        long[] non_local = configureHSIds(hsids);

        MpTestPlan plan = createTestPlan(batch_size, true, true, false, non_local);

        Mailbox mailbox = mock(Mailbox.class);
        SiteProcedureConnection siteConnection = mock(SiteProcedureConnection.class);

        MpTransactionState dut =
            new MpTransactionState(mailbox, txnId, taskmsg, allHsids, buddyHSId);

        // emulate ProcedureRunner's use for a single local fragment
        dut.setupProcedureResume(true, plan.depsToResume);
        dut.createLocalFragmentWork(plan.localWork, false);

        // This will be passed a FragmentTaskMessage with no deps
        dut.createAllParticipatingFragmentWork(plan.remoteWork);
        // verify no messages sent to non-3 HSIDs for read-only
        verify(mailbox, never()).send(anyLong(), (VoltMessage)any());
        verify(mailbox, never()).send(new long[] {anyLong()}, (VoltMessage)any());

        // to simplify, offer messages first
        // offer all the necessary fragment responses to satisfy deps
        for (FragmentResponseMessage msg : plan.generatedResponses) {
            dut.offerReceivedFragmentResponse(msg);
        }

        // if we've satisfied everything, this should run to completion
        Map<Integer, List<VoltTable>> results = dut.recursableRun(siteConnection);
        verify(mailbox).send(eq(buddyHSId), (BorrowTaskMessage)any());

        // verify returned deps/tables
        assertEquals(batch_size, results.size());
        System.out.println(results);
    }

    @Test
    public void testOneSitePartitionedReadWithRollback() throws IOException, MessagingException
    {
        long txnId = 1234l;
        int batch_size = 3;
        Iv2InitiateTaskMessage taskmsg =
            new Iv2InitiateTaskMessage(0, 0, txnId, true, false, null, 0);
        int hsids = 1;
        buddyHSId = 0;
        long[] non_local = configureHSIds(hsids);

        MpTestPlan plan = createTestPlan(batch_size, true, false, true, non_local);

        Mailbox mailbox = mock(Mailbox.class);
        SiteProcedureConnection siteConnection = mock(SiteProcedureConnection.class);

        MpTransactionState dut =
            new MpTransactionState(mailbox, txnId, taskmsg, allHsids, buddyHSId);

        // emulate ProcedureRunner's use for a single local fragment
        dut.setupProcedureResume(true, plan.depsToResume);
        dut.createLocalFragmentWork(plan.localWork, false);

        // This will be passed a FragmentTaskMessage with no deps
        dut.createAllParticipatingFragmentWork(plan.remoteWork);
        // we should send one message
        verify(mailbox).send(eq(new long[] {0}), (VoltMessage)any());

        // to simplify, offer messages first
        // offer all the necessary fragment responses to satisfy deps
        for (FragmentResponseMessage msg : plan.generatedResponses) {
            System.out.println("Offering response: " + msg);
            dut.offerReceivedFragmentResponse(msg);
        }

        // We're getting an error, so this should throw something
        boolean threw = false;
        try {
            Map<Integer, List<VoltTable>> results = dut.recursableRun(siteConnection);
            fail();
        }
        catch (EEException eee) {
            if (eee.getErrorCode() == 1234) {
                threw = true;
            }
        }
        assertTrue(threw);
    }

    @Test
    public void testOneSitePartitionedReadWithBuddyRollback() throws IOException, MessagingException
    {
        long txnId = 1234l;
        int batch_size = 3;
        Iv2InitiateTaskMessage taskmsg =
            new Iv2InitiateTaskMessage(0, 0, txnId, true, false, null, 0);
        int hsids = 1;
        buddyHSId = 0;
        long[] non_local = configureHSIds(hsids);

        MpTestPlan plan = createTestPlan(batch_size, true, false, false, non_local);

        Mailbox mailbox = mock(Mailbox.class);
        SiteProcedureConnection siteConnection = mock(SiteProcedureConnection.class);

        MpTransactionState dut =
            new MpTransactionState(mailbox, txnId, taskmsg, allHsids, buddyHSId);

        // emulate ProcedureRunner's use for a single local fragment
        dut.setupProcedureResume(true, plan.depsToResume);
        dut.createLocalFragmentWork(plan.localWork, false);

        // This will be passed a FragmentTaskMessage with no deps
        dut.createAllParticipatingFragmentWork(plan.remoteWork);
        // we should send one message
        verify(mailbox).send(eq(new long[] {0}), (VoltMessage)any());

        // to simplify, offer messages first
        // offer all the necessary fragment responses to satisfy deps
        // just be lazy and perturb the buddy response here
        plan.generatedResponses.get(plan.generatedResponses.size() - 1).
             setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new EEException(1234));
        for (FragmentResponseMessage msg : plan.generatedResponses) {
            System.out.println("Offering response: " + msg);
            dut.offerReceivedFragmentResponse(msg);
        }

        // We're getting an error, so this should throw something
        boolean threw = false;
        try {
            Map<Integer, List<VoltTable>> results = dut.recursableRun(siteConnection);
            fail();
        }
        catch (EEException eee) {
            if (eee.getErrorCode() == 1234) {
                threw = true;
            }
        }
        assertTrue(threw);
    }
}
