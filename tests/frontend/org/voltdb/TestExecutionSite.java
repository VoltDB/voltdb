/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.WriterAppender;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.debugstate.ExecutorContext.ExecutorTxnState;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.RestrictedPriorityQueue;
import org.voltdb.dtxn.SinglePartitionTxnState;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.executionsitefuzz.ExecutionSiteFuzzChecker;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.messaging.FailureSiteUpdateMessage;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.VoltLoggerFactory;

public class TestExecutionSite extends TestCase {

    private static int messagesSinceFail = 0;

    private static final int FAIL_RANGE = 80000;

    private static HashMap<Integer, RussianRouletteMailbox> postoffice =
        new HashMap<Integer, RussianRouletteMailbox>();


    private void registerMailbox(int siteId, RussianRouletteMailbox mbox)
    {
        postoffice.put(siteId, mbox);
    }

    // A Mailbox implementation that will randomly simulate node failure by
    // killing a site instead of sending a message.
    // TODO: The failure decision is "tuned" to generate a decent number of
    // site failures but not so many as to kill every site in rapid succession.
    // Still fragile and could use some more work.
    class RussianRouletteMailbox implements Mailbox
    {
        int m_totalSends;
        private final Integer m_siteId;
        // Queue for DEFAULT Messages
        private final LinkedBlockingQueue<VoltMessage> incomingMessages;
        // Queue for FAILURE_SITE_UPDATE Messages
        private final LinkedBlockingQueue<VoltMessage> m_failureNoticeMessages;
        private int m_failureProb;

        public RussianRouletteMailbox(LinkedBlockingQueue<VoltMessage> queue,
                                      Integer siteId)
        {
            incomingMessages = queue;
            m_failureNoticeMessages = new LinkedBlockingQueue<VoltMessage>();
            m_failureProb = 0;
            m_siteId = siteId;
            m_totalSends = 0;
        }

        void setFailureLikelihood(int failChance)
        {
            m_failureProb = failChance;
        }

        // Synchronized so messagesSinceFail changes are thread-safe
        // across execution site accesses
        synchronized boolean shouldFail(boolean broadcast)
        {
            boolean fail = false;

            if (messagesSinceFail >= SAFE_SENDS)
            {
                messagesSinceFail = 0;
                System.out.println("Resetting safe count at site: " + m_siteId);
                fail = false;
            }
            else if (messagesSinceFail > 0)
            {
                messagesSinceFail++;
                return false;
            }

            int failchance = m_failureProb;
            if (broadcast)
            {
                failchance *= 10;
            }
            if (m_rand.nextInt(FAIL_RANGE) >= (FAIL_RANGE - failchance))
            {
                if (messagesSinceFail == 0)
                {
                    messagesSinceFail++;
                    System.out.println("Starting safe count at site: " + m_siteId);
                    fail = true;
                }
            }

            return fail;
        }

        public void send(int siteId, int mailboxId, VoltMessage message) throws MessagingException {
            m_totalSends++;
            if (shouldFail(false))
            {
                killSite();
                return;
            }

            RussianRouletteMailbox dest = postoffice.get(siteId);
            if (dest != null) {
                dest.deliver(message);
            }
        }

        public void send(int[] siteIds, int mailboxId, VoltMessage message) throws MessagingException {
            for (int i=0; siteIds != null && i < siteIds.length; ++i)
            {
                m_totalSends++;
                // There are more single send() rather than these broadcast
                // send()s, so we increase the chance of these because
                // there's more interesting behavior here.
                if (shouldFail(true))
                {
                    //System.out.println("FAILING NODE MID-BROADCAST");
                    killSite();
                    return;
                }

                RussianRouletteMailbox dest = postoffice.get(siteIds[i]);
                if (dest != null) {
                    dest.deliver(message);
                }
            }
        }

        public int getWaitingCount() {
            throw new UnsupportedOperationException();
        }

        public VoltMessage recv() {
            return recv(Subject.DEFAULT);
        }

        public VoltMessage recvBlocking() {
            return recvBlocking(Subject.DEFAULT);
        }

        @Override
        public VoltMessage recvBlocking(long timeout) {
            return recvBlocking(Subject.DEFAULT, timeout);
        }

        @Override
        public VoltMessage recv(Subject s) {
            if (s == Subject.DEFAULT)
                return incomingMessages.poll();
            else
                return m_failureNoticeMessages.poll();
        }

        @Override
        public VoltMessage recvBlocking(Subject s) {
            try {
                if (s == Subject.DEFAULT)
                    return incomingMessages.take();

                else
                    return m_failureNoticeMessages.take();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public VoltMessage recvBlocking(Subject s, long timeout) {
            do {
                try {
                    if (s == Subject.DEFAULT)
                        return incomingMessages.take();
                    else
                        return m_failureNoticeMessages.take();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            } while (true);
        }

        @Override
        public void deliver(VoltMessage message) {
            if (message.getSubject() == Subject.DEFAULT.getId())
                incomingMessages.offer(message);
            else
            {
                m_failureNoticeMessages.offer(message);
            }
        }

        // Unfortunately uses Thread.stop() to swiftly slay an execution site
        // like ninja...easiest way I found to kill the site and not allow it to
        // finish sending the messages for whatever transaction it was
        // involved in.
        @SuppressWarnings("deprecation")
        void killSite()
        {
            // Log breadcrumbs for validator
            m_siteLogger[m_siteId].trace("FUZZTEST selfNodeFailure " + getHostIdForSiteId(m_siteId));
            // mark the site as down in the catalog
            //System.out.println("KILLING SITE: " + m_siteId);
            m_sites[m_siteId].shutdown();
            m_voltdb.killSite(m_siteId);
            // send failure message to all remaining sites
            int[] site_ids = m_voltdb.getCatalogContext().siteTracker.getUpExecutionSites();
            for (int site_id : site_ids)
            {
                RussianRouletteMailbox dest = postoffice.get(site_id);
                if (dest != null)
                {
                    //System.out.println("Sending Failure Notification to site: " + site_id);
                    dest.deliver(new ExecutionSite.ExecutionSiteNodeFailureMessage(getHostIdForSiteId(m_siteId)));
                }
            }
            // remove this site from the postoffice
            postoffice.remove(m_siteId);
            // stop/join this site's thread
            m_siteThreads[m_siteId].stop();
        }
    }

    // ExecutionSite's snapshot processor requires the shared library
    static { EELibraryLoader.loadExecutionEngineLibrary(true); }

    // Topology parameters
    private static final int K_FACTOR = 2;
    private static final int PARTITION_COUNT = 3;
    private static final int SITE_COUNT = PARTITION_COUNT * (K_FACTOR + 1);
    // These are to hack around the fact that we don't handle concurrent
    // failures gracefully.  If we see a fail, we prevent the RussianRouletteMailbox
    // from inducing another failure for 5000 messages per site, which should be enough
    // for the failure handling to resolve itself
    // This number is large because there appears to be a message flood
    // on failures.  rbetts and I speculate some sort of heartbeat response flail, as yet
    // unconfirmed.
    private static final int SAFE_SENDS = 5000 * SITE_COUNT;

    MockVoltDB m_voltdb;
    ExecutionSiteFuzzChecker m_checker;
    RestrictedPriorityQueue m_rpqs[] = new RestrictedPriorityQueue[SITE_COUNT];
    ExecutionSite m_sites[] = new ExecutionSite[SITE_COUNT];
    RussianRouletteMailbox m_mboxes[] = new RussianRouletteMailbox[SITE_COUNT];
    Thread m_siteThreads[] = new Thread[SITE_COUNT];
    Logger[] m_siteLogger = new Logger[SITE_COUNT];
    StringWriter[] m_siteResults = new StringWriter[SITE_COUNT];
    Random m_rand;

    @Override
    protected void setUp() throws Exception
    {
        super.setUp();
        messagesSinceFail = 0;
        long seed = System.currentTimeMillis();
        //System.out.println("FUZZ SEED: " + seed);
        m_rand = new Random(seed);
        m_checker = new ExecutionSiteFuzzChecker();

        m_voltdb = new MockVoltDB();
        m_voltdb.setFaultDistributor(new FaultDistributor());

        // one host and one initiator per site
        for (int ss=0; ss < SITE_COUNT; ss++) {
            m_voltdb.addHost(getHostIdForSiteId(ss));
            m_voltdb.addSite(getInitiatorIdForSiteId(ss),
                             getHostIdForSiteId(ss),
                             getPartitionIdForSiteId(ss),
                             false);
            // Configure log4j so that ExecutionSite generates FUZZTEST output
            String logname = ExecutionSite.class.getName() + "." + ss;
            m_siteLogger[ss] = Logger.getLogger(logname,
                                                VoltLoggerFactory.instance());
            m_siteResults[ss] = new StringWriter();
            m_siteLogger[ss].addAppender(new WriterAppender(new SimpleLayout(),
                                                            m_siteResults[ss]));
            m_siteLogger[ss].setLevel(Level.TRACE);
        }

        // create k+1 sites per partition
        int siteid = 0;
        for (int pp=0; pp < PARTITION_COUNT; pp++) {
            m_voltdb.addPartition(pp);
            for (int kk=0; kk < (K_FACTOR + 1); kk++) {
                m_voltdb.addSite(siteid, getHostIdForSiteId(siteid), pp, true);
                m_checker.addSite(siteid, pp, m_siteResults[siteid]);
                ++siteid;
            }
        }

        if (siteid != SITE_COUNT) {
            throw new RuntimeException("Invalid setup logic.");
        }

        Procedure proc = null;
        proc = m_voltdb.addProcedureForTest(MockSPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(true);

        proc = m_voltdb.addProcedureForTest(MockROSPVoltProcedure.class.getName());
        proc.setReadonly(true);
        proc.setSinglepartition(true);

        proc = m_voltdb.addProcedureForTest(MockMPVoltProcedure.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(false);

        proc = m_voltdb.addProcedureForTest(MockMPVoltProcedureRollbackParticipant.class.getName());
        proc.setReadonly(false);
        proc.setSinglepartition(false);

        // Done with the logical topology.
        VoltDB.replaceVoltDBInstanceForTest(m_voltdb);

        // Create the real objects
        for (int ss=0; ss < SITE_COUNT; ++ss) {
            m_mboxes[ss] = new RussianRouletteMailbox(new LinkedBlockingQueue<VoltMessage>(),
                                                      ss);
            m_rpqs[ss] = new RestrictedPriorityARRR(getInitiatorIds(), ss, m_mboxes[ss]);
            m_sites[ss] = new ExecutionSite(m_voltdb, m_mboxes[ss], ss, null, m_rpqs[ss]);
            registerMailbox(ss, m_mboxes[ss]);
        }
    }

    @Override
    protected void tearDown() throws Exception
    {
        super.tearDown();
        for (int i=0; i < m_sites.length; ++i) {
            m_sites[i] = null;
            m_mboxes[i] = null;
        }
        m_voltdb = null;
        m_checker = null;
    }

    /* Partitions are assigned to sites in sequence: a,a,b,b,c,c.. */
    int getPartitionIdForSiteId(int siteId) {
        return (int) Math.floor(siteId / (K_FACTOR + 1));
    }

    /* Initiator ids are site ids + 1,000 */
    int getInitiatorIdForSiteId(int siteId) {
        return  siteId + 1000;
    }

    /* Get a site on the same "host" as the initiator */
    int getSiteIdForInitiatorId(int initiatorId) {
        int siteId = initiatorId - 1000;
        assert(getInitiatorIdForSiteId(siteId) == initiatorId);
        return siteId;
    }

    /* return a new array of initiator ids */
    int[] getInitiatorIds() {
        int[] ids = new int[SITE_COUNT];
        for (int ss=0; ss < SITE_COUNT; ss++) {
            ids[ss] = getInitiatorIdForSiteId(ss);
        }
        return ids;
    }

    /* Random initiator */
    private int selectRandomInitiator(Random rand) {
        int site =  rand.nextInt(SITE_COUNT);
        return getInitiatorIdForSiteId(site);
    }

    /* Given a partition, return a coordinator by value
       and the participants by out-param. */
    private int selectCoordinatorAndParticipants(Random rand, int partition,
                                                 int initiator,
                                                 List<Integer> participants)
    {
        // Failure detection relies on the assumption that coordinator and
        // initiator are co-located
        int coordinator = getSiteIdForInitiatorId(initiator);
        for (int i=0; i < SITE_COUNT; i++) {
            if (i == coordinator)
                continue;
            else
                participants.add(i);
        }
        return coordinator;
    }

    /* Host ids are site ids + 10,000 */
    int getHostIdForSiteId(int siteId) {
        return siteId + 66000;
    }

    List<Integer> getSiteIdsForPartitionId(int partitionId) {
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int ss=0; ss < SITE_COUNT; ++ss) {
            if (getPartitionIdForSiteId(ss) == partitionId) {
                result.add(ss);
            }
        }
        return result;
    }

    /* Fake RestrictedPriorityQueue implementation */
    public static class RestrictedPriorityARRR
    extends RestrictedPriorityQueue
    {
        private static final long serialVersionUID = 1L;

        /**
         * Initialize the RPQ with the set of initiators in the system and
         * the corresponding execution site's mailbox. Ugh.
         */
        public
        RestrictedPriorityARRR(int[] initiatorSiteIds, int siteId, Mailbox mbox)
        {
            super(initiatorSiteIds, siteId, mbox);
        }
    }


    /* Single partition write */
    public static class MockSPVoltProcedure extends VoltProcedure
    {
        public static int m_called = 0;

        boolean testReadOnly() {
            return false;
        }

        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList)
        {
            m_site.simulateExecutePlanFragments(txnState.txnId, testReadOnly());

            final ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                    new VoltTable[] {}, "MockSPVoltProcedure Response");
            ++m_called;
            return response;
        }
    }

     /* Single partition read */
    public static class MockROSPVoltProcedure extends MockSPVoltProcedure
    {
        @Override
        boolean testReadOnly() {
            return true;
        }
    }

    public static class MockMPVoltProcedureRollbackParticipant extends MockMPVoltProcedure
    {
        @Override
        boolean rollbackParticipant() { return true; }
    }

    /* Multi-partition - mock VoltProcedure.slowPath() */
    public static class MockMPVoltProcedure extends VoltProcedure
    {
        // Enable these simulated faults before running the procedure by setting
        // one of these booleans to true. Allows testcases to simulate various
        // coordinator node failures. Faults are turned back off once simulated
        // by the procedure (since they're static...)
        public static boolean simulate_coordinator_dies_during_commit = false;

        // Counter for test cases that want to see if the procedure ran.
        public static int m_called = 0;

        // Some functions that can be overridden by subclasses to change behavior.
        boolean finalTask()             { return false; }
        boolean nonTransactional()      { return false; }
        boolean rollbackParticipant()   { return false; }

        /* TODO: implement these.
        boolean rollbackCoordinator()   { return false; }
        boolean userRollbackProcStart() { return false; }
        boolean userRollbackProcEnd()   { return false; }
        */

        /** Helper to turn object list into parameter set buffer */
        private ByteBuffer createParametersBuffer(Object... paramList) {
            ParameterSet paramSet = new ParameterSet(true);
            paramSet.setParameters(paramList);
            FastSerializer fs = new FastSerializer();
            try {
                fs.writeObject(paramSet);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            ByteBuffer paramBuf = fs.getBuffer();
            return paramBuf;
        }

        @SuppressWarnings("deprecation")
        @Override
        ClientResponseImpl call(TransactionState txnState, Object... paramList)
        {
            try {
                ByteBuffer paramBuf = createParametersBuffer(paramList);

                // Build the aggregator and the distributed tasks.
                int localTask_startDep = txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
                int localTask_outputDep = txnState.getNextDependencyId();

                FragmentTaskMessage localTask =
                    new FragmentTaskMessage(txnState.initiatorSiteId,
                                            txnState.coordinatorSiteId,
                                            txnState.txnId,
                                            txnState.isReadOnly,
                                            new long[] {1},
                                            new int[] {localTask_outputDep},
                                            new ByteBuffer[] {paramBuf},
                                            finalTask());

                localTask.addInputDepId(0, localTask_startDep);

                FragmentTaskMessage distributedTask =
                    new FragmentTaskMessage(txnState.initiatorSiteId,
                                            txnState.coordinatorSiteId,
                                            txnState.txnId,
                                            txnState.isReadOnly,
                                            new long[] {0},
                                            new int[] {localTask_startDep},
                                            new ByteBuffer[] {paramBuf},
                                            finalTask());

                txnState.createLocalFragmentWork(localTask, nonTransactional() && finalTask());
                txnState.createAllParticipatingFragmentWork(distributedTask);
                txnState.setupProcedureResume(finalTask(), new int[] {localTask_outputDep});

                final Map<Integer, List<VoltTable>> resultDeps =
                    m_site.recursableRun(txnState);
                assertTrue(resultDeps != null);

                ++m_called;

                // simulate node failure: no commit sent to participant
                if (simulate_coordinator_dies_during_commit) {
                    // turn off the fault for the next time through
                    simulate_coordinator_dies_during_commit = false;
                    Thread.currentThread().stop();
                }

                // Return a made up table (no EE interaction anyway.. )
                VoltTable[] vta = new VoltTable[1];
                vta[0] = new VoltTable(new VoltTable.ColumnInfo("", VoltType.INTEGER));
                vta[0].addRow(new Integer(1));

                return new ClientResponseImpl(ClientResponse.SUCCESS, vta, null);
            }
            // VoltProcedure's call method converts invocation exceptions
            // to this error path. Do the same here.
            catch (SerializableException ex) {
                byte status = 0;
                return new ClientResponseImpl(ClientResponse.GRACEFUL_FAILURE,
                                              status, "", new VoltTable[0],
                                              "Intentional fuzz failure.", ex);
            }
        }
    }

    /*
     * SinglePartition basecase. Show that recursableRun completes a
     * single partition transaction.
     */
    public void testSinglePartitionTxn()
    {
        final boolean readOnly = false;
        final boolean singlePartition = true;

        // That the full procedure name is necessary is a bug in the
        // mock objects - or perhaps an issue with a nested class?
        // Or maybe a difference in what ClientInterface does?
        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        final InitiateTaskMessage tx1_mn =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0),
                                    0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[0], m_sites[0], tx1_mn);

        int callcheck = MockSPVoltProcedure.m_called;

        assertFalse(tx1.isDone());
        assertEquals(0, m_sites[0].lastCommittedTxnId);
        assertEquals(0, m_sites[0].lastCommittedMultiPartTxnId);
        m_sites[0].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[0].recursableRun(tx1);

        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[0].m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPVoltProcedure.m_called);
        assertEquals(1000, m_sites[0].lastCommittedTxnId);
        assertEquals(0, m_sites[0].lastCommittedMultiPartTxnId);
    }

    /*
     * Single partition read-only
     */
    public void testROSinglePartitionTxn()
    {
        final boolean readOnly = true;
        final boolean singlePartition = true;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockROSPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        final InitiateTaskMessage tx1_mn =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes[0], m_sites[0], tx1_mn);

        int callcheck = MockSPVoltProcedure.m_called;

        assertFalse(tx1.isDone());
        m_sites[0].m_transactionsById.put(tx1.txnId, tx1);
        m_sites[0].recursableRun(tx1);
        assertTrue(tx1.isDone());
        assertEquals(null, m_sites[0].m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPVoltProcedure.m_called);
    }

    /*
     * Multipartition basecase. Show that recursableRun completes a
     * multi partition transaction.
     */
    public void testMultiPartitionTxn() throws InterruptedException {
        final boolean readOnly = false, singlePartition = false;
        Thread es1, es2;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        // site 1 is the coordinator
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantMessage tx1_mn_2 =
            new MultiPartitionParticipantMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes[1], m_sites[1], tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());

        assertEquals(0, m_sites[0].lastCommittedTxnId);
        assertEquals(0, m_sites[0].lastCommittedMultiPartTxnId);
        assertEquals(0, m_sites[1].lastCommittedTxnId);
        assertEquals(0, m_sites[1].lastCommittedMultiPartTxnId);

        m_sites[0].m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites[1].m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[0].recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            public void run() {m_sites[1].recursableRun(tx1_2);}});
        es2.start();

        es1.join();
        es2.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertTrue(tx1_2.isDone());

        assertEquals(null, m_sites[0].m_transactionsById.get(tx1_1.txnId));
        assertEquals(null, m_sites[1].m_transactionsById.get(tx1_2.txnId));
        assertEquals(1000, m_sites[0].lastCommittedTxnId);
        assertEquals(1000, m_sites[0].lastCommittedMultiPartTxnId);
        assertEquals(1000, m_sites[1].lastCommittedTxnId);
        assertEquals(1000, m_sites[1].lastCommittedMultiPartTxnId);

        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }


    public
    void testMultipartitionParticipantCommitsOnFailure()
    throws InterruptedException
    {
        // cause the coordinator to die before committing.
        TestExecutionSite.MockMPVoltProcedure.
        simulate_coordinator_dies_during_commit = true;

        // The initiator's global commit point will be -1 because
        // the restricted priority queue is never fed by this testcase.
        // TxnIds in this testcase are chosen to make -1 a valid
        // global commit point. (Where -1 is DUMMY_LAST_SEEN...)

        // Want to commit this participant. Global commit pt must
        // be GT than the running txnid.
        m_sites[0].lastCommittedMultiPartTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;
        m_sites[1].lastCommittedMultiPartTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;

        boolean test_rollback = false;
        multipartitionNodeFailure(test_rollback, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
    }

    public
    void testMultiPartitionParticipantRollsbackOnFailure()
    throws InterruptedException
    {
        // cause the coordinator to die before committing.
        TestExecutionSite.MockMPVoltProcedure.
        simulate_coordinator_dies_during_commit = true;

        // The initiator's global commit point will be -1 because
        // the restricted priority queue is never fed by this testcase.
        // TxnIds in this testcase are chosen to make -1 a valid
        // global commit point. (Where -1 is DUMMY_LAST_SEEN...)

        // Want to NOT commit this participant. Global commit pt must
        // be LT than the running txnid.
        m_sites[0].lastCommittedMultiPartTxnId =  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID - 1;
        m_sites[1].lastCommittedMultiPartTxnId =  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID - 1;

        boolean test_rollback = true;
        multipartitionNodeFailure(test_rollback, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
    }

    /*
     * Simulate a multipartition participant blocked because the coordinating
     * node failed; at least one other node in the cluster has completed
     * this transaction -- and therefore it must commit at this participant.
     */
    private
    void multipartitionNodeFailure(boolean should_rollback, long txnid)
    throws InterruptedException
    {
        final boolean readOnly = false, singlePartition = false;
        Thread es1, es2;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        // site 1 is the coordinator
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, txnid, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantMessage tx1_mn_2 =
            new MultiPartitionParticipantMessage(getInitiatorIdForSiteId(0), 0, txnid, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes[1], m_sites[1], tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());
        m_sites[0].m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites[1].m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[0].recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            public void run() {m_sites[1].recursableRun(tx1_2);}});
        es2.start();

        es1.join();

        // coordinator is now dead. Update the survivor's catalog and
        // push a fault notice to the participant. Must supply the host id
        // corresponding to the coordinator site id.
        m_voltdb.killSite(0);

        // the fault data message from the "other surviving" sites
        // (all but the site actually running and the site that failed).
        for (int ss=2; ss < SITE_COUNT; ++ss) {
            m_mboxes[1].deliver(new FailureSiteUpdateMessage(ss, getHostIdForSiteId(0),
                                                             getInitiatorIdForSiteId(0),
                                                             txnid, (txnid -1 )));
        }
        // the fault distributer message to the execution site.
        m_mboxes[1].deliver(new ExecutionSite.ExecutionSiteNodeFailureMessage(getHostIdForSiteId(0)));
        es2.join();

        // post-conditions
        assertFalse(tx1_1.isDone());      // did not run to completion because of simulated fault
        assertTrue(tx1_2.isDone());       // did run to completion because of globalCommitPt.
        assertEquals(should_rollback, tx1_2.didRollback()); // did not rollback because of globalCommitPt.
        assertEquals(null, m_sites[1].m_transactionsById.get(tx1_2.txnId));
        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }

    /*
     * Create a multipartition work unit to test the removal of non-coordinator
     * site ids on failure. A little out of place in this file but the configured
     * ExecutionSite and Mailbox are necessary to construct a MP txn state.
     */
    public void testMultiPartitionParticipantTxnState_handleSiteFaults() {
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams("commit", 57, "gooniestoo");
        InitiateTaskMessage mn = new InitiateTaskMessage(-1, -1, -1, false, false, spi, Long.MIN_VALUE);
        mn.setNonCoordinatorSites(new int[] {1,2,3,4,5});

        MultiPartitionParticipantTxnState ts =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], mn);

        // fail middle and last site
        ArrayList<Integer> failedSites = new ArrayList<Integer>();
        failedSites.add(1);
        failedSites.add(2);
        failedSites.add(3);
        failedSites.add(5);
        ts.handleSiteFaults(failedSites);

        // steal dump accessors to peek at some internals
        ExecutorTxnState dumpContents = ts.getDumpContents();
        assertEquals(1, dumpContents.nonCoordinatingSites.length);
        assertEquals(4, dumpContents.nonCoordinatingSites[0]);

        // fail first site
        mn.setNonCoordinatorSites(new int[] {1,2,3,4,5});
        ts = new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], mn);
        failedSites.clear();
        failedSites.add(1);
        ts.handleSiteFaults(failedSites);

        dumpContents = ts.getDumpContents();
        assertEquals(4, dumpContents.nonCoordinatingSites.length);
        assertEquals(2, dumpContents.nonCoordinatingSites[0]);
        assertEquals(3, dumpContents.nonCoordinatingSites[1]);
        assertEquals(4, dumpContents.nonCoordinatingSites[2]);
        assertEquals(5, dumpContents.nonCoordinatingSites[3]);

        // fail site that isn't a non-coordinator site
        mn.setNonCoordinatorSites(new int[] {1,2,3,4,5});
        ts = new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], mn);
        failedSites.clear();
        failedSites.add(6);
        failedSites.add(7);
        ts.handleSiteFaults(failedSites);

        dumpContents = ts.getDumpContents();
        assertEquals(5, dumpContents.nonCoordinatingSites.length);
        for (int i=0; i < 5; i++) {
            assertEquals(i+1, dumpContents.nonCoordinatingSites[i]);
        }

    }

    /*
     * Show that a multi-partition transaction proceeds if one of the participants
     * fails
     */
    public void testFailedMultiPartitionParticipant() throws InterruptedException {
        final boolean readOnly = false, singlePartition = false;
        Thread es1;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        // site 1 is the coordinator. Use the txn id (DUMMY...) that the R.P.Q.
        // thinks is a valid safe-to-run txnid.
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0),
                                    0,
                                    DtxnConstants.DUMMY_LAST_SEEN_TXN_ID,
                                    readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);
        tx1_mn_1.setNonCoordinatorSites(new int[] {1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes[0], m_sites[0], tx1_mn_1);

        // Site 2 won't exist; we'll claim it fails.

        // pre-conditions
        int callcheck = MockMPVoltProcedure.m_called;
        assertFalse(tx1_1.isDone());
        m_sites[0].m_transactionsById.put(tx1_1.txnId, tx1_1);

        // execute transaction
        es1 = new Thread(new Runnable() {
            public void run() {m_sites[0].recursableRun(tx1_1);}});
        es1.start();

        m_voltdb.killSite(1);

        // the fault data message from the "other surviving" sites
        // (all but the site actually running and the site that failed).
        for (int ss=0; ss < SITE_COUNT; ++ss) {
            if ((ss != 1) || (ss != 0)) {
                m_mboxes[0].deliver(new FailureSiteUpdateMessage
                                    (ss, getHostIdForSiteId(1),
                                     getInitiatorIdForSiteId(1),
                                     tx1_1.txnId, tx1_1.txnId));
            }
        }

        // the fault message from the fault distributer to the execution site
        m_mboxes[0].deliver(new ExecutionSite.ExecutionSiteNodeFailureMessage(getHostIdForSiteId(1)));

        es1.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertFalse(tx1_1.didRollback());
        assertEquals(null, m_sites[0].m_transactionsById.get(tx1_1.txnId));
        assertEquals((++callcheck), MockMPVoltProcedure.m_called);
    }

    /*
     * FUZZ TESTS FOLLOW
     *
     * Driven directly through the ExecutionSite mailboxes.
     * Mailboxes can terminate a sender (at random) instead of delivering a message.
     * Verification is performed using the execution site trace logger.
     */

    /**
     * Create a single partition transaction.
     */
    private void createSPInitiation(
            boolean readOnly,
            long txn_id,
            long safe_txn_id,
            int initiator_id,
            int partition_id)
    {
        final StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
        spi.setParams("commit", new Integer(partition_id));

        List<Integer> sitesForPartition = getSiteIdsForPartitionId(partition_id);
        for (int i : sitesForPartition) {
            final InitiateTaskMessage itm =
                new InitiateTaskMessage(initiator_id,
                                        i,             // each site is its own coordinator
                                        txn_id,
                                        readOnly,
                                        true,          // single partition
                                        spi,
                                        safe_txn_id);  // last safe txnid
            m_mboxes[i].deliver(itm);
        }
    }

    /**
     * Create a multiple partition transaction
     */
    private void createMPInitiation(
            boolean rollback,
            boolean rollback_all,
            boolean readOnly,
            long txn_id,
            long safe_txn_id,
            int initiator_id,
            int partition_id,
            int coordinator_id,
            List<Integer> participants)
    {
        final StoredProcedureInvocation spi = new StoredProcedureInvocation();
        if (!rollback) {
            spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
            spi.setParams("commit", new Integer(partition_id));
        }
        else {
            if (rollback_all)
            {
                spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedureRollbackParticipant");
                spi.setParams("rollback_all", new Integer(partition_id));
            }
            else
            {
                spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedureRollbackParticipant");
                spi.setParams("rollback_random", new Integer(partition_id));
            }
        }

        System.out.println("Creating MP proc, TXN ID: " + txn_id + ", participants: " + participants.toString());

        final InitiateTaskMessage itm =
            new InitiateTaskMessage(initiator_id,
                                    coordinator_id,
                                    txn_id,
                                    readOnly,
                                    false,         // multi-partition
                                    spi,
                                    safe_txn_id);  // last safe txnid

        // just turn the list into an array .. grrr
        int[] participants_arr = new int[participants.size()];
        for (int i=0; i < participants.size(); ++i)
            participants_arr[i] = participants.get(i);

        itm.setNonCoordinatorSites(participants_arr);
        m_mboxes[coordinator_id].deliver(itm);

        for (int participant : participants) {
            final MultiPartitionParticipantMessage mppm =
                new MultiPartitionParticipantMessage
                (initiator_id, coordinator_id, txn_id, readOnly);
            m_mboxes[participant].deliver(mppm);
        }
    }

    /**
     * Create a heartbeat for initiator_id.
     * Currently sent to all up sites (also what simple dtxn does).
     * @param txn_id
     * @param safe_txn_id
     * @param initiator_id
     */
    private void createHeartBeat(
            long txn_id,
            long safe_txn_id,
            int initiator_id)
    {
        HeartbeatMessage hbm =
            new HeartbeatMessage(initiator_id, txn_id, safe_txn_id);
        int[] upExecutionSites =
            m_voltdb.getCatalogContext().siteTracker.getUpExecutionSites();

        for (int i : upExecutionSites) {
            m_mboxes[i].deliver(hbm);
        }
    }

    /*
     * Pick a random thing to do. If doing the last transaction,
     * send a heartbeat to flush all the queues.
     */
    private void queueTransactions(long firstTxnId, int totalTransactions, Random rand)
    {
        for (int i=0; i <= totalTransactions; ++i) {
            boolean rollback = rand.nextBoolean();
            // Disabling this as it results in too many all-failures currently
            //boolean rollback_all = rand.nextBoolean();
            boolean rollback_all = false;
            boolean readOnly = rand.nextBoolean();
            long txnid = i + firstTxnId;
            long safe_txnid = txnid;
            int initiator = selectRandomInitiator(rand);
            int partition = i % PARTITION_COUNT;

            int wheelOfDestiny = rand.nextInt(100);
            if (i == totalTransactions) {
                System.out.println("Queueing final heartbeat.");
                int offset = 0;
                for (int inid : getInitiatorIds()) {
                    createHeartBeat(txnid + offset, txnid + offset, inid);
                    ++offset;
                }
            }
            else if (wheelOfDestiny < 50) {
                createSPInitiation(readOnly, txnid, safe_txnid, initiator, partition);
            }
            else if (wheelOfDestiny < 70) {
                List<Integer> participants = new ArrayList<Integer>();
                int coordinator = selectCoordinatorAndParticipants(rand, partition, initiator, participants);
                createMPInitiation(rollback, rollback_all, readOnly, txnid,
                                   safe_txnid, initiator, partition,
                                   coordinator, participants);
            }
            else {
                createHeartBeat(txnid, safe_txnid, initiator);
            }
        }
    }



    /*
     * Run the mailboxes / sites to completion.
     */
    private void createAndRunSiteThreads()
    {
        for (int i=0; i < SITE_COUNT; ++i) {
            final int site_id = i;
            m_siteThreads[i] = new Thread(new Runnable() {
               @Override
               public void run() {
                   m_sites[site_id].runLoop();
               }
            });
        }

        for (int i=0; i < SITE_COUNT; ++i) {
            m_siteThreads[i].start();
        }
    }

    public void testFuzzedTransactions()
    {
        final int totalTransactions = 20000;
        final long firstTxnId = 10000;
        for (int i=0; i < SITE_COUNT; ++i) {
            m_mboxes[i].setFailureLikelihood(1);
        }
        queueTransactions(firstTxnId, totalTransactions, m_rand);
        createAndRunSiteThreads();

        // wait for all the sites to terminate runLoops
        for (int i=0; i < SITE_COUNT; ++i) {
            boolean stopped = false;
            do {
                try {
                    m_siteThreads[i].join();
                }
                catch (InterruptedException e) {
                }
                if (m_siteThreads[i].isAlive() == false) {
                    System.out.println("Joined site " + i);
                    stopped = true;
                }
            } while (!stopped);
        }

        for (int i = 0; i < SITE_COUNT; ++i)
        {
            System.out.println("sends for mailbox: " + i + ": " + m_mboxes[i].m_totalSends);
        }

        m_checker.dumpLogs();
        assertTrue(m_checker.validateLogs());
    }
}
