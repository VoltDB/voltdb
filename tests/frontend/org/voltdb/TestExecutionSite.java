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

package org.voltdb;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.commons.lang3.ArrayUtils;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HeartbeatMessage;
import org.voltcore.messaging.HeartbeatResponseMessage;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.RestrictedPriorityQueue;
import org.voltdb.dtxn.SinglePartitionTxnState;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.executionsitefuzz.ExecutionSiteFuzzChecker;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

public class TestExecutionSite extends TestCase {

    private static final VoltLogger testLog = new VoltLogger("TEST");

    private static final int FAIL_RANGE = 80000;

    private static HashMap<Long, RussianRouletteMailbox> postoffice =
        new HashMap<Long, RussianRouletteMailbox>();


    private void registerMailbox(long siteId, RussianRouletteMailbox mbox)
    {
        postoffice.put(siteId, mbox);
    }

    private static final AtomicInteger m_nextMustDie = new AtomicInteger(-1);

    // A Mailbox implementation that will randomly simulate node failure by
    // killing a site instead of sending a message.
    // TODO: The failure decision is "tuned" to generate a decent number of
    // site failures but not so many as to kill every site in rapid succession.
    // Still fragile and could use some more work.
    class RussianRouletteMailbox implements Mailbox
    {
        int m_totalSends;
        int m_heartBeatSends;
        private final Long m_siteId;
        final ArrayList<Deque<VoltMessage>> m_messages = new ArrayList<Deque<VoltMessage>>();
        private int m_failureProb;

        public RussianRouletteMailbox(Long siteId)
        {
            for (Subject s : Subject.values()) {
                m_messages.add( s.getId(), new ArrayDeque<VoltMessage>());
            }
            m_failureProb = 0;
            m_siteId = siteId;
            m_totalSends = 0;
        }

        @Override
        public long getHSId() {
            return m_siteId;
        }

        @Override
        public void setHSId(long hsId) {
            throw new UnsupportedOperationException();
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
            // if we don't want failure, don't give m_nextMustDie a chance to kill us
            if (m_failureProb == 0)
            {
                return false;
            }
            int failchance = m_failureProb;
            if (broadcast)
            {
                failchance *= 10;
            }
            int nextMustDie = m_nextMustDie.decrementAndGet();
            if (nextMustDie == 0 || (m_rand.nextInt(FAIL_RANGE) >= (FAIL_RANGE - failchance)))
            {
                fail = true;
                if (nextMustDie < 0) {
                    if (m_rand.nextInt(1) == 0) {
                        m_nextMustDie.set(100);
                    }
                }
            }

            return fail;
        }

        @Override
        public void send(long siteId, VoltMessage message) {
            message.m_sourceHSId = m_siteId;
            m_totalSends++;
            if (message instanceof HeartbeatResponseMessage) {
                m_heartBeatSends++;
            }
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

        @Override
        public void send(long[] siteIds, VoltMessage message) {
            message.m_sourceHSId = m_siteId;
            if (message instanceof HeartbeatResponseMessage) {
                m_heartBeatSends += siteIds.length;
            }
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

        private final Subject m_defaultSubjects[] = new Subject[] { Subject.FAILURE, Subject.DEFAULT };

        @Override
        public VoltMessage recv() {
            return recv(m_defaultSubjects);
        }

        @Override
        public VoltMessage recvBlocking() {
            return recvBlocking(m_defaultSubjects);
        }

        @Override
        public VoltMessage recvBlocking(long timeout) {
            return recvBlocking(m_defaultSubjects, timeout);
        }

        @Override
        public synchronized VoltMessage recv(Subject subjects[]) {
            for (Subject s : subjects) {
                final Deque<VoltMessage> dq = m_messages.get(s.getId());
                assert(dq != null);
                VoltMessage m = dq.poll();
                if (m != null) {
                    return m;
                }
            }
            return null;
        }

        @Override
        public synchronized VoltMessage recvBlocking(Subject subjects[]) {
            VoltMessage message = null;
            while (message == null) {
                for (Subject s : subjects) {
                    final Deque<VoltMessage> dq = m_messages.get(s.getId());
                    message = dq.poll();
                    if (message != null) {
                        return message;
                    }
                }
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    return null;
                }
            }
            return null;
        }

        @Override
        public synchronized VoltMessage recvBlocking(Subject subjects[], long timeout) {
            VoltMessage message = null;
            for (Subject s : subjects) {
                final Deque<VoltMessage> dq = m_messages.get(s.getId());
                message = dq.poll();
                if (message != null) {
                    return message;
                }
            }
            try {
                this.wait(timeout);
            } catch (InterruptedException e) {
                return null;
            }
            for (Subject s : subjects) {
                final Deque<VoltMessage> dq = m_messages.get(s.getId());
                message = dq.poll();
                if (message != null) {
                    return message;
                }
            }
            return null;
        }

        @Override
        public void deliver(VoltMessage message) {
            deliver(message, false);
        }

        @Override
        public void deliverFront(VoltMessage message) {
            deliver(message, true);
        }

        public void deliver(VoltMessage message, final boolean toFront) {
            final Deque<VoltMessage> dq = m_messages.get(message.getSubject());
            synchronized (this) {
                if (toFront){
                    dq.push(message);
                } else {
                    dq.offer(message);
                }
                this.notify();
            }
        }

        // Unfortunately uses Thread.stop() to swiftly slay an execution site
        // like ninja...easiest way I found to kill the site and not allow it to
        // finish sending the messages for whatever transaction it was
        // involved in.
        void killSite()
        {
            // Log breadcrumbs for validator
            m_siteLogger.get(m_siteId).trace("FUZZTEST selfNodeFailure " + CoreUtils.getHostIdFromHSId(m_siteId));
            // mark the site as down in the catalog
            //System.out.println("KILLING SITE: " + m_siteId);
            m_sites.get(m_siteId).shutdown();
            m_voltdb.killSite(m_siteId);
            long initiatorId = getInitiatorIds()[CoreUtils.getSiteIdFromHSId(m_siteId)];
            m_voltdb.killSite(initiatorId);

            /*
             * Fail the site, and its initiator
             */
            m_voltdb.getFaultDistributor().reportFault(
                    new SiteFailureFault(Arrays.asList(new Long[] {
                            m_siteId, initiatorId})));
            // remove this site from the postoffice
            postoffice.remove(m_siteId);
            // stop/join this site's thread
            throw new Error();
        }
    }

    // ExecutionSite's snapshot processor requires the shared library
    static { EELibraryLoader.loadExecutionEngineLibrary(true); }

    /*
     * If you change the topology parameters there is a chance you might
     * make the test for ENG1617 invalid
     */
    // Topology parameters
    private static final int K_FACTOR = 2;
    private static final int PARTITION_COUNT = 3;
    private static final int SITE_COUNT = PARTITION_COUNT * (K_FACTOR + 1);

    MockVoltDB m_voltdb;
    ExecutionSiteFuzzChecker m_checker;
    Map<Long, RestrictedPriorityQueue> m_rpqs = new HashMap<Long, RestrictedPriorityQueue>();
    Map<Long, ExecutionSite> m_sites = new HashMap<Long, ExecutionSite>();
    Map<Long, RussianRouletteMailbox> m_mboxes = new HashMap<Long, RussianRouletteMailbox>();
    Map<Long, Thread> m_siteThreads = new HashMap<Long, Thread>();
    Map<Long, VoltLogger> m_siteLogger = new HashMap<Long, VoltLogger>();
    Map<Long, StringWriter> m_siteResults = new HashMap<Long, StringWriter>();
    Random m_rand;

    private void start(int siteCount, int partitionCount, int kFactor) throws Exception {
        long seed = System.currentTimeMillis();
        m_rand = new Random(seed);
        m_checker = new ExecutionSiteFuzzChecker();

        m_voltdb = new MockVoltDB();
        m_voltdb.setFaultDistributor(new FaultDistributor(m_voltdb));

        // one host and one initiator per site
        for (int ss=0; ss < siteCount; ss++) {
            final long siteId = CoreUtils.getHSIdFromHostAndSite(getHostIdForSiteId(ss), getInitiatorIdForSiteId(ss));
            m_voltdb.addSite( siteId, MailboxType.Initiator);
            // Configure log4j so that ExecutionSite generates FUZZTEST output
            String logname = ExecutionSite.class.getName() + "." + ss;

            final VoltLogger siteLogger = new VoltLogger(logname);
            m_siteLogger.put(siteId, siteLogger);
            final StringWriter siteResults = new StringWriter();
            m_siteResults.put(siteId, siteResults);
            siteLogger.addSimpleWriterAppender(siteResults);
            siteLogger.setLevel(Level.TRACE);
        }

        // create k+1 sites per partition
        int siteIndex = 0;
        for (int pp=0; pp < partitionCount; pp++) {
            for (int kk=0; kk < (kFactor + 1); kk++) {
                final long siteId = CoreUtils.getHSIdFromHostAndSite(getHostIdForSiteId(siteIndex), siteIndex);
                m_voltdb.addSite(siteId, pp);
                m_checker.addSite(siteId, pp, m_siteResults.get(CoreUtils.getHSIdFromHostAndSite( siteIndex, getInitiatorIdForSiteId(siteIndex))));
                ++siteIndex;
            }
        }

        if (siteIndex != siteCount) {
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
        for (int ss=0; ss < siteCount; ++ss) {
            long siteId = CoreUtils.getHSIdFromHostAndSite( ss, ss);
            m_mboxes.put(siteId, new RussianRouletteMailbox( siteId ));
            m_siteLogger.put(
                    siteId,
                    m_siteLogger.get(CoreUtils.getHSIdFromHostAndSite( ss, getInitiatorIdForSiteId(ss))));
            m_rpqs.put( siteId, new RestrictedPriorityARRR(getInitiatorIds(), ss, m_mboxes.get(siteId)));
            m_sites.put(siteId,
                new ExecutionSite(
                        m_voltdb,
                        m_mboxes.get(siteId),
                        null,
                        m_rpqs.get(siteId),
                        new MockProcedureRunnerFactory(),
                        false,
                        null,
                        0,
                        partitionCount,
                        null));
            registerMailbox(siteId, m_mboxes.get(siteId));
        }
    }

    @Override
    protected void tearDown() throws Exception
    {
        System.out.println("Doing shutdown");
        super.tearDown();
        m_sites.clear();
        m_mboxes.clear();
        if (m_voltdb != null) {
            m_voltdb.shutdown(null);
            m_voltdb = null;
        }
        System.out.println("Shutdown ZK");
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
    long getSiteIdForInitiatorId(long initiatorId) {
        int siteId = CoreUtils.getSiteIdFromHSId(initiatorId) - 1000;
        assert(CoreUtils.getHSIdFromHostAndSite( siteId, getInitiatorIdForSiteId(siteId)) == initiatorId);
        return CoreUtils.getHSIdFromHostAndSite( siteId, siteId);
    }

    long getHSIdForES(int siteId) {
        return CoreUtils.getHSIdFromHostAndSite(getHostIdForSiteId(siteId), siteId);
    }

    /* return a new array of initiator ids */
    long[] getInitiatorIds() {
        long[] ids = new long[SITE_COUNT];
        for (int ss=0; ss < SITE_COUNT; ss++) {
            ids[ss] = CoreUtils.getHSIdFromHostAndSite( getHostIdForSiteId(ss), getInitiatorIdForSiteId(ss));
        }
        return ids;
    }

    /* Random initiator */
    private long selectRandomInitiator(Random rand) {
        int site =  rand.nextInt(SITE_COUNT);
        return CoreUtils.getHSIdFromHostAndSite(getHostIdForSiteId(site), getInitiatorIdForSiteId(site));
    }

    /* Given a partition, return a coordinator by value
       and the participants by out-param. */
    private long selectCoordinatorAndParticipants(Random rand, int partition,
                                                 long initiator,
                                                 List<Long> participants)
    {
        // Failure detection relies on the assumption that coordinator and
        // initiator are co-located
        long coordinator = getSiteIdForInitiatorId(initiator);
        for (int i=0; i < SITE_COUNT; i++) {
            long hsId = getHSIdForES(i);
            if (hsId == coordinator)
                continue;
            else
                participants.add(hsId);
        }
        return coordinator;
    }

    /* Host ids are site ids */
    int getHostIdForSiteId(int siteId) {
        return siteId;
    }

    List<Long> getSiteIdsForPartitionId(int partitionId) {
        ArrayList<Long> result = new ArrayList<Long>();
        for (int ss=0; ss < SITE_COUNT; ++ss) {
            if (getPartitionIdForSiteId(ss) == partitionId) {
                result.add(getHSIdForES(ss));
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
        RestrictedPriorityARRR(long[] initiatorSiteIds, long siteId, Mailbox mbox)
        {
            super(initiatorSiteIds, siteId, mbox, true);
        }
    }

    public static class MockProcedureRunnerFactory extends ProcedureRunnerFactory {

        @Override
        public ProcedureRunner create(VoltProcedure procedure, Procedure catProc, CatalogSpecificPlanner csp) {
            if (procedure instanceof MockROSPVoltProcedure)
                return new MockSPProcedureRunner((MockSPVoltProcedure) procedure, (ExecutionSite) super.m_site);
            else if (procedure instanceof MockSPVoltProcedure)
                return new MockSPProcedureRunner((MockSPVoltProcedure) procedure, (ExecutionSite) super.m_site);
            else if (procedure instanceof MockMPVoltProcedure)
                return new MockMPProcedureRunner((MockMPVoltProcedure) procedure, (ExecutionSite) super.m_site);
            else if (procedure instanceof VoltSystemProcedure)
                return super.create(procedure, catProc, csp);
            else
                assert(false);
            return null;
        }
    }

    public static class MockSPProcedureRunner extends ProcedureRunner {

        public static int m_called = 0;
        final MockSPVoltProcedure m_procedure;
        final ExecutionSite m_site;

        MockSPProcedureRunner(MockSPVoltProcedure procedure, ExecutionSite site) {
            super(procedure, site, null, null, null);
            m_procedure = procedure;
            m_site = site;
        }

        @Override
        public
        ClientResponseImpl call(Object... paramList) {
            m_site.simulateExecutePlanFragments(m_txnState.txnId, m_procedure.testReadOnly());

            final ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                    new VoltTable[] {}, "MockSPVoltProcedure Response");
            ++m_called;
            return response;
        }

        @Override
        protected void reflect() {}
    }

    /* Single partition write */
    public static class MockSPVoltProcedure extends VoltProcedure
    {
        boolean testReadOnly() {
            return false;
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

    public static class MockMPVoltProcedureRollbackParticipant extends MockMPVoltProcedure {
        @Override
        boolean rollbackParticipant() { return true; }
    }

    public static class MockMPVoltProcedure extends VoltProcedure {
        boolean rollbackParticipant()   { return false; }
    }

    /* Multi-partition - mock VoltProcedure.slowPath() */
    public static class MockMPProcedureRunner extends ProcedureRunner
    {
        public final MockMPVoltProcedure m_procedure;
        public final ExecutionSite m_site;

        int m_numberOfBatches = 1;

        // Enable these simulated faults before running the procedure by setting
        // one of these booleans to true. Allows testcases to simulate various
        // coordinator node failures. Faults are turned back off once simulated
        // by the procedure (since they're static...)
        public static boolean simulate_coordinator_dies_during_commit = false;

        // Counter for test cases that want to see if the procedure ran.
        public static int m_called = 0;

        // Some functions that can be overridden by subclasses to change behavior.
        int numberOfBatches()           { return m_numberOfBatches; }
        int statementsPerBatch()        { return 1; }
        boolean nonTransactional()      { return true; }

        /* TODO: implement these.
        boolean rollbackCoordinator()   { return false; }
        boolean userRollbackProcStart() { return false; }
        boolean userRollbackProcEnd()   { return false; }
        */

        public MockMPProcedureRunner(MockMPVoltProcedure procedure, ExecutionSite site) {
            super(procedure, site, null, null, null);
            m_procedure = procedure;
            m_site = site;
        }

        @Override
        protected void reflect() {}

        /** Helper to look for interesting params in the list and set
         * internal state based on it
         */
        private void parseParamList(Object... paramList)
        {
            ArrayList<Object> params = new ArrayList<Object>();
            for (Object param : paramList)
            {
                params.add(param);
            }

            // parse out number of batches
            int num_batches_index = params.indexOf("number_of_batches");
            if (num_batches_index != -1)
            {
                m_numberOfBatches = (Integer)params.get(num_batches_index + 1);
            }
        }

        /** Helper to turn object list into parameter set buffer */
        private ByteBuffer createParametersBuffer(Object... paramList) {
            ParameterSet paramSet = new ParameterSet();
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

        @Override
        public
        ClientResponseImpl call(Object... paramList)
        {
            try {
                parseParamList(paramList);
                ByteBuffer paramBuf = createParametersBuffer(paramList);

                for (int i = 0; i < numberOfBatches(); i++)
                {
                    boolean finalTask = (i == numberOfBatches() - 1);

                    // XXX-IZZY these will turn into arrays for multi-statement batches
                    // Build the aggregator and the distributed tasks.
                    int localTask_startDep = m_txnState.getNextDependencyId() | DtxnConstants.MULTIPARTITION_DEPENDENCY;
                    int localTask_outputDep = m_txnState.getNextDependencyId();

                    FragmentTaskMessage localTask =
                        FragmentTaskMessage.createWithOneFragment(m_txnState.initiatorHSId,
                                                m_txnState.coordinatorSiteId,
                                                m_txnState.txnId,
                                                m_txnState.uniqueId,
                                                m_txnState.isReadOnly(),
                                                1,
                                                localTask_outputDep,
                                                paramBuf,
                                                false,
                                                false);

                    localTask.addInputDepId(0, localTask_startDep);

                    FragmentTaskMessage distributedTask =
                        FragmentTaskMessage.createWithOneFragment(m_txnState.initiatorHSId,
                                                m_txnState.coordinatorSiteId,
                                                m_txnState.txnId,
                                                m_txnState.uniqueId,
                                                m_txnState.isReadOnly(),
                                                0,
                                                localTask_startDep,
                                                paramBuf,
                                                finalTask,
                                                false);

                    m_txnState.createLocalFragmentWork(localTask, nonTransactional() && finalTask);
                    m_txnState.createAllParticipatingFragmentWork(distributedTask);
                    m_txnState.setupProcedureResume(finalTask, new int[] {localTask_outputDep});

                    final Map<Integer, List<VoltTable>> resultDeps =
                        m_site.recursableRun(m_txnState);
                    assertTrue(resultDeps != null);
                }

                ++m_called;

                // simulate node failure: no commit sent to participant
                if (simulate_coordinator_dies_during_commit) {
                    // turn off the fault for the next time through
                    simulate_coordinator_dies_during_commit = false;
                    throw new ThreadDeath();
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

    public void testFuzzedTransactions() throws Exception
    {
        for (int ii = 0; ii < 1; ii++) {
            tearDown();
            System.gc();
            start(SITE_COUNT, PARTITION_COUNT, K_FACTOR);

            final int totalTransactions = 20000;
            final long firstTxnId = 10000;
            for (int i=0; i < SITE_COUNT; ++i) {
                m_mboxes.get(getHSIdForES(i)).setFailureLikelihood(1);
            }
            queueTransactions(firstTxnId, totalTransactions, m_rand);
            createAndRunSiteThreads();

            // wait for all the sites to terminate runLoops
            for (int i=0; i < SITE_COUNT; ++i) {
                boolean stopped = false;
                do {
                    try {
                        m_siteThreads.get(getHSIdForES(i)).join();
                    }
                    catch (InterruptedException e) {
                    }
                    if (m_siteThreads.get(getHSIdForES(i)).isAlive() == false) {
                        System.out.println("Joined site " + i);
                        stopped = true;
                    }
                } while (!stopped);
            }

            for (int i = 0; i < SITE_COUNT; ++i)
            {
                System.out.println("sends for mailbox: " + i + ": " + m_mboxes.get(getHSIdForES(i)).m_totalSends);
            }

            m_checker.dumpLogs();
            assertTrue(m_checker.validateLogs());
        }
    }

    /*
     * SinglePartition basecase. Show that recursableRun completes a
     * single partition transaction.
     */
    public void testSinglePartitionTxn() throws Exception
    {
        start(SITE_COUNT, PARTITION_COUNT, K_FACTOR);
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

        final long siteId0 = getHSIdForES(0);
        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes.get(siteId0), m_sites.get(siteId0), tx1_mn);

        int callcheck = MockSPProcedureRunner.m_called;

        assertFalse(tx1.isDone());
        assertEquals(0, m_sites.get(siteId0).lastCommittedTxnId);
        assertEquals(0, m_sites.get(siteId0).lastKnownGloballyCommitedMultiPartTxnId);
        m_sites.get(siteId0).m_transactionsById.put(tx1.txnId, tx1);
        m_sites.get(siteId0).recursableRun(tx1);

        assertTrue(tx1.isDone());
        assertEquals(null, m_sites.get(siteId0).m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPProcedureRunner.m_called);
        assertEquals(1000, m_sites.get(siteId0).lastCommittedTxnId);
        assertEquals(0, m_sites.get(siteId0).lastKnownGloballyCommitedMultiPartTxnId);
    }

    /*
     * Single partition read-only
     */
    public void testROSinglePartitionTxn() throws Exception
    {
        start(SITE_COUNT, PARTITION_COUNT, K_FACTOR);
        final boolean readOnly = true;
        final boolean singlePartition = true;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockROSPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        final InitiateTaskMessage tx1_mn =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE);

        final long siteId0 = getHSIdForES(0);
        final SinglePartitionTxnState tx1 =
            new SinglePartitionTxnState(m_mboxes.get(siteId0), m_sites.get(siteId0), tx1_mn);

        int callcheck = MockSPProcedureRunner.m_called;

        assertFalse(tx1.isDone());
        m_sites.get(siteId0).m_transactionsById.put(tx1.txnId, tx1);
        m_sites.get(siteId0).recursableRun(tx1);
        assertTrue(tx1.isDone());
        assertEquals(null, m_sites.get(siteId0).m_transactionsById.get(tx1.txnId));
        assertEquals((++callcheck), MockSPProcedureRunner.m_called);
    }

    /*
     * Multipartition basecase. Show that recursableRun completes a
     * multi partition transaction.
     */
    public void testMultiPartitionTxn() throws Exception {
        tearDown();
        start( 2, 2, 0);
        final boolean readOnly = false, singlePartition = false;
        Thread es1, es2;

        final StoredProcedureInvocation tx1_spi = new StoredProcedureInvocation();
        tx1_spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
        tx1_spi.setParams("commit", new Integer(0));

        final long siteId0 = getHSIdForES(0);
        final long siteId1 = getHSIdForES(1);

        // site 1 is the coordinator
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE,
                                    new long[] {siteId1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes.get(siteId0), m_sites.get(siteId0), tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantMessage tx1_mn_2 =
            new MultiPartitionParticipantMessage(getInitiatorIdForSiteId(0), 0, 1000, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes.get(siteId1), m_sites.get(siteId1), tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPProcedureRunner.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());

        assertEquals(0, m_sites.get(siteId0).lastCommittedTxnId);
        assertEquals(0, m_sites.get(siteId0).lastKnownGloballyCommitedMultiPartTxnId);
        assertEquals(0, m_sites.get(siteId1).lastCommittedTxnId);
        assertEquals(0, m_sites.get(siteId1).lastKnownGloballyCommitedMultiPartTxnId);

        m_sites.get(siteId0).m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites.get(siteId1).m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            @Override
            public void run() {m_sites.get(siteId0).recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            @Override
            public void run() {m_sites.get(siteId1).recursableRun(tx1_2);}});
        es2.start();

        es1.join();
        es2.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertTrue(tx1_2.isDone());

        assertEquals(null, m_sites.get(siteId0).m_transactionsById.get(tx1_1.txnId));
        assertEquals(null, m_sites.get(siteId1).m_transactionsById.get(tx1_2.txnId));
        assertEquals(1000, m_sites.get(siteId1).lastCommittedTxnId);
        assertEquals(1000, m_sites.get(siteId0).lastKnownGloballyCommitedMultiPartTxnId);
        assertEquals(1000, m_sites.get(siteId1).lastCommittedTxnId);
        assertEquals(1000, m_sites.get(siteId1).lastKnownGloballyCommitedMultiPartTxnId);

        assertEquals((++callcheck), MockMPProcedureRunner.m_called);
    }


    public
    void testMultipartitionParticipantCommitsOnFailure()
    throws Exception
    {
        tearDown();
        start( 2, 2, 0);
        // cause the coordinator to die before committing.
        TestExecutionSite.MockMPProcedureRunner.
        simulate_coordinator_dies_during_commit = true;

        // The initiator's global commit point will be -1 because
        // the restricted priority queue is never fed by this testcase.
        // TxnIds in this testcase are chosen to make -1 a valid
        // global commit point. (Where -1 is DUMMY_LAST_SEEN...)

        final long siteId0 = getHSIdForES(0);
        final long siteId1 = getHSIdForES(1);
        // Want to commit this participant. Global commit pt must
        // be GT than the running txnid.
        m_sites.get(siteId0).lastKnownGloballyCommitedMultiPartTxnId =
            DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;
        m_sites.get(siteId1).lastKnownGloballyCommitedMultiPartTxnId =
            DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;

        boolean test_rollback = false;
        multipartitionNodeFailure(test_rollback, DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
    }

    public
    void testMultiPartitionParticipantRollsbackOnFailure()
    throws Exception
    {
        tearDown();
        start( 2, 2, 0);
        // cause the coordinator to die before committing.
        TestExecutionSite.MockMPProcedureRunner.
        simulate_coordinator_dies_during_commit = true;

        // The initiator's global commit point will be -1 because
        // the restricted priority queue is never fed by this testcase.
        // TxnIds in this testcase are chosen to make -1 a valid
        // global commit point. (Where -1 is DUMMY_LAST_SEEN...)

        final long siteId0 = getHSIdForES(0);
        final long siteId1 = getHSIdForES(1);

        // Want to NOT commit this participant. Global commit pt must
        // be LT than the running txnid.
        m_sites.get(siteId0).lastKnownGloballyCommitedMultiPartTxnId =  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID - 1;
        m_sites.get(siteId1).lastKnownGloballyCommitedMultiPartTxnId =  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID - 1;

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

        final long siteId0 = getHSIdForES(0);
        final long siteId1 = getHSIdForES(1);

        // site 1 is the coordinator
        final InitiateTaskMessage tx1_mn_1 =
            new InitiateTaskMessage(getInitiatorIdForSiteId(0), 0, txnid, readOnly, singlePartition, tx1_spi, Long.MAX_VALUE,
                                    new long[] {siteId1});

        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m_mboxes.get(siteId0), m_sites.get(siteId0), tx1_mn_1);

        // site 2 is a participant
        final MultiPartitionParticipantMessage tx1_mn_2 =
            new MultiPartitionParticipantMessage(getInitiatorIdForSiteId(0), 0, txnid, readOnly);

        final MultiPartitionParticipantTxnState tx1_2 =
            new MultiPartitionParticipantTxnState(m_mboxes.get(siteId1), m_sites.get(siteId1), tx1_mn_2);

        // pre-conditions
        int callcheck = MockMPProcedureRunner.m_called;
        assertFalse(tx1_1.isDone());
        assertFalse(tx1_2.isDone());
        m_sites.get(siteId0).m_transactionsById.put(tx1_1.txnId, tx1_1);
        m_sites.get(siteId1).m_transactionsById.put(tx1_2.txnId, tx1_2);

        // execute transaction
        es1 = new Thread(new Runnable() {
            @Override
            public void run() {m_sites.get(siteId0).recursableRun(tx1_1);}});
        es1.start();

        es2 = new Thread(new Runnable() {
            @Override
            public void run() {m_sites.get(siteId1).recursableRun(tx1_2);}});
        es2.start();

        es1.join();

        // coordinator is now dead. Update the survivor's catalog and
        // push a fault notice to the participant. Must supply the host id
        // corresponding to the coordinator site id.
        m_voltdb.killSite(0);
        m_voltdb.killSite(CoreUtils.getHSIdFromHostAndSite( 0, getInitiatorIdForSiteId(0)));
        m_voltdb.getFaultDistributor().reportFault(
                new SiteFailureFault(
                        Arrays.asList(
                                new Long[] {
                                        CoreUtils.getHSIdFromHostAndSite(
                                                0, getInitiatorIdForSiteId(0)), getHSIdForES(0)})));

        es2.join();

        // post-conditions
        assertFalse(tx1_1.isDone());      // did not run to completion because of simulated fault
        assertTrue(tx1_2.isDone());       // did run to completion because of globalCommitPt.
        assertEquals(should_rollback, tx1_2.needsRollback()); // did not rollback because of globalCommitPt.
        assertEquals(null, m_sites.get(siteId1).m_transactionsById.get(tx1_2.txnId));
        assertEquals((++callcheck), MockMPProcedureRunner.m_called);
    }

    /*
     * Create a multipartition work unit to test the removal of non-coordinator
     * site ids on failure. A little out of place in this file but the configured
     * ExecutionSite and Mailbox are necessary to construct a MP txn state.
     */
    @SuppressWarnings("deprecation")
    public void testMultiPartitionParticipantTxnState_handleSiteFaults() throws Exception {
        start(SITE_COUNT, PARTITION_COUNT, K_FACTOR);
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setClientHandle(25);
        spi.setProcName("johnisgreat");
        spi.setParams("commit", 57, "gooniestoo");

        long[] nonCoordinatorSites = new long[SITE_COUNT - 1];
        for (int i = 0; i < SITE_COUNT - 1; i++) {
            nonCoordinatorSites[i] = getHSIdForES(i + 1);
        }

        InitiateTaskMessage mn = new InitiateTaskMessage(-1, 0, -1, false, false, spi, Long.MIN_VALUE,
                                                         nonCoordinatorSites);

        final long siteId0 = getHSIdForES(0);
        Mailbox m0 = m_mboxes.get(siteId0);
        ExecutionSite es0 = m_sites.get(siteId0);
        MultiPartitionParticipantTxnState ts =
            new MultiPartitionParticipantTxnState(m0, es0, mn);

        // fail middle and last site
        HashSet<Long> failedSites = new HashSet<Long>();
        failedSites.add(getHSIdForES(1));
        failedSites.add(getHSIdForES(2));
        failedSites.add(getHSIdForES(3));
        failedSites.add(getHSIdForES(5));
        ts.handleSiteFaults(failedSites);

        // peek at some internals
        long[] nonCoordinatingSites = ts.getNonCoordinatingSites();
        assertEquals(4, nonCoordinatingSites.length);
        assertEquals(getHSIdForES(8), nonCoordinatingSites[3]);
        assertEquals(getHSIdForES(7), nonCoordinatingSites[2]);
        assertEquals(getHSIdForES(6), nonCoordinatingSites[1]);
        assertEquals(getHSIdForES(4), nonCoordinatingSites[0]);

        // fail first site
        ts = new MultiPartitionParticipantTxnState(m0, es0, mn);
        failedSites.clear();
        failedSites.add(getHSIdForES(1));
        ts.handleSiteFaults(failedSites);

        nonCoordinatingSites = ts.getNonCoordinatingSites();
        assertEquals(7, nonCoordinatingSites.length);
        assertEquals(getHSIdForES(8), nonCoordinatingSites[6]);
        assertEquals(getHSIdForES(7), nonCoordinatingSites[5]);
        assertEquals(getHSIdForES(6), nonCoordinatingSites[4]);
        assertEquals(getHSIdForES(4), nonCoordinatingSites[2]);
        assertEquals(getHSIdForES(3), nonCoordinatingSites[1]);
        assertEquals(getHSIdForES(2), nonCoordinatingSites[0]);

        // fail site that isn't a non-coordinator site
        ts = new MultiPartitionParticipantTxnState(m0, es0, mn);
        failedSites.clear();
        failedSites.add(getHSIdForES(9));
        failedSites.add(getHSIdForES(10));
        ts.handleSiteFaults(failedSites);

        nonCoordinatingSites = ts.getNonCoordinatingSites();
        assertEquals(8, nonCoordinatingSites.length);
        assertEquals(getHSIdForES(8), nonCoordinatingSites[7]);
        assertEquals(getHSIdForES(7), nonCoordinatingSites[6]);
        assertEquals(getHSIdForES(6), nonCoordinatingSites[5]);
        assertEquals(getHSIdForES(5), nonCoordinatingSites[4]);
        assertEquals(getHSIdForES(3), nonCoordinatingSites[2]);
        assertEquals(getHSIdForES(2), nonCoordinatingSites[1]);
        assertEquals(getHSIdForES(1), nonCoordinatingSites[0]);
    }

    /*
     * Show that a multi-partition transaction proceeds if one of the participants
     * fails
     */
    public void testFailedMultiPartitionParticipant() throws Exception {
        tearDown();
        start( 2, 2, 0);
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
                                    readOnly, singlePartition, tx1_spi, Long.MAX_VALUE,
                                    new long[] {getHSIdForES(1)});

        final long siteId0 = getHSIdForES(0);
        final Mailbox m0 = m_mboxes.get(siteId0);
        final ExecutionSite es0 = m_sites.get(siteId0);
        es0.lastKnownGloballyCommitedMultiPartTxnId = DtxnConstants.DUMMY_LAST_SEEN_TXN_ID + 1;
        final MultiPartitionParticipantTxnState tx1_1 =
            new MultiPartitionParticipantTxnState(m0, es0, tx1_mn_1);

        // Site 2 won't exist; we'll claim it fails.

        // pre-conditions
        int callcheck = MockMPProcedureRunner.m_called;
        assertFalse(tx1_1.isDone());
        es0.m_transactionsById.put(tx1_1.txnId, tx1_1);

        // execute transaction
        es1 = new Thread(new Runnable() {
            @Override
            public void run() {es0.recursableRun(tx1_1);}});
        es1.start();

        m_voltdb.killSite(getHSIdForES(1));
        m_voltdb.killSite(getInitiatorIds()[1]);
        m_voltdb.getFaultDistributor().reportFault(
                new SiteFailureFault(
                        Arrays.asList(new Long[] { getInitiatorIds()[1], getHSIdForES(1) })));
        es1.join();

        // post-conditions
        assertTrue(tx1_1.isDone());
        assertFalse(tx1_1.needsRollback());
        assertEquals(null, es0.m_transactionsById.get(tx1_1.txnId));
        assertEquals((++callcheck), MockMPProcedureRunner.m_called);
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
            long initiator_id,
            int partition_id)
    {
        final StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
        spi.setParams("commit", new Integer(partition_id));

        List<Long> sitesForPartition = getSiteIdsForPartitionId(partition_id);
        for (long i : sitesForPartition) {
            final InitiateTaskMessage itm =
                new InitiateTaskMessage(initiator_id,
                                        i,             // each site is its own coordinator
                                        txn_id,
                                        readOnly,
                                        true,          // single partition
                                        spi,
                                        safe_txn_id);  // last safe txnid
            itm.m_sourceHSId = initiator_id;
            m_mboxes.get(i).deliver(itm);
        }
    }

    /**
     * Create a multiple partition transaction
     */
    private void createMPInitiation(
            boolean rollback,
            boolean rollback_all,
            boolean readOnly,
            int numberOfBatches,
            long txn_id,
            long safe_txn_id,
            long initiator_id,
            int partition_id,
            long coordinator_id,
            List<Long> participants)
    {
        ArrayList<Object> params = new ArrayList<Object>();
        params.add("number_of_batches");
        params.add(new Integer(numberOfBatches));
        final StoredProcedureInvocation spi = new StoredProcedureInvocation();
        if (!rollback) {
            spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedure");
            params.add("txn_outcome");
            params.add("commit");
            params.add(new Integer(partition_id));
            spi.setParams(params.toArray());
        }
        else {
            if (rollback_all)
            {
                spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedureRollbackParticipant");
                params.add("txn_outcome");
                params.add("rollback_all");
                params.add(new Integer(partition_id));
                spi.setParams(params.toArray());
            }
            else
            {
                spi.setProcName("org.voltdb.TestExecutionSite$MockMPVoltProcedureRollbackParticipant");
                params.add("txn_outcome");
                params.add("rollback_random");
                params.add(new Integer(partition_id));
                spi.setParams(params.toArray());
            }
        }

        testLog.info("Creating MP proc, TXN ID: " + txn_id + ", participants: " + participants.toString());

        assert(participants.size() == (SITE_COUNT - 1));
        final InitiateTaskMessage itm =
            new InitiateTaskMessage(initiator_id,
                                    coordinator_id,
                                    txn_id,
                                    readOnly,
                                    false,         // multi-partition
                                    spi,
                                    safe_txn_id,  // last safe txnid
                                    ArrayUtils.toPrimitive(participants.toArray(new Long[0])));
        itm.m_sourceHSId = initiator_id;
        m_mboxes.get(coordinator_id).deliver(itm);

        for (long participant : participants) {
            final MultiPartitionParticipantMessage mppm =
                new MultiPartitionParticipantMessage
                (initiator_id, coordinator_id, txn_id, readOnly);
            mppm.m_sourceHSId = initiator_id;
            m_mboxes.get(participant).deliver(mppm);
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
            long initiator_id)
    {
        HeartbeatMessage hbm =
            new HeartbeatMessage(initiator_id, txn_id, safe_txn_id);
        hbm.m_sourceHSId = initiator_id;
        for (Mailbox m : m_mboxes.values()) {
            m.deliver(hbm);
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
            long initiator = selectRandomInitiator(rand);
            int partition = i % PARTITION_COUNT;

            int wheelOfDestiny = rand.nextInt(100);
            if (i == totalTransactions) {
                testLog.info("Queueing final heartbeat.");
                int offset = 0;
                for (long inid : getInitiatorIds()) {
                    createHeartBeat(txnid + offset, txnid + offset, inid);
                    ++offset;
                }
            }
            else if (wheelOfDestiny < 50) {
                createSPInitiation(readOnly, txnid, safe_txnid, initiator, partition);
            }
            else if (wheelOfDestiny < 70) {
                int numberOfBatches = rand.nextInt(4) + 1;
                List<Long> participants = new ArrayList<Long>();
                long coordinator = selectCoordinatorAndParticipants(rand, partition, initiator, participants);
                createMPInitiation(rollback, rollback_all, readOnly, numberOfBatches,
                                   txnid, safe_txnid, initiator, partition,
                                   coordinator, participants);
            }
            else {
                createHeartBeat(txnid, safe_txnid, initiator);
            }
        }
    }



    private void createAndRunSiteThreads() {
        createAndRunSiteThreads(false);
    }

    /*
     * Run the mailboxes / sites to completion.
     */
    private void createAndRunSiteThreads(final boolean loopUntilPoison)
    {
        for (int i=0; i < SITE_COUNT; ++i) {
            final long site_id = getHSIdForES(i);
            m_siteThreads.put(site_id, new Thread(new Runnable() {
               @Override
               public void run() {
                   m_sites.get(site_id).runLoop(loopUntilPoison);
               }
            }, "Site: " + CoreUtils.hsIdToString(site_id)));
        }

        for (int i=0; i < SITE_COUNT; ++i) {
            m_siteThreads.get(getHSIdForES(i)).start();
        }
    }

    /*
     * Create one txn for each initiator. Both initiators will fail concurrently.
     * The first transaction will not be fully replicated (only exists at 1).
     * The second transaction from a different initiator
     * will be fully replicated, and will have a higher transaction id.
     * When failure agreement runs for both failures at once,
     * if the bug exists you should see the transaction from the second failed initiator cause the partially
     * initiated transaction from the first to be executed at a subset of surviving replicas sites.
     */
    public void testENG1617() throws Exception {
        System.out.println("Starting testENG1617");
        start(SITE_COUNT, PARTITION_COUNT, K_FACTOR);

        for (RussianRouletteMailbox m : m_mboxes.values()) {
             m.setFailureLikelihood(0);
        }
        createAndRunSiteThreads(true);

        /*
         * These are the sites that will receive the txns from the two concurrently dieing initiators
         */
        List<Long> involvedSites1 = getSiteIdsForPartitionId(0);

        /*
         * Will use these sites to find initiators to kill
         */
        List<Long> involvedSites2 = getSiteIdsForPartitionId(1);

        //This initiator will initiate the txn with the lower id that is partially replicated to just one site
        int initiatorToDie1 = getInitiatorIdForSiteId(CoreUtils.getSiteIdFromHSId(involvedSites2.get(0)));

        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
        spi.setParams("commit", new Integer(0));

        InitiateTaskMessage itm =
                new InitiateTaskMessage(getInitiatorIds()[initiatorToDie1 / 1000],
                                        involvedSites1.get(2),             // each site is its own coordinator
                                        getHSIdForES(1),
                                        false,
                                        true,          // single partition
                                        spi,
                                        0);  // last safe txnid
        itm.m_sourceHSId = getInitiatorIds()[initiatorToDie1 / 1000];
        m_mboxes.get(involvedSites1.get(2)).deliver(itm);

        //This initiator will initiate the txn with the higher txn id that is fully replicated
        int initiatorToDie2 = getInitiatorIdForSiteId(CoreUtils.getSiteIdFromHSId(involvedSites2.get(1)));

        for (int ii = 0; ii < 3; ii++) {
            spi = new StoredProcedureInvocation();
            spi.setProcName("org.voltdb.TestExecutionSite$MockSPVoltProcedure");
            spi.setParams("commit", new Integer(0));

            itm =
                    new InitiateTaskMessage(getInitiatorIds()[initiatorToDie2 / 1000],
                                            involvedSites1.get(ii),             // each site is its own coordinator
                                            getHSIdForES(3),
                                            false,
                                            true,          // single partition
                                            spi,
                                            2);  // last safe txnid
            itm.m_sourceHSId = getInitiatorIds()[initiatorToDie2 / 1000];
            m_mboxes.get(involvedSites1.get(ii)).deliver(itm);
        }

        LocalObjectMessage lom = new LocalObjectMessage(new Runnable() {
            @Override
            public void run() {
                throw new Error();
            }
        });
        lom.m_sourceHSId = involvedSites2.get(0);
        /*
         * Kill the two initiators
         */
        m_mboxes.get(involvedSites2.get(0)).deliver(lom);
        lom = new LocalObjectMessage(new Runnable() {
            @Override
            public void run() {
                throw new Error();
            }
        });
        lom.m_sourceHSId = involvedSites2.get(1);
        m_mboxes.get(involvedSites2.get(1)).deliver(lom);

        m_siteThreads.get(involvedSites2.get(0)).join();
        m_siteThreads.get(involvedSites2.get(1)).join();

        m_sites.get(involvedSites2.get(0)).shutdown();
        m_voltdb.killSite(involvedSites2.get(0));
        m_voltdb.getFaultDistributor().reportFault(
                new SiteFailureFault(
                        Arrays.asList( new Long[] {
                                CoreUtils.getHSIdFromHostAndSite(
                                        involvedSites2.get(0).intValue(),
                                        (int)(involvedSites2.get(0).longValue() >> 32) + 1000),
                                        involvedSites2.get(0)})));
        // remove this site from the postoffice
        postoffice.remove(involvedSites2.get(0));

        m_sites.get(involvedSites2.get(1)).shutdown();
        m_voltdb.killSite(involvedSites2.get(1));
        m_voltdb.getFaultDistributor().reportFault(
                new SiteFailureFault(
                        Arrays.asList(new Long[]{
                                CoreUtils.getHSIdFromHostAndSite(
                                        involvedSites2.get(1).intValue(),
                                        (int)(involvedSites2.get(1).longValue() >> 32) + 1000),
                                        involvedSites2.get(1)})));
        // remove this site from the postoffice
        postoffice.remove(involvedSites2.get(1));

        Thread.sleep(200);
        /*
         * Spin for a while giving them a chance to process all the failures.
         */
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start > 15000) {
            boolean containsBadValue = false;
            for (ExecutionSite site : m_sites.values()) {
                if (site != null) {
                    if (site.m_transactionsById.containsKey(1L) ||
                            site.m_transactionsById.containsKey(3L)) {
                        containsBadValue = true;
                    }
                }
            }
            if (!containsBadValue) {
                break;
            } else {
                Thread.sleep(100);
            }
        }

        for (RussianRouletteMailbox mailbox : m_mboxes.values()) {
            if (mailbox != null) {
                lom = new LocalObjectMessage(new Runnable() {
                    @Override
                    public void run() {
                        throw new Error();
                    }
                });
                lom.m_sourceHSId = mailbox.getHSId();
                mailbox.deliver(lom);
            }
        }

        for (Thread t : m_siteThreads.values()) {
            t.join();
        }

        /*
         * Txn 1 should have been dropped because it was partially replicated when the initiator failed.
         * In the old code it would have been marked as safely replicated due to txn 2 from the other initiator
         * that failed concurrently.
         */
        for (ExecutionSite es : m_sites.values()) {
            if (es != null) {
                if (es.m_transactionsById.containsKey(1L)) {
                    System.out.println("Site " + es.getCorrespondingSiteId() + " contains txn 1");
                }
                assertFalse(es.m_transactionsById.containsKey(1L));
                if (es.m_transactionsById.containsKey(3L)) {
                    System.out.println("Site " + es.getCorrespondingSiteId() + " contains txn 3");
                }
                assertFalse(es.m_transactionsById.containsKey(3L));
            }
        }
    }

}
