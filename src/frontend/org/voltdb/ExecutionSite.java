/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.debugstate.ExecutorContext;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.dtxn.MultiPartitionParticipantTxnState;
import org.voltdb.dtxn.RestrictedPriorityQueue;
import org.voltdb.dtxn.SinglePartitionTxnState;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.SiteTransactionConnection;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.RestrictedPriorityQueue.QueueState;
import org.voltdb.elt.ELTManager;
import org.voltdb.elt.ELTProtoMessage;
import org.voltdb.elt.processors.RawProcessor;
import org.voltdb.elt.processors.RawProcessor.ELTInternalMessage;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.messaging.DebugMessage;
import org.voltdb.messaging.FailureSiteUpdateMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.HeartbeatMessage;
import org.voltdb.messaging.HeartbeatResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.MessagingException;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.Subject;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite
implements Runnable, DumpManager.Dumpable, SiteTransactionConnection, SiteProcedureConnection
{
    private Logger m_txnlog;
    private Logger m_recoveryLog = Logger.getLogger("RECOVERY", VoltLoggerFactory.instance());
    private static final Logger log = Logger.getLogger(ExecutionSite.class.getName(), VoltLoggerFactory.instance());
    private static final Logger hostLog = Logger.getLogger("HOST", VoltLoggerFactory.instance());
    private static AtomicInteger siteIndexCounter = new AtomicInteger(0);
    private final int siteIndex = siteIndexCounter.getAndIncrement();
    private final ExecutionSiteNodeFailureFaultHandler m_faultHandler =
        new ExecutionSiteNodeFailureFaultHandler();

    final HashMap<String, VoltProcedure> procs = new HashMap<String, VoltProcedure>(16, (float) .1);
    private final Mailbox m_mailbox;
    final ExecutionEngine ee;
    final HsqlBackend hsql;
    public volatile boolean m_shouldContinue = true;

    // Catalog
    public CatalogContext m_context;
    Site getCatalogSite() {
        return m_context.cluster.getSites().get(Integer.toString(getSiteId()));
    }

    final int m_siteId;
    public final int getSiteId() {
        return m_siteId;
    }

    HashMap<Long, TransactionState> m_transactionsById = new HashMap<Long, TransactionState>();
    private final RestrictedPriorityQueue m_transactionQueue;

    // The time in ms since epoch of the last call to tick()
    long lastTickTime = 0;
    long lastCommittedTxnId = 0;
    // The transaction ID of the last committed multi-partition transaction is
    // tracked so that participants can determine whether or not to commit or
    // roll back if the coordinator fails while notifying each participant of
    // the final outcome for the txn.
    long lastCommittedMultiPartTxnId = 0;

    public final static long kInvalidUndoToken = -1L;
    private long latestUndoToken = 0L;

    public long getNextUndoToken() {
        return ++latestUndoToken;
    }

    private final HashSet<Long> faultedTxns = new HashSet<Long>();

    // store the id used by the DumpManager to identify this execution site
    public final String m_dumpId;
    public long m_currentDumpTimestamp = 0;

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private final SnapshotSiteProcessor m_snapshotter;

    // Trigger if shutdown has been run already.
    private boolean haveShutdownAlready;

    private final TableStats m_tableStats;
    private final StarvationTracker m_starvationTracker;
    private final Watchdog m_watchdog;
    private class Watchdog extends Thread {
        private volatile boolean m_shouldContinue = true;
        private volatile boolean m_petted = false;
        private final int m_siteIndex;
        private final int m_siteId;
        private Thread m_watchThread = null;
        public Watchdog(final int siteIndex, final int siteId) {
            super(null, null, "ExecutionSite " + siteIndex + " siteId: " + siteId + " watchdog ", 262144);
            m_siteIndex = siteIndex;
            m_siteId = siteId;
        }

        public void pet() {
            m_petted = true;
        }

        @Override
        public void run() {
            if (m_watchThread == null) {
                throw new RuntimeException("Use start(Thread watchThread) not Thread.start()");
            }
            try {
                Thread.sleep(30000);
            } catch (final InterruptedException e) {
                return;
            }
            while (m_shouldContinue) {
                try {
                    Thread.sleep(5000);
                } catch (final InterruptedException e) {
                    return;
                }
                if (!m_petted) {
                    final StackTraceElement trace[] = m_watchThread.getStackTrace();
                    final Throwable throwable = new Throwable();
                    throwable.setStackTrace(trace);
                    log.l7dlog( Level.WARN, LogKeys.org_voltdb_ExecutionSite_Watchdog_possibleHang.name(), new Object[]{ m_siteIndex, m_siteId}, throwable);
                }
                m_petted = false;
            }
        }

        @Override
        public void start() {
            throw new UnsupportedOperationException("Use start(Thread watchThread)");
        }

        public void start(final Thread thread) {
            m_watchThread = thread;
            super.start();
        }
    }

    // This message is used locally to schedule a node failure event's
    // required  processing at an execution site.
    static class ExecutionSiteNodeFailureMessage extends VoltMessage
    {
        final HashSet<NodeFailureFault> m_failedHosts;
        ExecutionSiteNodeFailureMessage(HashSet<NodeFailureFault> failedHosts) {
            m_failedHosts = failedHosts;
        }

        @Override
        protected void flattenToBuffer(DBBPool pool) {} // can be empty if only used locally
        @Override
        protected void initFromBuffer() {} // can be empty if only used locally

        @Override
        public byte getSubject() {
            return Subject.FAILURE.getId();
        }
    }

    // This message is used locally to get the currently active TransactionState
    // to check whether or not its WorkUnit's dependencies have been satisfied.
    // Necessary after handling a node failure.
    static class CheckTxnStateCompletionMessage extends VoltMessage
    {
        final long m_txnId;
        CheckTxnStateCompletionMessage(long txnId)
        {
            m_txnId = txnId;
        }

        @Override
        protected void flattenToBuffer(DBBPool pool) {} // can be empty if only used locally
        @Override
        protected void initFromBuffer() {} // can be empty if only used locally
    }

    private class ExecutionSiteNodeFailureFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            if (m_shouldContinue == false) {
                return;
            }

            HashSet<NodeFailureFault> failedNodes = new HashSet<NodeFailureFault>();
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault)
                {
                    NodeFailureFault node_fault = (NodeFailureFault)fault;
                    failedNodes.add(node_fault);
                } else {
                    VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
                }
            }
            if (!failedNodes.isEmpty()) {
                m_mailbox.deliver(new ExecutionSiteNodeFailureMessage(failedNodes));
            }
        }
    }

    private final HashMap<Long, VoltSystemProcedure> m_registeredSysProcPlanFragments =
        new HashMap<Long, VoltSystemProcedure>();


    /**
     * Log settings changed. Signal EE to update log level.
     */
    public void updateBackendLogLevels() {
        ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
    }

    void startShutdown() {
        m_shouldContinue = false;
    }

    /**
     * Shutdown all resources that need to be shutdown for this <code>ExecutionSite</code>.
     * May be called twice if recursing via recursableRun(). Protected against that..
     */
    public void shutdown() {
        if (haveShutdownAlready) {
            return;
        }
        haveShutdownAlready = true;
        m_shouldContinue = false;

        boolean finished = false;
        while (!finished) {
            try {
                if (m_watchdog.isAlive()) {
                    m_watchdog.m_shouldContinue = false;
                    m_watchdog.interrupt();
                    m_watchdog.join();
                }

                m_transactionQueue.shutdown();

                ProcedureProfiler.flushProfile();
                if (hsql != null) {
                    hsql.shutdown();
                }
                if (ee != null) {
                    ee.release();
                }
                finished = true;
            } catch (final InterruptedException e) {
                //Ignore interruptions and finish shutting down.
            }
        }

        m_snapshotter.shutdown();
    }



    public void tick() {
        // invoke native ee tick if at least one second has passed
        final long time = EstTime.currentTimeMillis();
        if ((time - lastTickTime) >= 1000) {
            if ((lastTickTime != 0) && (ee != null)) {
                ee.tick(time, lastCommittedTxnId);
            }
            lastTickTime = time;
        }

        // do other periodic work
        m_snapshotter.doSnapshotWork(ee);
        m_watchdog.pet();

        /*
         * grab the table statistics from ee and put it into the statistics
         * agent if at least 1/3 of the statistics broadcast interval has past.
         * This ensures that when the statistics are broadcasted, they are
         * relatively up-to-date.
         */
        if (m_tableStats != null
                && (time - lastTickTime) >= StatsManager.POLL_INTERVAL * 2) {
            CatalogMap<Table> tables = m_context.database.getTables();
            int[] tableIds = new int[tables.size()];
            int i = 0;
            for (Table table : tables)
                tableIds[i++] = table.getRelativeIndex();
            final VoltTable[] s = ee.getStats(SysProcSelector.TABLE,
                                              tableIds,
                                              false,
                                              System.currentTimeMillis());
            if (s != null)
                m_tableStats.setStatsTable(s[0]);
        }
    }


    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     */
    public interface SystemProcedureExecutionContext {
        public Database getDatabase();
        public Cluster getCluster();
        public Site getSite();
        public ExecutionEngine getExecutionEngine();
        public long getLastCommittedTxnId();
        public long getNextUndo();
        public ExecutionSite getExecutionSite();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        public Database getDatabase()               { return m_context.database; }
        public Cluster getCluster()                 { return m_context.cluster; }
        public Site getSite()                       { return getCatalogSite(); }
        public ExecutionEngine getExecutionEngine() { return ee; }
        public long getLastCommittedTxnId()         { return lastCommittedTxnId; }
        public long getNextUndo()                   { return getNextUndoToken(); }
        public ExecutionSite getExecutionSite()     { return ExecutionSite.this; }
    }

    SystemProcedureContext m_systemProcedureContext;

    /**
     * Dummy ExecutionSite useful to some tests that require Mock/Do-Nothing sites.
     * @param siteId
     */
    ExecutionSite(int siteId) {
        m_siteId = siteId;
        m_systemProcedureContext = new SystemProcedureContext();
        m_watchdog = null;
        ee = null;
        hsql = null;
        m_dumpId = "MockExecSite";
        m_snapshotter = null;
        m_mailbox = null;
        m_transactionQueue = null;
        m_starvationTracker = null;
        m_tableStats = null;
    }

    ExecutionSite(VoltDBInterface voltdb, Mailbox mailbox, final int siteId)
    {
        this(voltdb, mailbox, siteId, null, null);
    }

    ExecutionSite(VoltDBInterface voltdb, Mailbox mailbox,
                  final int siteId, String serializedCatalog,
                  RestrictedPriorityQueue transactionQueue)
    {
        m_siteId = siteId;
        String txnlog_name = ExecutionSite.class.getName() + "." + m_siteId;
        m_txnlog =
            Logger.getLogger(txnlog_name, VoltLoggerFactory.instance());

        hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_Initializing.name(),
                        new Object[] { String.valueOf(siteId) }, null);

        m_context = voltdb.getCatalogContext();

        VoltDB.instance().getFaultDistributor().
        registerFaultHandler(FaultType.NODE_FAILURE,
                             m_faultHandler,
                             NodeFailureFault.NODE_FAILURE_EXECUTION_SITE);

        if (voltdb.getBackendTargetType() == BackendTarget.NONE) {
            ee = new MockExecutionEngine();
            hsql = null;
        }
        else if (voltdb.getBackendTargetType() == BackendTarget.HSQLDB_BACKEND) {
            hsql = initializeHSQLBackend();
            ee = new MockExecutionEngine();
        }
        else {
            if (serializedCatalog == null) {
                serializedCatalog = voltdb.getCatalogContext().catalog.serialize();
            }
            hsql = null;
            ee = initializeEE(voltdb.getBackendTargetType(), serializedCatalog);
        }

        // Should pass in the watchdog class to allow sleepy dogs..
        m_watchdog = new Watchdog(siteId, siteIndex);

        m_dumpId = "ExecutionSite." + String.valueOf(getSiteId());
        DumpManager.register(m_dumpId, this);

        m_systemProcedureContext = new SystemProcedureContext();
        m_mailbox = mailbox;

        // allow dependency injection of the transaction queue implementation
        m_transactionQueue =
            (transactionQueue != null) ? transactionQueue : initializeTransactionQueue(siteId);

        loadProceduresFromCatalog(voltdb.getBackendTargetType());
        m_snapshotter = new SnapshotSiteProcessor();
        final StatsAgent statsAgent = VoltDB.instance().getStatsAgent();
        m_starvationTracker = new StarvationTracker(String.valueOf(getCorrespondingSiteId()), getCorrespondingSiteId());
        statsAgent.registerStatsSource(SysProcSelector.STARVATION,
                                       Integer.parseInt(getCorrespondingCatalogSite().getTypeName()),
                                       m_starvationTracker);
        m_tableStats = new TableStats(String.valueOf(getCorrespondingSiteId()), getCorrespondingSiteId());
        statsAgent.registerStatsSource(SysProcSelector.TABLE,
                                       Integer.parseInt(getCorrespondingCatalogSite().getTypeName()),
                                       m_tableStats);

    }

    private RestrictedPriorityQueue initializeTransactionQueue(final int siteId)
    {
        // build an array of all the initiators
        int initiatorCount = 0;
        for (final Site s : m_context.siteTracker.getUpSites())
            if (s.getIsexec() == false)
                initiatorCount++;
        final int[] initiatorIds = new int[initiatorCount];
        int index = 0;
        for (final Site s : m_context.siteTracker.getUpSites())
            if (s.getIsexec() == false)
                initiatorIds[index++] = Integer.parseInt(s.getTypeName());

        assert(m_mailbox != null);
        return new RestrictedPriorityQueue(initiatorIds, siteId, m_mailbox);
    }

    private HsqlBackend initializeHSQLBackend()
    {
        HsqlBackend hsqlTemp = null;
        try {
            hsqlTemp = new HsqlBackend(getSiteId());
            final String hexDDL = m_context.database.getSchema();
            final String ddl = Encoder.hexDecodeToString(hexDDL);
            final String[] commands = ddl.split(";");
            for (String command : commands) {
                command = command.trim();
                if (command.length() == 0) {
                    continue;
                }
                hsqlTemp.runDDL(command);
            }
        }
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { getSiteId(), siteIndex }, ex);
            VoltDB.crashVoltDB();
        }
        return hsqlTemp;
    }

    private ExecutionEngine
    initializeEE(BackendTarget target, String serializedCatalog)
    {
        String hostname = ConnectionUtil.getHostnameOrAddress();

        ExecutionEngine eeTemp = null;
        try {
            if (target == BackendTarget.NATIVE_EE_JNI) {
                Site site = getCatalogSite();
                eeTemp =
                    new ExecutionEngineJNI(
                        this,
                        m_context.cluster.getRelativeIndex(),
                        getSiteId(),
                        Integer.valueOf(site.getPartition().getTypeName()),
                        Integer.valueOf(site.getHost().getTypeName()),
                        hostname);
                eeTemp.loadCatalog(serializedCatalog);
                lastTickTime = EstTime.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
            }
            else {
                // set up the EE over IPC
                Site site = getCatalogSite();
                eeTemp =
                    new ExecutionEngineIPC(
                            this,
                            m_context.cluster.getRelativeIndex(),
                            getSiteId(),
                            Integer.valueOf(site.getPartition().getTypeName()),
                            Integer.valueOf(site.getHost().getTypeName()),
                            hostname,
                            target);
                eeTemp.loadCatalog(serializedCatalog);
                lastTickTime = EstTime.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
            }
        }
        // just print error info an bail if we run into an error here
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { getSiteId(), siteIndex }, ex);
            VoltDB.crashVoltDB();
        }
        return eeTemp;
    }

    public boolean updateCatalog(String catalogDiffCommands) {
        m_context = VoltDB.instance().getCatalogContext();
        loadProceduresFromCatalog(VoltDB.getEEBackendType());
        ee.updateCatalog(catalogDiffCommands);
        return true;
    }

    void loadProceduresFromCatalog(BackendTarget backendTarget) {
        m_registeredSysProcPlanFragments.clear();
        procs.clear();
        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = m_context.database.getProcedures();
        for (final Procedure proc : catalogProcedures) {
            VoltProcedure wrapper = null;
            if (proc.getHasjava()) {
                final String className = proc.getClassname();
                Class<?> procClass = null;
                try {
                    procClass = m_context.classForProcedure(className);
                }
                catch (final ClassNotFoundException e) {
                    hostLog.l7dlog(
                            Level.WARN,
                            LogKeys.host_ExecutionSite_GenericException.name(),
                            new Object[] { getSiteId(), siteIndex },
                            e);
                    VoltDB.crashVoltDB();
                }
                try {
                    wrapper = (VoltProcedure) procClass.newInstance();
                }
                catch (final InstantiationException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { getSiteId(), siteIndex }, e);
                }
                catch (final IllegalAccessException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(),
                                    new Object[] { getSiteId(), siteIndex }, e);
                }
            }
            else {
                wrapper = new VoltProcedure.StmtProcedure();
            }

            wrapper.init(m_context.cluster.getPartitions().size(),
                         this, proc, backendTarget, hsql, m_context.cluster);
            procs.put(proc.getTypeName(), wrapper);
        }
    }

    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        // enumerate site id (pad to 4 digits for sort)
        String name = "ExecutionSite:";
        if (getSiteId() < 10) name += "0";
        if (getSiteId() < 100) name += "0";
        if (getSiteId() < 1000) name += "0";
        name += String.valueOf(getSiteId());
        Thread.currentThread().setName(name);

        NDC.push("ExecutionSite - " + getSiteId() + " index " + siteIndex);
        if (VoltDB.getUseWatchdogs()) {
            m_watchdog.start(Thread.currentThread());
        }
        if (VoltDB.getUseThreadAffinity()) {
            final boolean startingAffinity[] = org.voltdb.utils.ThreadUtils.getThreadAffinity();
            for (int ii = 0; ii < startingAffinity.length; ii++) {
                log.l7dlog( Level.INFO, LogKeys.org_voltdb_ExecutionSite_StartingThreadAffinity.name(), new Object[] { startingAffinity[ii] }, null);
                startingAffinity[ii] = false;
            }
            startingAffinity[ siteIndex % startingAffinity.length] = true;
            org.voltdb.utils.ThreadUtils.setThreadAffinity(startingAffinity);
            final boolean endingAffinity[] = org.voltdb.utils.ThreadUtils.getThreadAffinity();
            for (int ii = 0; ii < endingAffinity.length; ii++) {
                log.l7dlog( Level.INFO, LogKeys.org_voltdb_ExecutionSite_EndingThreadAffinity.name(), new Object[] { endingAffinity[ii] }, null);
                startingAffinity[ii] = false;
            }
        }
        try {
            // Only poll messaging layer if necessary. Allow the poll
            // to block if the execution site is truly idle.
            while (m_shouldContinue) {
                TransactionState currentTxnState = m_transactionQueue.poll();
                if (currentTxnState == null) {
                    // poll the messaging layer for a while as this site has nothing to do
                    // this will likely have a message/several messages immediately in a heavy workload
                    // Before blocking record the starvation
                    VoltMessage message = m_mailbox.recv();
                    if (message == null) {
                        m_starvationTracker.beginStarvation();
                        message = m_mailbox.recvBlocking(5);
                    }
                    // do periodic work
                    tick();
                    if (message != null) {
                        m_starvationTracker.endStarvation();
                        handleMailboxMessage(message);
                    }
                }
                if (currentTxnState != null) {
                    assert(!faultedTxns.contains(currentTxnState.txnId));
                    recursableRun(currentTxnState);
                }
            }
        }
        catch (final RuntimeException e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_RuntimeException.name(), e);
            throw e;
        }
        shutdown();
    }

    /**
     * Run the execution site execution loop, for tests currently.
     * Will integrate this in to the real run loop soon.. ish.
     */
    public void runLoop() {
        while (m_shouldContinue) {
            TransactionState currentTxnState = m_transactionQueue.poll();
            if (currentTxnState == null) {
                // poll the messaging layer for a while as this site has nothing to do
                // this will likely have a message/several messages immediately in a heavy workload
                VoltMessage message = m_mailbox.recv();
                tick();
                if (message != null) {
                    handleMailboxMessage(message);
                }
                else {
                    // Terminate run loop on empty mailbox AND no currentTxnState
                    return;
                }
            }
            if (currentTxnState != null) {
                assert(!faultedTxns.contains(currentTxnState.txnId));
                //System.out.println("ExecutionSite " + getSiteId() + " running txnid " + currentTxnState.txnId);
                recursableRun(currentTxnState);
            }
        }
    }

    private void completeTransaction(TransactionState txnState) {
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST completeTransaction " + txnState.txnId);
        }
        if (!txnState.isReadOnly) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(txnState.getBeginUndoToken() != kInvalidUndoToken);
            assert(latestUndoToken >= txnState.getBeginUndoToken());

            // release everything through the end of the current window.
            if (latestUndoToken > txnState.getBeginUndoToken()) {
                ee.releaseUndoToken(latestUndoToken);
            }

            // reset for error checking purposes
            txnState.setBeginUndoToken(kInvalidUndoToken);
        }

        // advance the committed transaction point. Necessary for both ELT
        // commit tracking and for fault detection transaction partial-transaction
        // resolution.
        if (!txnState.needsRollback())
        {
            if (txnState.txnId > lastCommittedTxnId) {
                lastCommittedTxnId = txnState.txnId;
                if (!txnState.isSinglePartition())
                {
                    lastCommittedMultiPartTxnId = txnState.txnId;
                }
            }
        }
    }

    private void handleMailboxMessage(VoltMessage message)
    {
        if (message instanceof TransactionInfoBaseMessage) {
            TransactionInfoBaseMessage info = (TransactionInfoBaseMessage)message;
            assertTxnIdOrdering(info);

            // Special case heartbeats which only update RPQ
            if (info instanceof HeartbeatMessage) {
                // use the heartbeat to unclog the priority queue if clogged
                long lastSeenTxnFromInitiator = m_transactionQueue.noteTransactionRecievedAndReturnLastSeen(
                        info.getInitiatorSiteId(), info.getTxnId(),
                        true, ((HeartbeatMessage) info).getLastSafeTxnId());

                // respond to the initiator with the last seen transaction
                HeartbeatResponseMessage response = new HeartbeatResponseMessage(
                        m_siteId, lastSeenTxnFromInitiator,
                        m_transactionQueue.getQueueState() == QueueState.BLOCKED_SAFETY);
                try {
                    m_mailbox.send(info.getInitiatorSiteId(), VoltDB.DTXN_MAILBOX_ID, response);
                } catch (MessagingException e) {
                    // hope this never happens... it doesn't right?
                    throw new RuntimeException(e);
                }
                // we're done here (in the case of heartbeats)
                return;
            }
            else if (info instanceof InitiateTaskMessage) {
                m_transactionQueue.noteTransactionRecievedAndReturnLastSeen(info.getInitiatorSiteId(),
                                                  info.getTxnId(),
                                                  false,
                                                  ((InitiateTaskMessage) info).getLastSafeTxnId());
            }
            // FragmentTasks aren't sent by initiators and shouldn't update
            // transaction queue initiator states.
            else if (info instanceof MultiPartitionParticipantMessage) {
                m_transactionQueue.noteTransactionRecievedAndReturnLastSeen(info.getInitiatorSiteId(),
                                                  info.getTxnId(),
                                                  false,
                                                  DtxnConstants.DUMMY_LAST_SEEN_TXN_ID);
            }
            else {
                assert(info instanceof FragmentTaskMessage);
            }

            // Every non-heartbeat notice requires a transaction state.
            TransactionState ts = m_transactionsById.get(info.getTxnId());

            // Check for a rollback FragmentTask.  Would eventually like to
            // replace the overloading of FragmentTask with a separate
            // TransactionCompletionMessage (or something similarly named)
            boolean isRollback = false;
            if (message instanceof FragmentTaskMessage)
            {
                isRollback = ((FragmentTaskMessage) message).shouldUndo();
            }
            // don't create a new transaction state
            if (ts == null && !isRollback) {
                if (info.isSinglePartition()) {
                    ts = new SinglePartitionTxnState(m_mailbox, this, info);
                }
                else {
                    ts = new MultiPartitionParticipantTxnState(m_mailbox, this, info);
                }
                m_transactionQueue.add(ts);
                m_transactionsById.put(ts.txnId, ts);
            }

            if (ts != null)
            {
                if (message instanceof FragmentTaskMessage) {
                    ts.createLocalFragmentWork((FragmentTaskMessage)message, false);
                }
            }
        }
        else if (message instanceof FragmentResponseMessage) {
            FragmentResponseMessage response = (FragmentResponseMessage)message;
            TransactionState txnState = m_transactionsById.get(response.getTxnId());
            // possible in rollback to receive an unnecessary response
            if (txnState != null) {
                assert (txnState instanceof MultiPartitionParticipantTxnState);
                txnState.processRemoteWorkResponse(response);
            }
        }
        else if (message instanceof DebugMessage) {
            DebugMessage dmsg = (DebugMessage) message;
            if (dmsg.shouldDump)
                DumpManager.putDump(m_dumpId, m_currentDumpTimestamp, true, getDumpContents());
        }
        else if (message instanceof ExecutionSiteNodeFailureMessage) {
            discoverGlobalFaultData(((ExecutionSiteNodeFailureMessage)message).m_failedHosts);
        }
        else if (message instanceof CheckTxnStateCompletionMessage) {
            long txn_id = ((CheckTxnStateCompletionMessage)message).m_txnId;
            TransactionState txnState = m_transactionsById.get(txn_id);
            if (txnState != null)
            {
                assert(txnState instanceof MultiPartitionParticipantTxnState);
                ((MultiPartitionParticipantTxnState)txnState).checkWorkUnits();
            }
        }
        else if (message instanceof RawProcessor.ELTInternalMessage) {
            RawProcessor.ELTInternalMessage eltm =
                (RawProcessor.ELTInternalMessage) message;
            ELTProtoMessage response =
                ee.eltAction(eltm.m_m.isAck(),
                             eltm.m_m.isPoll(),
                             eltm.m_m.isClose(),
                             eltm.m_m.getAckOffset(),
                             eltm.m_m.getPartitionId(),
                             eltm.m_m.getTableId());
            // not all actions generate a response
            if (response != null) {
                ELTInternalMessage mbp = new ELTInternalMessage(eltm.m_sb, response);
                ELTManager.instance().queueMessage(mbp);
            }
        }
        else {
            hostLog.l7dlog(Level.FATAL, LogKeys.org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass.name(),
                           new Object[] { message.getClass().getName() }, null);
            VoltDB.crashVoltDB();
        }
    }

    private void assertTxnIdOrdering(final TransactionInfoBaseMessage notice) {
        // Because of our rollback implementation, fragment tasks can arrive
        // late. This participant can have aborted and rolled back already,
        // for example.
        //
        // Additionally, commit messages for read-only MP transactions can
        // arrive after sneaked-in SP transactions have advanced the last
        // committed transaction point. A commit message is a fragment task
        // with a null payload.
        if (notice instanceof FragmentTaskMessage) {
            return;
        }

        if (notice.getTxnId() < lastCommittedTxnId) {
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock (DTXN) at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(lastCommittedTxnId).append(" (");
            msg.append(TransactionIdManager.toString(lastCommittedTxnId)).append(" HB: ?");
            msg.append(") before\n");
            msg.append("   txn ").append(notice.getTxnId()).append(" (");
            msg.append(TransactionIdManager.toString(notice.getTxnId())).append(" HB:");
            msg.append(notice instanceof HeartbeatMessage).append(").\n");

            TransactionState txn = m_transactionsById.get(notice.getTxnId());
            if (txn != null) {
                msg.append("New notice transaction already known: " + txn.toString() + "\n");
            }
            else {
                msg.append("New notice is for new or completed transaction.\n");
            }
            msg.append("New notice of type: " + notice.getClass().getName());
            log.fatal(msg);
            VoltDB.crashVoltDB();
        }

        if (notice instanceof InitiateTaskMessage) {
            InitiateTaskMessage task = (InitiateTaskMessage)notice;
            assert (task.getInitiatorSiteId() != getSiteId());
        }
    }

    /**
     * Find the global multi-partition commit point and the global initiator point for the
     * failed host.
     *
     * @param failedHostId the host id of the failed node.
     */
    private void discoverGlobalFaultData(HashSet<NodeFailureFault> failedHosts)
    {
        HashSet<Integer> failedHostIds = new HashSet<Integer>();
        for (NodeFailureFault fault : failedHosts) {
            failedHostIds.add(fault.getHostId());
        }

        m_knownFailedHosts.addAll(failedHostIds);

        // Fix context and associated site tracker first - need
        // an accurate topology to perform discovery.
        m_context = VoltDB.instance().getCatalogContext();

        int expectedResponses = discoverGlobalFaultData_send();
        long[] commit_and_safe = discoverGlobalFaultData_rcv(expectedResponses);

        if (commit_and_safe == null) {
            return;
        }

        /*
         * Do a little work to identify the newly failed HostIDs and only handle those
         */
        HashSet<Integer> newFailedHostIds = new HashSet<Integer>(failedHostIds);
        newFailedHostIds.removeAll(m_handledFailedHosts);
        handleNodeFault(newFailedHostIds, commit_and_safe[0], commit_and_safe[1]);
        m_handledFailedHosts.addAll(failedHostIds);
        for (NodeFailureFault fault : failedHosts) {
            if (newFailedHostIds.contains(fault.getHostId())) {
                VoltDB.instance().getFaultDistributor().reportFaultHandled(m_faultHandler, fault);
            }
        }
    }

    /**
     * The list of failed hosts we know about. Included with all failure messages
     * to identify what the information was used to generate commit points
     */
    private HashSet<Integer> m_knownFailedHosts = new HashSet<Integer>();

    /**
     * Failed hosts for which agreement has been reached.
     */
    private HashSet<Integer> m_handledFailedHosts = new HashSet<Integer>();

    /**
     * Store values from older failed nodes. They are repeated with every failure message
     */
    private HashMap<Integer, HashMap<Integer, Long>> m_newestSafeTransactionForInitiatorLedger =
        new HashMap<Integer, HashMap<Integer, Long>>();

    /**
     * Send one message to each surviving execution site providing this site's
     * multi-partition commit point and this site's safe txnid
     * (the receiver will filter the later for its
     * own partition). Do this once for each failed initiator that we know about.
     * Sends all data all the time to avoid a need for request/response.
     */
    private int discoverGlobalFaultData_send()
    {
        int expectedResponses = 0;
        int[] survivors = m_context.siteTracker.getUpExecutionSites();
        HashSet<Integer> survivorSet = new HashSet<Integer>();
        for (int survivor : survivors) {
            survivorSet.add(survivor);
        }
        m_recoveryLog.info("Sending fault data " + m_knownFailedHosts.toString() + " to " + survivorSet.toString() + " survivors with lastCommitedMultiPartTxnId " + lastCommittedMultiPartTxnId);
        try {
            for (Integer hostId : m_knownFailedHosts) {
                HashMap<Integer, Long> siteMap = m_newestSafeTransactionForInitiatorLedger.get(hostId);
                if (siteMap == null) {
                    siteMap = new HashMap<Integer, Long>();
                    m_newestSafeTransactionForInitiatorLedger.put(hostId, siteMap);
                }

                for (Integer site : m_context.siteTracker.getAllSitesForHost(hostId))
                {
                    if (m_context.siteTracker.getSiteForId(site).getIsexec() == false) {
                        /*
                         * Check the queue for the data and get it from the ledger if necessary
                         */
                        Long txnId = m_transactionQueue.getNewestSafeTransactionForInitiator(site);
                        if (txnId == null) {
                            txnId = siteMap.get(site);
                            assert(txnId != null);
                        } else {
                            siteMap.put(site, txnId);
                        }

                        FailureSiteUpdateMessage srcmsg =
                            new FailureSiteUpdateMessage(m_siteId,
                                                         m_knownFailedHosts,
                                                         site,
                                                         txnId,
                                                         lastCommittedMultiPartTxnId);

                        m_mailbox.send(survivors, 0, srcmsg);
                        expectedResponses += (survivors.length);
                    }
                }
            }
        }
        catch (MessagingException e) {
            // TODO: unsure what to do with this. maybe it implies concurrent failure?
            e.printStackTrace();
            VoltDB.crashVoltDB();
        }
        return expectedResponses;
    }

    /**
     * Collect the failure site update messages from all sites This site sent
     * its own mailbox the above broadcast the maximum is local to this site.
     * This also ensures at least one response.
     *
     * Concurrent failures can be detected by additional reports from the FaultDistributor
     * or a mismatch in the set of failed hosts reported in a message from another site
     */
    private long[] discoverGlobalFaultData_rcv(int expectedResponses)
    {
        final int localPartitionId =
            m_context.siteTracker.getPartitionForSite(m_siteId);
        int responses = 0;
        int responsesFromSamePartition = 0;
        long commitPoint = Long.MIN_VALUE;
        long safeInitPoint = Long.MIN_VALUE;
        java.util.ArrayList<FailureSiteUpdateMessage> messages = new java.util.ArrayList<FailureSiteUpdateMessage>();
        do {
            VoltMessage m = m_mailbox.recvBlocking(new Subject[] { Subject.FAILURE, Subject.FAILURE_SITE_UPDATE });
            FailureSiteUpdateMessage fm = null;

            if (m.getSubject() == Subject.FAILURE_SITE_UPDATE.getId()) {
                fm = (FailureSiteUpdateMessage)m;
                messages.add(fm);
            } else if (m.getSubject() == Subject.FAILURE.getId()) {
                /*
                 * If the fault distributor reports a new fault, assert that the fault currently
                 * being handled is included, redeliver the message to ourself and then abort so
                 * that the process can restart.
                 */
                HashSet<NodeFailureFault> faults = ((ExecutionSiteNodeFailureMessage)m).m_failedHosts;
                HashSet<Integer> newFailedHostIds = new HashSet<Integer>();
                for (NodeFailureFault fault : faults) {
                    newFailedHostIds.add(fault.getHostId());
                }
                m_mailbox.deliver(m);
                m_recoveryLog.info("Detected a concurrent failure from FaultDistributor, new failed hosts "
                        + newFailedHostIds);
                return null;
            }

            /*
             * If the other surviving host saw a different set of failures
             */
            if (!m_knownFailedHosts.equals(fm.m_failedHostIds)) {
                if (!m_knownFailedHosts.containsAll(fm.m_failedHostIds)) {
                    /*
                     * In this case there is a new failed host we didn't know about. Time to
                     * start the process again from square 1 with knowledge of the new failed hosts
                     * First fail all the ones we didn't know about.
                     */
                    HashSet<Integer> difference = new HashSet<Integer>(fm.m_failedHostIds);
                    difference.removeAll(m_knownFailedHosts);
                    for (Integer hostId : difference) {
                        String hostname = String.valueOf(hostId);
                        if (VoltDB.instance() != null) {
                            if (VoltDB.instance().getHostMessenger() != null) {
                                hostname = VoltDB.instance().getHostMessenger().getHostnameForHostID(hostId);
                            }
                        }
                        VoltDB.instance().getFaultDistributor().
                            reportFault(new NodeFailureFault( hostId, hostname));
                    }
                    m_recoveryLog.info("Detected a concurrent failure from " +
                            fm.m_sourceSiteId + " with new failed hosts " + difference.toString());
                    m_mailbox.deliver(m);
                    /*
                     * Return null and skip handling the fault for now. Will try again
                     * later once the other failed hosts are detected and can be dealt with at once.
                     */
                    return null;
                } else {
                    /*
                     * In this instance they are not equal because the message is missing some
                     * failed sites. Drop the message. The sender will detect the fault and resend
                     * the message later with the correct information.
                     */
                    HashSet<Integer> difference = new HashSet<Integer>(m_knownFailedHosts);
                    difference.removeAll(fm.m_failedHostIds);
                    m_recoveryLog.info("Discarding failure message from " +
                            fm.m_sourceSiteId + " because it was missing failed hosts " + difference.toString());
                    continue;
                }
            }

            ++responses;
            m_recoveryLog.trace("Received failure message " + responses + " of " + expectedResponses
                    + " from " + fm.m_sourceSiteId + " for failed sites " + fm.m_failedHostIds +
                    " with commit point " + fm.m_committedTxnId + " safe txn id " + fm.m_safeTxnId +
                    " with failed host ids " + fm.m_failedHostIds);
            commitPoint =
                Math.max(commitPoint, fm.m_committedTxnId);

            final int remotePartitionId =
                m_context.siteTracker.getPartitionForSite(fm.m_sourceSiteId);

            if (remotePartitionId == localPartitionId) {
                safeInitPoint =
                    Math.max(safeInitPoint, fm.m_safeTxnId);
                responsesFromSamePartition++;
            }
        } while(responses < expectedResponses);

        assert(commitPoint != Long.MIN_VALUE);
        assert(safeInitPoint != Long.MIN_VALUE);
        return new long[] {commitPoint, safeInitPoint};
    }


    /**
     * Process a node failure detection.
     *
     * Different sites can process UpdateCatalog sysproc and handleNodeFault()
     * in different orders. UpdateCatalog changes MUST be commutative with
     * handleNodeFault.
     *
     * @param HashSet<hostId> Host Ids of failed nodes
     * @param globalCommitPoint the surviving cluster's greatest committed multi-partition transaction id
     * @param globalInitiationPoint the greatest transaction id acknowledged as globally
     * 2PC to any surviving cluster execution site by the failed initiator.
     *
     */
    void handleNodeFault(HashSet<Integer> hostIds, long globalMultiPartCommitPoint,
                         long globalInitiationPoint) {
        StringBuffer sb = new StringBuffer();
        for (Integer hostId : hostIds) {
            sb.append(hostId).append(' ');
        }
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST handleNodeFault " + sb.toString() +
                    " with globalMultiPartCommitPoint " + globalMultiPartCommitPoint + " and globalInitiationPoint "
                    + globalInitiationPoint);
        }

        // Fix safe transaction scoreboard in transaction queue
        HashSet<Integer> failedSites = new HashSet<Integer>();
        for (Integer hostId : hostIds) {
            failedSites.addAll(m_context.siteTracker.getAllSitesForHost(hostId));
        }


        for (Integer i : failedSites)
        {
            if (m_context.siteTracker.getSiteForId(i).getIsexec() == false) {
                m_transactionQueue.gotFaultForInitiator(i);
            }
        }

        // Correct transaction state internals and commit
        // or remove affected transactions from RPQ and txnId hash.
        Iterator<Long> it = m_transactionsById.keySet().iterator();
        while (it.hasNext())
        {
            final long tid = it.next();
            TransactionState ts = m_transactionsById.get(tid);
            ts.handleSiteFaults(failedSites);

            // Fault a transaction that was not globally initiated
            if (ts.txnId > globalInitiationPoint &&
                failedSites.contains(ts.initiatorSiteId))
            {
                m_recoveryLog.info("Faulting non-globally initiated transaction " + ts.txnId);
                it.remove();
                m_transactionQueue.faultTransaction(ts);
                faultedTxns.add(ts.txnId);
            }

            // Multipartition transaction without a surviving coordinator:
            // Commit a txn that is in progress and committed elsewhere.
            // (Must have lost the commit message during the failure.)
            // Otherwise, without a coordinator, the transaction can't
            // continue. Must rollback, if in progress, or fault it
            // from the queues if not yet started.
            else if (ts instanceof MultiPartitionParticipantTxnState &&
                     failedSites.contains(ts.coordinatorSiteId))
            {
                MultiPartitionParticipantTxnState mpts = (MultiPartitionParticipantTxnState) ts;
                if (ts.isInProgress() && ts.txnId <= globalMultiPartCommitPoint)
                {
                    m_recoveryLog.info("Committing in progress multi-partition txn " + ts.txnId +
                            " even though coordinator was on a failed host because the txnId <= " +
                            "the global multi-part commit point");
                    FragmentTaskMessage ft = mpts.createConcludingFragmentTask();
                    ft.setShouldUndo(false);
                    m_mailbox.deliver(ft);
                }
                else if (ts.isInProgress() && ts.txnId > globalMultiPartCommitPoint) {
                    m_recoveryLog.info("Rolling back in progress multi-partition txn " + ts.txnId +
                            " because the coordinator was on a failed host and the txnId > " +
                            "the global multi-part commit point");
                    FragmentTaskMessage ft = mpts.createConcludingFragmentTask();
                    ft.setShouldUndo(true);
                    m_mailbox.deliver(ft);
                }
                else
                {
                    m_recoveryLog.info("Faulting multi-part transaction " + ts.txnId +
                            " because the coordinator was on a failed node");
                    it.remove();
                    m_transactionQueue.faultTransaction(ts);
                    faultedTxns.add(ts.txnId);
                }
            }
            // If we're the coordinator, then after we clean up our internal
            // state due to a failed node, we need to poke ourselves to check
            // to see if all the remaining dependencies are satisfied.  Do this
            // with a message to our mailbox so that happens in the
            // execution site thread
            else if (ts instanceof MultiPartitionParticipantTxnState &&
                     ts.coordinatorSiteId == m_siteId)
            {
                if (ts.isInProgress())
                {
                    m_mailbox.deliver(new CheckTxnStateCompletionMessage(ts.txnId));
                }
            }
        }
    }


    private FragmentResponseMessage processSysprocFragmentTask(
            final TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final long fragmentId, final FragmentResponseMessage currentFragResponse,
            final ParameterSet params)
    {
        // assume success. errors correct this assumption as they occur
        currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);

        VoltSystemProcedure proc = null;
        synchronized (m_registeredSysProcPlanFragments) {
            proc = m_registeredSysProcPlanFragments.get(fragmentId);
        }

        try {
            // set transaction state for non-coordinator snapshot restore sites
            proc.setTransactionState(txnState);
            final DependencyPair dep
                = proc.executePlanFragment(dependencies,
                                           fragmentId,
                                           params,
                                           m_systemProcedureContext);

            sendDependency(currentFragResponse, dep.depId, dep.dependency);
        }
        catch (final EEException e)
        {
            hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
            currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
        }
        catch (final SQLException e)
        {
            hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
            currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
        }
        catch (final Exception e)
        {
            // Just indicate that we failed completely
            currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(e));
        }

        return currentFragResponse;
    }


    private void sendDependency(
            final FragmentResponseMessage currentFragResponse,
            final int dependencyId,
            final VoltTable dependency)
    {
        if (log.isTraceEnabled()) {
            log.l7dlog(Level.TRACE,
                       LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(),
                       new Object[] { dependencyId }, null);
        }
        currentFragResponse.addDependency(dependencyId, dependency);
    }


    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    public void initiateSnapshots(Deque<SnapshotTableTask> tasks) {
        m_snapshotter.initiateSnapshots(ee, tasks);
    }

    public HashSet<Exception> completeSnapshotWork() throws InterruptedException {
        return m_snapshotter.completeSnapshotWork(ee);
    }


    /*
     *
     *  SiteConnection Interface (VoltProcedure -> ExecutionSite)
     *
     */

    @Override
    public void registerPlanFragment(final long pfId, final VoltSystemProcedure proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false);
            m_registeredSysProcPlanFragments.put(pfId, proc);
        }
    }

    @Override
    public Site getCorrespondingCatalogSite() {
        return getCatalogSite();
    }

    @Override
    public int getCorrespondingSiteId() {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId() {
        return Integer.valueOf(getCatalogSite().getPartition().getTypeName());
    }

    @Override
    public int getCorrespondingHostId() {
        return Integer.valueOf(getCatalogSite().getHost().getTypeName());
    }

    @Override
    public void loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data,
            int allowELT)
    throws VoltAbortException
    {
        Cluster cluster = m_context.cluster;
        if (cluster == null) {
            throw new VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        Database db = cluster.getDatabases().get(databaseName);
        if (db == null) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        ee.loadTable(table.getRelativeIndex(), data,
                     txnId,
                     lastCommittedTxnId,
                     getNextUndoToken(),
                     allowELT != 0);
    }

    @Override
    public VoltTable[] executeQueryPlanFragmentsAndGetResults(
            long[] planFragmentIds,
            int numFragmentIds,
            ParameterSet[] parameterSets,
            int numParameterSets,
            long txnId,
            boolean readOnly) throws EEException
    {
        return ee.executeQueryPlanFragmentsAndGetResults(
            planFragmentIds,
            numFragmentIds,
            parameterSets,
            numParameterSets,
            txnId,
            lastCommittedTxnId,
            readOnly ? Long.MAX_VALUE : getNextUndoToken());
    }

    @Override
    public void simulateExecutePlanFragments(long txnId, boolean readOnly) {
        if (!readOnly) {
            // pretend real work was done
            getNextUndoToken();
        }
    }

    /**
     * Continue doing runnable work for the current transaction.
     * If doWork() returns true, the transaction is over.
     * Otherwise, the procedure may have more java to run
     * or a dependency or fragment to collect from the network.
     *
     * doWork() can sneak in a new SP transaction. Maybe it would
     * be better if transactions didn't trigger other transactions
     * and those optimization decisions where made somewhere closer
     * to this code?
     */
    @Override
    public Map<Integer, List<VoltTable>>
    recursableRun(TransactionState currentTxnState)
    {
        do
        {
            if (currentTxnState.doWork()) {
                if (currentTxnState.needsRollback())
                {
                    rollbackTransaction(currentTxnState);
                }
                completeTransaction(currentTxnState);
                TransactionState ts = m_transactionsById.remove(currentTxnState.txnId);
                assert(ts != null);
                return null;
            }
            else if (currentTxnState.shouldResumeProcedure()){
                Map<Integer, List<VoltTable>> retval =
                    currentTxnState.getPreviousStackFrameDropDependendencies();
                assert(retval != null);
                return retval;
            }
            else {
                VoltMessage message = m_mailbox.recvBlocking(5);
                tick();
                if (message != null) {
                    handleMailboxMessage(message);
                }
            }
        } while (true);
    }

    /*
     *
     *  SiteTransactionConnection Interface (TransactionState -> ExecutionSite)
     *
     */

    public SiteTracker getSiteTracker() {
        return m_context.siteTracker;
    }

    /**
     * Set the txn id from the WorkUnit and set/release undo tokens as
     * necessary. The DTXN currently has no notion of maintaining undo
     * tokens beyond the life of a transaction so it is up to the execution
     * site to release the undo data in the EE up until the current point
     * when the transaction ID changes.
     */
    @Override
    public final void beginNewTxn(TransactionState txnState)
    {
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST beginNewTxn " + txnState.txnId + " " +
                           (txnState.isSinglePartition() ? "single" : "multi"));
        }
        if (!txnState.isReadOnly) {
            assert(txnState.getBeginUndoToken() == kInvalidUndoToken);
            txnState.setBeginUndoToken(latestUndoToken);
            assert(txnState.getBeginUndoToken() != kInvalidUndoToken);
        }
    }

    public final void rollbackTransaction(TransactionState txnState)
    {
        if (m_txnlog.isTraceEnabled())
        {
            m_txnlog.trace("FUZZTEST rollbackTransaction " + txnState.txnId);
        }
        if (!txnState.isReadOnly) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(txnState.getBeginUndoToken() != kInvalidUndoToken);
            assert(latestUndoToken >= txnState.getBeginUndoToken());

            // don't go to the EE if no work was done
            if (latestUndoToken > txnState.getBeginUndoToken()) {
                ee.undoUndoToken(txnState.getBeginUndoToken());
            }
        }
    }


    @Override
    public FragmentResponseMessage processFragmentTask(
            TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final VoltMessage task)
    {
        ParameterSet params = null;
        final FragmentTaskMessage ftask = (FragmentTaskMessage) task;
        assert(ftask.getFragmentCount() == 1);
        final long fragmentId = ftask.getFragmentId(0);
        final int outputDepId = ftask.getOutputDepId(0);

        final FragmentResponseMessage currentFragResponse = new FragmentResponseMessage(ftask, getSiteId());

        // this is a horrible performance hack, and can be removed with small changes
        // to the ee interface layer.. (rtb: not sure what 'this' encompasses...)
        final ByteBuffer paramData = ftask.getParameterDataForFragment(0);
        if (paramData != null) {
            final FastDeserializer fds = new FastDeserializer(paramData);
            try {
                params = fds.readObject(ParameterSet.class);
            }
            catch (final IOException e) {
                hostLog.l7dlog( Level.FATAL,
                                LogKeys.host_ExecutionSite_FailedDeserializingParamsForFragmentTask.name(), e);
                VoltDB.crashVoltDB();
            }
        }
        else {
            params = new ParameterSet();
        }

        if (ftask.isSysProcTask()) {
            return processSysprocFragmentTask(txnState, dependencies, fragmentId,
                                              currentFragResponse, params);
        }
        else {
            // start the clock on this statement
            ProcedureProfiler.startStatementCounter(fragmentId);

            if (dependencies != null) {
                ee.stashWorkUnitDependencies(dependencies);
            }
            final int inputDepId = ftask.getOnlyInputDepId(0);

            /*
             * Currently the error path when executing plan fragments
             * does not adequately distinguish between fatal errors and
             * abort type errors that should result in a roll back.
             * Assume that it is ninja: succeeds or doesn't return.
             * No roll back support.
             */
            currentFragResponse.setStatus(FragmentResponseMessage.SUCCESS, null);
            try {
                final DependencyPair dep = ee.executePlanFragment(fragmentId,
                                                                  outputDepId,
                                                                  inputDepId,
                                                                  params,
                                                                  txnState.txnId,
                                                                  lastCommittedTxnId,
                                                                  getNextUndoToken());

                sendDependency(currentFragResponse, dep.depId, dep.dependency);

            } catch (final EEException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
            } catch (final SQLException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, e);
            }

            ProcedureProfiler.stopStatementCounter();
            return currentFragResponse;
        }
    }


    @Override
    public InitiateResponseMessage processInitiateTask(
            TransactionState txnState,
            final VoltMessage task)
    {
        final InitiateTaskMessage itask = (InitiateTaskMessage)task;
        final VoltProcedure wrapper = procs.get(itask.getStoredProcedureName());
        if (wrapper == null) {
            System.err.printf("Missing procedure \"%s\" at execution site. %d\n", itask.getStoredProcedureName(), m_siteId);
            VoltDB.crashVoltDB();
        }

        final InitiateResponseMessage response = new InitiateResponseMessage(itask);

        try {
            if (wrapper instanceof VoltSystemProcedure) {
                Object[] callerParams = itask.getParameters();
                Object[] combinedParams = new Object[callerParams.length + 1];
                combinedParams[0] = m_systemProcedureContext;
                for (int i=0; i < callerParams.length; ++i) combinedParams[i+1] = callerParams[i];
                final ClientResponseImpl cr = wrapper.call(txnState, combinedParams);
                response.setResults(cr, itask);
            }
            else {
                final ClientResponseImpl cr = wrapper.call(txnState, itask.getParameters());
                response.setResults(cr, itask);
            }
        }
        catch (final ExpectedProcedureException e) {
            log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_ExpectedProcedureException.name(), e);
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
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_UnexpectedProcedureException.name(), e);
            VoltDB.crashVoltDB();
        }

        log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        return response;
    }

    /**
     * Try to execute a single partition procedure if one is available in the
     * priority queue.
     *
     * @return false if there is no possibility for speculative work.
     */
    @Override
    public boolean tryToSneakInASinglePartitionProcedure() {
        // poll for an available message. don't block
        VoltMessage message = m_mailbox.recv();
        tick(); // unclear if this necessary (rtb)
        if (message != null) {
            handleMailboxMessage(message);
            return true;
        }
        else {
            TransactionState nextTxn = m_transactionQueue.peek();

            // only sneak in single partition work
            if (nextTxn instanceof SinglePartitionTxnState) {

                // i think this line does nothing... it should go?
                // seems it will get popped later, but not do any work because the done state is true
                // ugh
                //nextTxn = m_transactionQueue.peek();

                boolean success = nextTxn.doWork();
                assert(success);
                return true;
            }
            else {
                // multipartition is next or no work
                return false;
            }
        }
    }



    /*
     *
     * Dump manager interface
     *
     */

    @Override
    public void goDumpYourself(final long timestamp) {
        m_currentDumpTimestamp = timestamp;
        DebugMessage dmsg = new DebugMessage();
        dmsg.shouldDump = true;
        try {
            m_mailbox.send(getSiteId(), 0, dmsg);
        }
        catch (org.voltdb.messaging.MessagingException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the actual file contents for a dump of state reachable by
     * this thread. Can be called unsafely or safely.
     */
    public ExecutorContext getDumpContents() {
        final ExecutorContext context = new ExecutorContext();
        context.siteId = getSiteId();

        // messaging log window stored in mailbox history
        if (m_mailbox instanceof SiteMailbox)
            context.mailboxHistory = ((SiteMailbox) m_mailbox).getHistory();

        // restricted priority queue content
        m_transactionQueue.getDumpContents(context);

        // TODO:
        // m_transactionsById.getDumpContents(context);

        return context;
    }
}
