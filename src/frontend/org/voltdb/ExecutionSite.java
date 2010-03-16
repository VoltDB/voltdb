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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.catalog.*;
import org.voltdb.client.ClientResponse;
import org.voltdb.debugstate.ExecutorContext;
import org.voltdb.dtxn.*;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.jni.*;
import org.voltdb.messages.*;
import org.voltdb.messaging.*;
import org.voltdb.messaging.impl.SiteMailbox;
import org.voltdb.utils.*;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite
extends SiteConnection
implements Runnable, DumpManager.Dumpable
{
    private static final Logger log = Logger.getLogger(ExecutionSite.class.getName(), VoltLoggerFactory.instance());
    private static final Logger hostLog = Logger.getLogger("HOST", VoltLoggerFactory.instance());
    private static AtomicInteger siteIndexCounter = new AtomicInteger(0);
    private final int siteIndex = siteIndexCounter.getAndIncrement();

    final HashMap<String, VoltProcedure> procs = new HashMap<String, VoltProcedure>(16, (float) .1);
    private final Mailbox m_mailbox;
    final ExecutionEngine ee;
    final HsqlBackend hsql;
    public volatile boolean m_shouldContinue = true;

    // Catalog
    public CatalogContext m_context;
    public int siteId;
    Site getCatalogSite() {
        return m_context.cluster.getSites().get(Integer.toString(siteId));
    }

    HashMap<Long, TransactionState> m_transactionsById = new HashMap<Long, TransactionState>();
    private final RestrictedPriorityQueue m_transactionQueue;

    // The time in ms since epoch of the last call to tick()
    long lastTickTime = 0;
    long lastCommittedTxnId = 0;

    private final static long kInvalidUndoToken = -1L;
    private long txnBeginUndoToken = kInvalidUndoToken;
    private long batchBeginUndoToken = kInvalidUndoToken;
    private long latestUndoToken = 0L;

    public long getNextUndoToken() {
        return ++latestUndoToken;
    }

    // store the id used by the DumpManager to identify this execution site
    public final String m_dumpId;
    public long m_currentDumpTimestamp = 0;

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private final SnapshotSiteProcessor m_snapshotter;

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

    // Associate the system procedure planfragment ids to wrappers.
    // Planfragments are registered when the procedure wrapper is init()'d.
    private final HashMap<Long, VoltSystemProcedure> m_registeredSysProcPlanFragments =
        new HashMap<Long, VoltSystemProcedure>();

    public void registerPlanFragment(final long pfId, final VoltSystemProcedure proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false);
            m_registeredSysProcPlanFragments.put(pfId, proc);
        }
    }

    /**
     * Log settings changed. Signal EE to update log level.
     */
    public void updateBackendLogLevels() {
        ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
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
    }

    /**
     * Set the txn id from the WorkUnit and set/release undo tokens as
     * necessary. The DTXN currently has no notion of maintaining undo
     * tokens beyond the life of a transaction so it is up to the execution
     * site to release the undo data in the EE up until the current point
     * when the transaction ID changes.
     */
    public final void beginNewTxn(long txnId, boolean readOnly) {
        if (!readOnly) {
            assert(txnBeginUndoToken == kInvalidUndoToken);
            assert(batchBeginUndoToken == kInvalidUndoToken);
            txnBeginUndoToken = latestUndoToken;
        }
    }

    public final void beginNewBatch(boolean readOnly) {
        if (!readOnly) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(txnBeginUndoToken != kInvalidUndoToken);
            assert(latestUndoToken >= txnBeginUndoToken);
            batchBeginUndoToken = latestUndoToken;
        }
    }

    public final void rollbackTransaction(boolean readOnly) {
        if (!readOnly) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(txnBeginUndoToken != kInvalidUndoToken);
            assert(latestUndoToken >= txnBeginUndoToken);

            // don't go to the EE of no work was done
            if (latestUndoToken > txnBeginUndoToken) {
                ee.undoUndoToken(txnBeginUndoToken);
            }
        }
    }

    public final void completeTransaction(boolean readOnly) {
        if (!readOnly) {
            assert(latestUndoToken != kInvalidUndoToken);
            assert(txnBeginUndoToken != kInvalidUndoToken);
            assert(latestUndoToken >= txnBeginUndoToken);

            // release everything through the end of the current window.
            if (latestUndoToken > txnBeginUndoToken) {
                ee.releaseUndoToken(latestUndoToken);
            }

            // reset for error checking purposes
            txnBeginUndoToken = kInvalidUndoToken;
            batchBeginUndoToken = kInvalidUndoToken;
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
        public Object getOperStatus();
        public ExecutionSite getExecutionSite();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        public Database getDatabase()               { return m_context.database; }
        public Cluster getCluster()                 { return m_context.cluster; }
        public Site getSite()                       { return getCatalogSite(); }
        public ExecutionEngine getExecutionEngine() { return ee; }
        public long getLastCommittedTxnId()         { return lastCommittedTxnId; }
        public long getNextUndo()                   { return getNextUndoToken(); }
        public String getOperStatus()               { return VoltDB.getOperStatus(); }
        public ExecutionSite getExecutionSite()     { return ExecutionSite.this; }
    }

    SystemProcedureContext m_systemProcedureContext;

    /** For test MockExecutionSite */
    ExecutionSite() {
        super(0);
        m_systemProcedureContext = new SystemProcedureContext();
        m_watchdog = null;
        ee = null;
        hsql = null;
        m_dumpId = "MockExecSite";
        m_snapshotter = null;
        m_mailbox = null;
        m_transactionQueue = null;
    }

    /**
     * Initialize the StoredProcedure runner and EE for this Site.
     * @param siteManager
     * @param siteId
     * @param context A reference to the current catalog context
     * newlines that, when executed, reconstruct the complete m_catalog.
     */
    ExecutionSite(final int siteId, final CatalogContext context, String serializedCatalog) {
        super(siteId);
        hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_Initializing.name(), new Object[] { String.valueOf(siteId) }, null);

        String hostname = "";
        try {
            java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
            hostname = localMachine.getHostName();
        } catch (java.net.UnknownHostException uhe) {
        }

        if (serializedCatalog == null)
            serializedCatalog = context.catalog.serialize();

        m_watchdog = new Watchdog(siteId, siteIndex);
        this.siteId = siteId;
        m_context = context;
        m_systemProcedureContext = new SystemProcedureContext();

        m_dumpId = "ExecutionSite." + String.valueOf(siteId);
        DumpManager.register(m_dumpId, this);

        // get an array of all the initiators
        int initiatorCount = 0;
        for (final Site s : m_context.cluster.getSites())
            if (s.getIsexec() == false)
                initiatorCount++;
        final int[] initiatorIds = new int[initiatorCount];
        int index = 0;
        for (final Site s : m_context.cluster.getSites())
            if (s.getIsexec() == false)
                initiatorIds[index++] = Integer.parseInt(s.getTypeName());

        m_mailbox = VoltDB.instance().getMessenger()
            .createMailbox(siteId, VoltDB.DTXN_MAILBOX_ID, null);

        m_transactionQueue = new RestrictedPriorityQueue(initiatorIds, siteId);

        // An execution site can be backed by HSQLDB, by volt's EE accessed
        // via JNI or by volt's EE accessed via IPC.  When backed by HSQLDB,
        // the VoltProcedure interface invokes HSQLDB directly through its
        // hsql Backend member variable.  The real volt backend is encapsulated
        // by the ExecutionEngine class. This class has implementations for both
        // JNI and IPC - and selects the desired implementation based on the
        // value of this.eeBackend.
        HsqlBackend hsqlTemp = null;
        ExecutionEngine eeTemp = null;
        try {
            final BackendTarget target = VoltDB.getEEBackendType();
            if (target == BackendTarget.HSQLDB_BACKEND) {
                hsqlTemp = new HsqlBackend(siteId);
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
                eeTemp = new MockExecutionEngine();
            }
            else if (target == BackendTarget.NATIVE_EE_JNI) {
                Site site = getCatalogSite();
                eeTemp =
                    new ExecutionEngineJNI(
                        this,
                        m_context.cluster.getRelativeIndex(),
                        siteId,
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
                            siteId,
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
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(), new Object[] { siteId, siteIndex }, ex);
            VoltDB.crashVoltDB();
        }
        ee = eeTemp;
        hsql = hsqlTemp;

        // load up all the stored procedures
        loadProceduresFromCatalog();
        m_snapshotter = new SnapshotSiteProcessor();
    }

    public boolean updateCatalog(String catalogDiffCommands) {
        m_context = VoltDB.instance().getCatalogContext();
        loadProceduresFromCatalog();
        ee.updateCatalog(catalogDiffCommands);
        return true;
    }

    void loadProceduresFromCatalog() {
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
                            new Object[] { siteId, siteIndex },
                            e);
                    VoltDB.crashVoltDB();
                }
                try {
                    wrapper = (VoltProcedure) procClass.newInstance();
                }
                catch (final InstantiationException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(), new Object[] { siteId, siteIndex }, e);
                }
                catch (final IllegalAccessException e) {
                    hostLog.l7dlog( Level.WARN, LogKeys.host_ExecutionSite_GenericException.name(), new Object[] { siteId, siteIndex }, e);
                }
            }
            else {
                wrapper = new VoltProcedure.StmtProcedure();
            }

            wrapper.init(this, proc, VoltDB.getEEBackendType(), hsql, m_context.cluster);
            procs.put(proc.getTypeName(), wrapper);
        }
    }

    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        // pick a name with four places for site id so it can be sorted later
        // bit hackish, sorry
        String name = "ExecutionSite:";
        if (siteId < 10) name += "0";
        if (siteId < 100) name += "0";
        if (siteId < 1000) name += "0";
        name += String.valueOf(siteId);
        Thread.currentThread().setName(name);

        NDC.push("ExecutionSite - " + siteId + " index " + siteIndex);
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
            // true indicates starting new stack; allow recursableRun to return.
            Object test = recursableRun(null, true);
            assert(test == null);
        }
        catch (final RuntimeException e) {
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_RuntimeException.name(), e);
            throw e;
        }
        shutdown();
    }

    public FragmentResponse processFragmentTask(TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies, final VoltMessage task)
    {
        ParameterSet params = null;
        final FragmentTask ftask = (FragmentTask) task;
        assert(ftask.getFragmentCount() == 1);
        final long fragmentId = ftask.getFragmentId(0);
        final int outputDepId = ftask.getOutputDepId(0);

        final FragmentResponse currentFragResponse = new FragmentResponse(ftask, siteId);

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
            currentFragResponse.setStatus(FragmentResponse.SUCCESS, null);
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
                currentFragResponse.setStatus(FragmentResponse.UNEXPECTED_ERROR, e);
            } catch (final SQLException e) {
                hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
                currentFragResponse.setStatus(FragmentResponse.UNEXPECTED_ERROR, e);
            }

            ProcedureProfiler.stopStatementCounter();
            return currentFragResponse;
        }
    }

    public FragmentResponse processSysprocFragmentTask(
            final TransactionState txnState,
            final HashMap<Integer,List<VoltTable>> dependencies,
            final long fragmentId, final FragmentResponse currentFragResponse,
            final ParameterSet params)
    {
        // assume success. errors correct this assumption as they occur
        currentFragResponse.setStatus(FragmentResponse.SUCCESS, null);

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
            if (dep == null) {
                log.l7dlog(Level.TRACE,
                           LogKeys.org_voltdb_ExecutionSite_SysprocReturnedNoDependencies.name(),
                           null);
            }

            sendDependency(currentFragResponse, dep.depId, dep.dependency);

        }
        catch (final EEException e)
        {
            hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
            currentFragResponse.setStatus(FragmentResponse.UNEXPECTED_ERROR, e);
        }
        catch (final SQLException e)
        {
            hostLog.l7dlog( Level.TRACE, LogKeys.host_ExecutionSite_ExceptionExecutingPF.name(), new Object[] { fragmentId }, e);
            currentFragResponse.setStatus(FragmentResponse.UNEXPECTED_ERROR, e);
        }
        catch (final Exception e)
        {
            // Just indicate that we failed completely
            currentFragResponse.setStatus(FragmentResponse.UNEXPECTED_ERROR, new SerializableException(e));
        }

        return currentFragResponse;
    }


    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    public HashSet<Exception> completeSnapshotWork() throws InterruptedException {
        return m_snapshotter.completeSnapshotWork(ee);
    }

    public InitiateResponse processInitiateTask(TransactionState txnState, final VoltMessage task) {
        final InitiateTask itask = (InitiateTask)task;
        final VoltProcedure wrapper = procs.get(itask.getStoredProcedureName());
        assert(wrapper != null); // existed in ClientInterface's catalog.

        final InitiateResponse response = new InitiateResponse(itask);

        try {
            final ClientResponseImpl cr = wrapper.call(txnState, itask.getParameters());
            response.setResults(cr, itask);
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
            // Show the WHOLE exception in the log
            hostLog.l7dlog( Level.ERROR, LogKeys.host_ExecutionSite_UnexpectedProcedureException.name(), e);
            VoltDB.crashVoltDB();
        }

        log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingCompletedWUToDtxn.name(), null);
        if (txnState.txnId > lastCommittedTxnId) {
            lastCommittedTxnId = txnState.txnId;
        }

        return response;
    }

    public void sendDependency(final FragmentResponse currentFragResponse,
                               final int dependencyId, final VoltTable dependency) {
        if (log.isTraceEnabled()) {
            log.l7dlog( Level.TRACE, LogKeys.org_voltdb_ExecutionSite_SendingDependency.name(), new Object[] { dependencyId }, null);
        }

        currentFragResponse.addDependency(dependencyId, dependency);
    }

    void startShutdown() {
        m_shouldContinue = false;
    }

    /**
     * Shutdown all resources that need to be shutdown for this <code>ExecutionSite</code>.
     * May be called twice if recurssing via recursableRun(). Protected against that..
     */
    private boolean haveShutdownAlready;
    @Override
    public
    void shutdown() {

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

    public void initiateSnapshots(Deque<SnapshotTableTask> tasks) {
        m_snapshotter.initiateSnapshots(ee, tasks);
    }

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
        context.siteId = siteId;

        // messaging log window stored in mailbox history
        if (m_mailbox instanceof SiteMailbox)
            context.mailboxHistory = ((SiteMailbox) m_mailbox).getHistory();

        // restricted priority queue content
        m_transactionQueue.getDumpContents(context);

        // TODO:
        // m_transactionsById.getDumpContents(context);

        return context;
    }


    @Override
    public Map<Integer, List<VoltTable>> recursableRun(TransactionState currentTxnState,
                                                       boolean shutdownAllowed)
    {
        // loop until the system decided to stop this thread
        // but don't return with null if not the base stack frame for this method
        while ((!shutdownAllowed) || (m_shouldContinue)) {


            // DO ALL READY-TO-RUN WORK WITHOUT MORE MESSAGES
            // repeat until:
            // 1. no current transaction AND no ready transaction, or
            // 2. the current transaction is blocked (see end of loop)

            boolean moreWork = (currentTxnState != null) ||
                               ((currentTxnState = m_transactionQueue.poll()) != null);

            while (moreWork)
            {
                // assume there's a current txn inside this loop
                assert(currentTxnState != null);

                // do all runnable work for the current txn
                // if doWork returns true, the transaction is over
                if (currentTxnState.doWork())
                {
                    completeTransaction(currentTxnState.isReadOnly);
                    TransactionState ts = m_transactionsById.remove(currentTxnState.txnId);
                    assert(ts != null);

                    // try to get the next current in line
                    // continue if there's a ready txn
                    moreWork = (currentTxnState = m_transactionQueue.poll()) != null;
                }
                else {
                    // the current transaction is blocked
                    moreWork = false;

                    // check if we should drop down a stack frame
                    //  note, calling this resets the internal state so
                    //  subsequent calls won't return true
                    if (currentTxnState.shouldResumeProcedure()) {
                        Map<Integer, List<VoltTable>> retval = currentTxnState.getPreviousStackFrameDropDependendencies();
                        assert(retval != null);
                        assert(shutdownAllowed == false);
                        return retval;
                    }
                }
            }

            // Poll messaging layer, possibly blocking for awhile if idle.
            VoltMessage message = m_mailbox.recvBlocking(5);
            tick();
            if (message != null) {
                handleMailboxMessage(message);
            }
        }

        assert(shutdownAllowed);
        return null;
    }

    private void handleMailboxMessage(VoltMessage message) {
        if (message instanceof InitiateTask) {
            messageArrived((InitiateTask) message);
        }
        else if (message instanceof FragmentTask) {
            messageArrived((FragmentTask) message);
        }
        else if (message instanceof FragmentResponse) {
            messageArrived((FragmentResponse) message);
        }
        else if (message instanceof MembershipNotice) {
            processTransactionMembership((MembershipNotice) message);
        }
        else if (message instanceof DebugMessage) {
            DebugMessage dmsg = (DebugMessage) message;
            if (dmsg.shouldDump)
                DumpManager.putDump(m_dumpId, m_currentDumpTimestamp, true, getDumpContents());
        }
        else {
            hostLog.l7dlog(Level.FATAL, LogKeys.org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass.name(), new Object[] { message.getClass().getName() }, null);
            VoltDB.crashVoltDB();
        }
    }

    private void messageArrived(InitiateTask request) {
        assert (request.getInitiatorSiteId() != getSiteId());
        processTransactionMembership(request);
        assert(request.isSinglePartition() || (request.getNonCoordinatorSites() != null));
    }

    private void messageArrived(FragmentTask request) {
        assert (request.getInitiatorSiteId() != getSiteId());
        TransactionState state = processTransactionMembership(request);
        if (state == null)
            return;
        assert(state instanceof MultiPartitionParticipantTxnState);
        state.createLocalFragmentWork(request, false);
    }

    /**
     * A decision arrived for an open transaction
     */
    private void messageArrived(FragmentResponse response) {
        TransactionState txnState = m_transactionsById.get(response.getTxnId());

        // possible in rollback to receive an unnecessary response
        // this makes me (john) nervous.
        if (txnState == null) {
            System.out.printf("Site %d got an old response\n", m_siteId);
            return;
        }
        assert (txnState instanceof MultiPartitionParticipantTxnState);
        txnState.processRemoteWorkResponse(response);
    }

    /**
     * When a message arrives that tells this site that it is part of a
     * multi-partition transaction, create a TxnState object for it. If
     * a transaction state message exists, return it.
     *
     * @param notice The message that contains transaction information.
     * @return The SimpleTxnState object created or found.
     */
    private TransactionState processTransactionMembership(MembershipNotice notice) {
        if (notice.getTxnId() < lastCommittedTxnId) {
            // Because of our rollback implementation, fragment tasks can arrive late.
            // This participant can have aborted, causing the coordinator to abort and
            // send this task, for example.
            if (notice instanceof FragmentTask) {
                return null;
            }

            // vanilla membership notices and initiate tasks must not
            // arrive out of order
            StringBuilder msg = new StringBuilder();
            msg.append("Txn ordering deadlock (DTXN) at site ").append(m_siteId).append(":\n");
            msg.append("   txn ").append(lastCommittedTxnId).append(" (");
            msg.append(TransactionIdManager.toString(lastCommittedTxnId)).append(" HB: ?");
            msg.append(") before\n");
            msg.append("   txn ").append(notice.getTxnId()).append(" (");
            msg.append(TransactionIdManager.toString(notice.getTxnId())).append(" HB:");
            msg.append(notice.isHeartBeat()).append(").\n");
            throw new RuntimeException(msg.toString());
        }

        m_transactionQueue.gotTransaction(notice.getInitiatorSiteId(),
                                          notice.getTxnId(),
                                          notice.isHeartBeat());

        // ignore HeartBeats
        if (notice.isHeartBeat()) {
            return null;
        }

        TransactionState state = m_transactionsById.get(notice.getTxnId());
        if (state == null) {
            if (notice instanceof InitiateTask) {
                InitiateTask it = (InitiateTask) notice;
                if (it.isSinglePartition()) {
                    state = new SinglePartitionTxnState(m_mailbox, this, it);
                }
                else {
                    state = new MultiPartitionParticipantTxnState(m_mailbox, this, it);
                }

            }
            else {
                state = new MultiPartitionParticipantTxnState(m_mailbox, this, notice);
            }

            m_transactionQueue.add(state);
            m_transactionsById.put(state.txnId, state);
        }
        return state;
    }

    /**
     * Try to execute a single partition procedure if one is available in the
     * priority queue.
     *
     * @return true if a procedure was executed, false if none available
     */
    public boolean tryToSneakInASinglePartitionProcedure() {
        // poll for an available message. don't block
        {
            VoltMessage message = m_mailbox.recv();
            tick(); // unclear if this necessary (rtb)
            if (message != null) {
                handleMailboxMessage(message);
            }
        }

        TransactionState nextTxn = m_transactionQueue.peek();

        // only sneak in single partition work
        if (nextTxn instanceof SinglePartitionTxnState) {
            nextTxn = m_transactionQueue.peek();
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
