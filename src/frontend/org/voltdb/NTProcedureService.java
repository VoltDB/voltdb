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

package org.voltdb;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.InitiateResponseMessage;

import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.CompoundProcPolicyType;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ThreadFactoryBuilder;

/**
 * NTProcedureService is the manager class that handles most of the work to
 * load, run, and update non-transactional procedures and system procedures.
 *
 * It maintains a the current set of procedures and sysprocs loaded and updates
 * this set on catalog change.
 *
 * It has a queue and two executor services to execute non-transactional work. See
 * comments below for how this works. It should work ok with backpressure,
 * authentication, statistics and other transactional procedure features.
 *
 */
public class NTProcedureService {
    public static final VoltLogger LOG = new VoltLogger("NT");

    /**
     * If the NTProcedureService is paused, we add pending requests to a pending
     * list using this simple class.
     */
    private static class PendingInvocation {
        final long ciHandle;
        final AuthUser user;
        final Connection ccxn;
        final boolean isAdmin;
        final boolean ntPriority;
        final StoredProcedureInvocation task;

        PendingInvocation(
                long ciHandle, AuthUser user, Connection ccxn, boolean isAdmin, boolean ntPriority,
                StoredProcedureInvocation task) {
            this.ciHandle = ciHandle;
            this.user = user;
            this.ccxn = ccxn;
            this.isAdmin = isAdmin;
            this.ntPriority = ntPriority;
            this.task = task;
        }
    }

    // User-supplied non-transactional procedures
    private Map<String, ProcedureRunnerNTGenerator> m_procs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().build();
    // Non-transactional system procedures
    private Map<String, ProcedureRunnerNTGenerator> m_sysProcs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().build();

    // A tracker of currently executing procedures by id, where id is a long that increments with each call
    final Map<Long, ProcedureRunnerNT> m_outstanding = new ConcurrentHashMap<>();

    // Create a singleton scheduled thread pool for timeouts - create only if Compound procedures are used
    private ScheduledThreadPoolExecutor m_timeoutExecutor = null;
    private Map<Long, ScheduledFuture<?>> m_timeouts = null;

    // This lets us respond over the network directly
    final LightweightNTClientResponseAdapter m_internalNTClientAdapter;
    // Mailbox for the client interface is used to send messages directly to other nodes (sysprocs only)
    private final Mailbox m_mailbox;
    // We pause the service mid-catalog update for stats reasons
    private boolean m_paused = false;
    // Transactions that arrived when paused (should always be empty if not paused)
    private final Queue<PendingInvocation> m_pendingInvocations = new ArrayDeque<>();
    // increments for every procedure call
    private long nextProcedureRunnerId = 0;

    // no need for thread safety as this can't race with @UAC at startup
    boolean isRestoring;

    // names for threads in the exec service
    // (exposed for test use)
    final static String NTPROC_THREADPOOL_NAMEPREFIX = "NTPServiceThread-";
    final static String NTPROC_THREADPOOL_PRIORITY_SUFFIX = "Priority-";
    final static String COMPROC_THREADPOOL_NAMEPREFIX = "CompoundProcSvcThread-";

    public static final String NTPROCEDURE_RUN_EVERYWHERE_TIMEOUT = "NTPROCEDURE_RUN_EVERYWHERE_TIMEOUT";

    // Parameters for threadpool executors
    // (For the compound proc executor, these are now set by the deployment file)
    private static int defaultThreadLimit = Math.max(CoreUtils.availableProcessors(), 4);
    private static int NTPROC_THREADS = Integer.getInteger("NTPROC_THREADS", defaultThreadLimit);
    private static int NTPROC_QUEUEMAX = Integer.getInteger("NTPROC_QUEUEMAX", 10_000);

    // Runs the initial run() method of nt procs.
    //  (doesn't run nt procs if started by other nt procs).
    // Uses multiple threads, created on demand up to a limit.
    // After that there is a bounded queue.
    // Threads do not idle out, despite the timeout setting.
    private final ExecutorService m_primaryExecutorService = new ThreadPoolExecutor(
            NTPROC_THREADS, NTPROC_THREADS, 60, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(NTPROC_QUEUEMAX),
            new ThreadFactoryBuilder()
                .setNameFormat(NTPROC_THREADPOOL_NAMEPREFIX + "%d")
                .build());

    // Runs any follow-up work from nt procs' run() method,
    //  including other nt procs, or other callbacks.
    // Use of synchronous queue causes direct handoff to new thread.
    // The number of threads is unbounded.
    // Threads are cached but idle out after 60 secs.
    private final ExecutorService m_priorityExecutorService = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS,
            new SynchronousQueue(),
            new ThreadFactoryBuilder()
                .setNameFormat(NTPROC_THREADPOOL_NAMEPREFIX + NTPROC_THREADPOOL_PRIORITY_SUFFIX + "%d")
                .build());

    // Used for requests for compound procs, and responses
    // from subprocs. Subprocs issued by the compound proc
    // will execute on the usual site thread. See primary
    // thread for other comments. Created only if we have
    // compound procedures loaded.
    private ExecutorService m_compoundProcExecutorService;
    private boolean m_compoundProcsExist;

    /**
     * All of the slow load-time stuff for each procedure is cached here.
     * This include stats objects, reflected method handles, etc...
     * These actually create new ProcedureRunnerNT instances for each NT
     * procedure call.
     */
    private class ProcedureRunnerNTGenerator {
        final String m_procedureName;
        final Class<? extends VoltNonTransactionalProcedure> m_procClz;
        final Method m_procMethod;
        final Class<?>[] m_paramTypes;
        final ProcedureStatsCollector m_statsCollector;
        final CompoundProcCallStats m_compoundCallStats;
        final boolean m_isCompound;

        ProcedureRunnerNTGenerator(Class<? extends VoltNonTransactionalProcedure> clz) {
            m_procClz = clz;
            m_procedureName = m_procClz.getSimpleName();
            m_isCompound = VoltCompoundProcedure.class.isAssignableFrom(clz);
            NTProcedureService.this.m_compoundProcsExist |= m_isCompound;

            // reflect
            Method procMethod = null;
            Class<?>[] paramTypes = null;

            // find either of "run()", "runUsingCalcite", or "runUsingLegacy" methods
            for (final Method m : m_procClz.getDeclaredMethods()) {
                if (Modifier.isPublic(m.getModifiers()) && m.getName().equals("run")) {
                    procMethod = m;
                    paramTypes = m.getParameterTypes();
                    break; // compiler has checked there's only one valid run() method
                }
            }

            m_procMethod = procMethod;
            m_paramTypes = paramTypes;

            // make any stats sources for this proc
            int siteId = CoreUtils.getSiteIdFromHSId(m_mailbox.getHSId());
            ProcedureStatsCollector.ProcType procType = m_isCompound
                ? ProcedureStatsCollector.ProcType.COMPOUND
                :  ProcedureStatsCollector.ProcType.NONTRANS;
            StatsAgent sa = VoltDB.instance().getStatsAgent();
            m_statsCollector = new ProcedureStatsCollector(siteId,
                                                           -1, // no partition
                                                           m_procClz.getName(), // full class name
                                                           false, // not single partition
                                                           null, // no statement list
                                                           procType);
            sa.registerStatsSource(StatsSelector.PROCEDURE, siteId, m_statsCollector);

            if (m_isCompound) {
                m_compoundCallStats = new CompoundProcCallStats(m_procClz.getName()); // same as above
                sa.registerStatsSource(StatsSelector.COMPOUNDPROCCALLS, -1, m_compoundCallStats);
            }
            else {
                m_compoundCallStats = null;
            }
        }

        /**
         * From the generator, create an actual procedure runner to be used
         * for a single invocation of an NT procedure run. We are synchronized
         * on the NTProcedureService when this method is called.
         */
        ProcedureRunnerNT generateProcedureRunnerNT(
                AuthUser user, Connection ccxn, boolean isAdmin, long ciHandle, long clientHandle, int timeout)
                throws InstantiationException, IllegalAccessException {
            // every single call gets a unique id as a key for the outstanding procedure map
            // in NTProcedureService
            long id = nextProcedureRunnerId++;
            final VoltNonTransactionalProcedure procedure = m_procClz.newInstance();
            if (m_isCompound) {
                return new CompoundProcedureRunner(
                    id, user, ccxn, isAdmin, ciHandle, clientHandle, timeout,
                    (VoltCompoundProcedure)procedure, m_procedureName,
                    m_procMethod, m_paramTypes, m_compoundProcExecutorService,
                    NTProcedureService.this, m_mailbox, m_statsCollector, m_compoundCallStats);
            } else {
                return new ProcedureRunnerNT(
                    id, user, ccxn, isAdmin, ciHandle, clientHandle, timeout,
                    procedure, m_procedureName,
                    m_procMethod, m_paramTypes, m_priorityExecutorService, // use priority to avoid deadlocks
                    NTProcedureService.this, m_mailbox, m_statsCollector);
            }
        }
    }

    NTProcedureService(ClientInterface clientInterface, InvocationDispatcher dispatcher, Mailbox mailbox) {
        assert(clientInterface != null);
        assert(mailbox != null);
        // create a specialized client response adapter for NT procs to use to call procedures
        m_internalNTClientAdapter = new LightweightNTClientResponseAdapter(ClientInterface.NT_ADAPTER_CID, dispatcher);
        clientInterface.bindAdapter(m_internalNTClientAdapter, null);
        m_mailbox = mailbox;
        m_sysProcs = loadSystemProcedures(true);
    }

    /**
     * Load the system procedures.
     * Optionally don't load UAC but use parameter instead.
     */
    @SuppressWarnings("unchecked")
    private ImmutableMap<String, ProcedureRunnerNTGenerator> loadSystemProcedures(boolean startup) {
        ImmutableMap.Builder<String, ProcedureRunnerNTGenerator> builder =
                ImmutableMap.<String, ProcedureRunnerNTGenerator>builder();

        Set<Entry<String,Config>> entrySet = SystemProcedureCatalog.listing.entrySet();
        for (Entry<String, Config> entry : entrySet) {
            String procName = entry.getKey();
            Config sysProc = entry.getValue();

            // transactional sysprocs handled by LoadedProcedureSet
            if (sysProc.transactional) {
                continue;
            }

            final String className = sysProc.getClassname();
            Class<? extends VoltNonTransactionalProcedure> procClass = null;

            // this check is for sysprocs that don't have a procedure class
            if (className != null) {
                try {
                    procClass = (Class<? extends VoltNonTransactionalProcedure>) Class.forName(className);
                } catch (final ClassNotFoundException e) {
                    if (sysProc.commercial) {
                        continue;
                    }
                    VoltDB.crashLocalVoltDB("Missing Java class for NT System Procedure: " + procName);
                }

                if (startup) {
                    // This is a startup-time check to make sure we can instantiate
                    try {
                        if (! (procClass.newInstance() instanceof VoltNTSystemProcedure)) {
                            VoltDB.crashLocalVoltDB("NT System Procedure is incorrect class type: " + procName);
                        }
                    } catch (InstantiationException | IllegalAccessException e) {
                        VoltDB.crashLocalVoltDB("Unable to instantiate NT System Procedure: " + procName);
                    }
                }
                builder.put(procName, new ProcedureRunnerNTGenerator(procClass));
            }
        }
        return builder.build();
    }

    /**
     * Stop accepting work while the cached stuff is refreshed.
     * This fixes a hole where stats gets messed up.
     */
    synchronized void preUpdate() {
        m_paused = true;
    }

    /**
     * Refresh the NT procedures when the catalog changes.
     */
    @SuppressWarnings("unchecked")
    synchronized void update(CatalogContext catalogContext) {
        CatalogMap<Procedure> procedures = catalogContext.database.getProcedures();

        Map<String, ProcedureRunnerNTGenerator> runnerGeneratorMap = new TreeMap<>();

        for (Procedure procedure : procedures) {
            if (procedure.getTransactional()) {
                continue;
            }

            // this code is mostly lifted from transactional procedures
            String className = procedure.getClassname();
            Class<? extends VoltNonTransactionalProcedure> clz = null;
            try {
                clz = (Class<? extends VoltNonTransactionalProcedure>) catalogContext.classForProcedureOrUDF(className);
            } catch (ClassNotFoundException e) {
                if (className.startsWith("org.voltdb.")) {
                    String msg = String.format(LoadedProcedureSet.ORGVOLTDB_PROCNAME_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                } else {
                    String msg = String.format(LoadedProcedureSet.UNABLETOLOAD_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }

            // The ProcedureRunnerNTGenerator has all of the dangerous and slow
            // stuff in it. Like classfinding, instantiation, and reflection.
            runnerGeneratorMap.put(procedure.getTypeName(), new ProcedureRunnerNTGenerator(clz));
        }
        m_procs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().putAll(runnerGeneratorMap).build();

        // reload all sysprocs
        loadSystemProcedures(false);

        // Start a compound proc executor if we now need one
        if (m_compoundProcsExist && m_compoundProcExecutorService == null) {
            LOG.info("Catalog contains one or more compound procedures");
            m_compoundProcExecutorService = initCompoundProcExecution();
        }

        // Set the system to start accepting work again now that everything is updated.
        // We had to stop because stats would be wonky if we called a proc while updating
        // this stuff.
        m_paused = false;

        // release all of the pending invocations into the real queue
        m_pendingInvocations.forEach(pi ->
                callProcedureNT(pi.ciHandle, pi.user, pi.ccxn, pi.isAdmin, pi.ntPriority, pi.task));
        m_pendingInvocations.clear();
    }

    /**
     * Invoke an NT procedure asynchronously on one of the exec services.
     * Principally called from InvocationDispatcher, though also called
     * from above code in the case of exiting 'paused' state.
     *
     * 'ntPriority' is used to select the executor service.
     * It is false for a request from a client to an NT proc,
     * and would be true if that NT proc called further NT procs.
     */
    synchronized void callProcedureNT(
            final long ciHandle, final AuthUser user, final Connection ccxn, final boolean isAdmin,
            final boolean ntPriority, final StoredProcedureInvocation task) {
        // If paused, stuff a record of the invocation into a queue that gets
        // drained when un-paused. We're counting on regular upstream backpressure
        // to prevent this from getting too out of hand.
        if (m_paused) {
            m_pendingInvocations.add(new PendingInvocation(ciHandle, user, ccxn, isAdmin, ntPriority, task));
            return;
        }

        String procName = task.getProcName();

        final ProcedureRunnerNTGenerator prntg;
        if (procName.startsWith("@")) {
            prntg = m_sysProcs.get(procName);
        } else {
            prntg = m_procs.get(procName);
        }

        final ProcedureRunnerNT runner;
        boolean isCompound = prntg.m_isCompound;
        if (isCompound && m_compoundProcExecutorService == null) { // should never happen
            LOG.error("Internal error: no compound procedure executor service");
            runner = null;
        }
        else {
            ProcedureRunnerNT temp;
            try {
                temp = prntg.generateProcedureRunnerNT(user, ccxn, isAdmin, ciHandle, task.getClientHandle(), task.getBatchTimeout());
            } catch (InstantiationException | IllegalAccessException e1) {
                temp = null; // error handled below
            }
            runner = temp;
        }

        // Must be done as IRM to CI mailbox for backpressure accounting
        if (runner == null) {
            ClientResponseImpl response = new ClientResponseImpl(
                    ClientResponseImpl.UNEXPECTED_FAILURE, new VoltTable[0],
                    "Could not create running context for " + procName + ".", task.getClientHandle());
            InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(
                    ciHandle, ccxn.connectionId(), response);
            m_mailbox.deliver(irm);
            return;
        }

        m_outstanding.put(runner.m_id, runner);
        startTimeout(runner, task.getRequestTimeout());

        Runnable invocationRunnable = () -> {
            try {
                runner.call(task.getParams().toArray());
            } catch (Throwable ex) {
                ex.printStackTrace();
                throw ex;
            }
        };

        // compound procedures always execute out of a dedicated thread pool.
        // for all other cases, pick the executor service based on nt priority.
        // - new (from user) txns get regular one
        // - sub tasks and sub nt procs generated by nt procs get
        //   immediate exec service (priority)
        ExecutorService exec = isCompound ? m_compoundProcExecutorService :
                               ntPriority ? m_priorityExecutorService :
                               m_primaryExecutorService;

        try {
            exec.submit(invocationRunnable);
        } catch (RejectedExecutionException e) {
            handleNTProcEnd(runner);

            // I really don't expect this to happen... but it's here.
            // must be done as IRM to CI mailbox for backpressure accounting
            // TODO: and yet it happens. Richard can repro it. Comment needs fixing.
            String err = isCompound ?
                String.format("Could not submit compound procedure %s to executor service.", procName) :
                String.format("Could not submit NT procedure %s to %s executor service.", procName,
                              ntPriority ? "priority" : "primary");
            ClientResponseImpl response =
                new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE, new VoltTable[0],
                                       err, task.getClientHandle());
            InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(
                    ciHandle, ccxn.connectionId(), response);
            m_mailbox.deliver(irm);
        }
    }

    /**
     * This absolutely must be called when a proc is done, so the set of
     * outstanding NT procs doesn't leak.
     */
    void handleNTProcEnd(ProcedureRunnerNT runner) {
        m_outstanding.remove(runner.m_id);
        cancelTimeout(runner);
    }

    /**
     * Debugging use - dump out list of outstanding NT Procs
     * to standard out. Not ordinarily used.
     */
    void dumpOutstanding() {
        boolean empty = true;
        System.out.println("Outstanding NT procs:");
        for (ProcedureRunnerNT runner : m_outstanding.values()) {
            System.out.println(runner);
            empty = false;
        }
        if (empty) {
            System.out.println("  none");
        }
    }

    /**
     * For all-host NT procs, use site failures to call callbacks for hosts
     * that will obviously never respond.
     *
     * ICH and the other plumbing should handle regular, txn procs.
     */
    void handleCallbacksForFailedHosts(final Set<Integer> failedHosts) {
        for (ProcedureRunnerNT runner : m_outstanding.values()) {
            runner.processAnyCallbacksFromFailedHosts(failedHosts);
        }
    }

    /**
     * Start a timeout on the procedure, if required by the runner
     * <p>
     * NOTE: assumes called from synchronized block
     *
     * @param runner        the procedure runner
     * @param taskTimeout   the specific timeout for this invocation, or {@code StoredProcedureInvocation.NO_TIMEOUT}
     */
    private synchronized void startTimeout(ProcedureRunnerNT runner, int taskTimeout) {
        int defaultTimeout = runner.getTimeout();
        if (defaultTimeout <= 0) {
            // Not a procedure that can time out
            return;
        }

        // If procedures that time out are used, create a timeout executor. Note that if those procedures are
        // subsequently removed, the executor will remain in order to service any active  procedures pending timeout.
        if (m_timeoutExecutor == null) {
            m_timeoutExecutor = CoreUtils.getScheduledThreadPoolExecutor("NT Procedures Timeouts", 1, CoreUtils.MEDIUM_STACK_SIZE);
            m_timeouts = new ConcurrentHashMap<>();
            LOG.info("Created NT procedure timeout executor");
        }

        // Override timeout with task timeout, if provided, and schedule it
        int effectiveTimeout = taskTimeout != StoredProcedureInvocation.NO_TIMEOUT ? taskTimeout : defaultTimeout;
        ScheduledFuture<?> timeoutTask = m_timeoutExecutor.schedule(() -> runner.timeoutCall(effectiveTimeout),
                effectiveTimeout, TimeUnit.MICROSECONDS);

        ScheduledFuture<?> prev = m_timeouts.put(runner.m_id, timeoutTask);
        assert prev == null : "Procedure " + runner.getProcedureName() + " has multiple timeouts";
    }

    /**
     * Cancel a running timeout, if required by the runner
     *
     * @param runner
     */
    private void cancelTimeout(ProcedureRunnerNT runner) {

        if (runner.getTimeout() <= 0) {
            return;
        }
        assert m_timeoutExecutor != null : "No timeout executor to handle procedure timeouts";
        assert m_timeouts != null : "No timeouts map to handle procedure timeouts";

        ScheduledFuture<?> tmo = m_timeouts.remove(runner.m_id);
        if (tmo != null) {
            tmo.cancel(false);
        }
        else if (LOG.isDebugEnabled()){
            LOG.debugFmt("Timeout %d, procedure %s was already canceled", runner.m_id, runner.getProcedureName());
        }
    }

    /*
     * Early check for compound procedure by name. Takes advantage of
     * handy runner generator we have made for every NT procedure.
     */
    boolean isCompoundProc(String procName) {
        ProcedureRunnerNTGenerator gen = m_procs.get(procName);
        return (gen != null && gen.m_isCompound);
    }

    /*
     * Set up everything for compound proc execution using parameters
     * from the deployment file.
     */
    private static ExecutorService initCompoundProcExecution() {
        int threadLimit = defaultThreadLimit;
        int queueLimit = 10_000;

        CatalogContext cat = VoltDB.instance().getCatalogContext();
        if (cat != null) {
            DeploymentType dep = cat.getDeploymentSafely();
            if (dep != null) {
                CompoundProcPolicyType pol = dep.getCompoundproc();
                if (pol != null) {
                    if (pol.getThreads() != null) {
                        threadLimit = pol.getThreads();
                    }
                    if (pol.getQueuelimit() != null) {
                        queueLimit = pol.getQueuelimit();
                    }
                    CompoundProcedureRunner.setExecutionPolicy(pol);
                }
            }
        }

        LOG.infoFmt("Compound procedure executor service: %,d threads, queue limit %,d",
                    threadLimit, queueLimit);
        ThreadPoolExecutor exec =
            new ThreadPoolExecutor(threadLimit, threadLimit,
                                   60, TimeUnit.SECONDS, // ineffective on fixed-size pool
                                   new ArrayBlockingQueue<>(queueLimit),
                                   new ThreadFactoryBuilder()
                                   .setNameFormat(COMPROC_THREADPOOL_NAMEPREFIX + "%d")
                                   .build());
        exec.prestartAllCoreThreads();
        return exec;
    }
}
