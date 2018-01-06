/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.voltcore.messaging.Mailbox;
import org.voltcore.network.Connection;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.InitiateResponseMessage;

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

    /**
     * If the NTProcedureService is paused, we add pending requests to a pending
     * list using this simple class.
     */
    static class PendingInvocation {
        final long ciHandle;
        final AuthUser user;
        final Connection ccxn;
        final boolean isAdmin;
        final boolean ntPriority;
        final StoredProcedureInvocation task;

        PendingInvocation(long ciHandle,
                          AuthUser user,
                          Connection ccxn,
                          boolean isAdmin,
                          boolean ntPriority,
                          StoredProcedureInvocation task)
        {
            this.ciHandle = ciHandle;
            this.user = user;
            this.ccxn = ccxn;
            this.isAdmin = isAdmin;
            this.ntPriority = ntPriority;
            this.task = task;
        }
    }

    // User-supplied non-transactional procedures
    ImmutableMap<String, ProcedureRunnerNTGenerator> m_procs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().build();
    // Non-transactional system procedures
    ImmutableMap<String, ProcedureRunnerNTGenerator> m_sysProcs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().build();

    // A tracker of currently executing procedures by id, where id is a long that increments with each call
    Map<Long, ProcedureRunnerNT> m_outstanding = new ConcurrentHashMap<>();
    // This lets us respond over the network directly
    final LightweightNTClientResponseAdapter m_internalNTClientAdapter;
    // Mailbox for the client interface is used to send messages directly to other nodes (sysprocs only)
    final Mailbox m_mailbox;
    // We pause the service mid-catalog update for stats reasons
    boolean m_paused = false;
    // Transactions that arrived when paused (should always be empty if not paused)
    Queue<PendingInvocation> m_pendingInvocations = new ArrayDeque<>();
    // increments for every procedure call
    long nextProcedureRunnerId = 0;

    // no need for thread safety as this can't race with @UAC at startup
    public boolean isRestoring;

    // names for threads in the exec service
    final static String NTPROC_THREADPOOL_NAMEPREFIX = "NTPServiceThread-";
    final static String NTPROC_THREADPOOL_PRIORITY_SUFFIX = "Priority-";

    public static final String NTPROCEDURE_RUN_EVERYWHERE_TIMEOUT = "NTPROCEDURE_RUN_EVERYWHERE_TIMEOUT";

    // runs the initial run() method of nt procs
    // (doesn't run nt procs if started by other nt procs)
    // from 2 to 20 threads in parallel, with a bounded queue
    private final ExecutorService m_primaryExecutorService = new ThreadPoolExecutor(
            2,
            20,
            60,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(10000),
            new ThreadFactoryBuilder()
                .setNameFormat(NTPROC_THREADPOOL_NAMEPREFIX + "%d")
                .build());

    // runs any follow-up work from nt procs' run() method,
    // including other nt procs, or other callbacks.
    // This one has no unbounded queue, but will create an unbounded number of threads
    // hopefully the number of actual threads will be limited by the number of concurrent
    // nt procs running in the first queue.
    // No unbounded queue here -- direct handoff of work to thread
    // note: threads are cached by default
    private final ExecutorService m_priorityExecutorService = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat(NTPROC_THREADPOOL_NAMEPREFIX + NTPROC_THREADPOOL_PRIORITY_SUFFIX + "%d")
                .build());

    /**
     * All of the slow load-time stuff for each procedure is cached here.
     * This include stats objects, reflected method handles, etc...
     * These actually create new ProcedureRunnerNT instances for each NT
     * procedure call.
     */
    class ProcedureRunnerNTGenerator {

        protected final String m_procedureName;
        protected final Class<? extends VoltNonTransactionalProcedure> m_procClz;
        protected final Method m_procMethod;
        protected final Class<?>[] m_paramTypes;
        protected final ProcedureStatsCollector m_statsCollector;

        ProcedureRunnerNTGenerator(Class<? extends VoltNonTransactionalProcedure> clz) {
            m_procClz = clz;
            m_procedureName = m_procClz.getSimpleName();

            // reflect
            Method procMethod = null;
            Class<?>[] paramTypes = null;

            Method[] methods = m_procClz.getDeclaredMethods();

            // find the "run()" method
            for (final Method m : methods) {
                String name = m.getName();
                if (name.equals("run")) {
                    if (Modifier.isPublic(m.getModifiers()) == false) {
                        continue;
                    }
                    procMethod = m;
                    paramTypes = m.getParameterTypes();
                    break; // compiler has checked there's only one valid run() method
                }
            }

            m_procMethod = procMethod;
            m_paramTypes = paramTypes;

            // make a stats source for this proc
            m_statsCollector = new ProcedureStatsCollector(
                    CoreUtils.getSiteIdFromHSId(m_mailbox.getHSId()),
                    0,
                    m_procClz.getName(),
                    false,
                    null,
                    false);
            VoltDB.instance().getStatsAgent().registerStatsSource(
                    StatsSelector.PROCEDURE,
                    CoreUtils.getSiteIdFromHSId(m_mailbox.getHSId()),
                    m_statsCollector);
        }

        /**
         * From the generator, create an actual procedure runner to be used
         * for a single invocation of an NT procedure run.
         */
        ProcedureRunnerNT generateProcedureRunnerNT(AuthUser user,
                                                    Connection ccxn,
                                                    boolean isAdmin,
                                                    long ciHandle,
                                                    long clientHandle,
                                                    int timeout)
                throws InstantiationException, IllegalAccessException
        {
            // every single call gets a unique id as a key for the outstanding procedure map
            // in NTProcedureService
            long id = nextProcedureRunnerId++;

            VoltNonTransactionalProcedure procedure = null;
            procedure = m_procClz.newInstance();
            ProcedureRunnerNT runner = new ProcedureRunnerNT(id,
                                                             user,
                                                             ccxn,
                                                             isAdmin,
                                                             ciHandle,
                                                             clientHandle,
                                                             timeout,
                                                             procedure,
                                                             m_procedureName,
                                                             m_procMethod,
                                                             m_paramTypes,
                                                             // use priority to avoid deadlocks
                                                             m_priorityExecutorService,
                                                             NTProcedureService.this,
                                                             m_mailbox,
                                                             m_statsCollector);
            return runner;
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
                }
                catch (final ClassNotFoundException e) {
                    if (sysProc.commercial) {
                        continue;
                    }
                    VoltDB.crashLocalVoltDB("Missing Java class for NT System Procedure: " + procName);
                }

                if (startup) {
                    // This is a startup-time check to make sure we can instantiate
                    try {
                        if ((procClass.newInstance() instanceof VoltNTSystemProcedure) == false) {
                            VoltDB.crashLocalVoltDB("NT System Procedure is incorrect class type: " + procName);
                        }
                    }
                    catch (InstantiationException | IllegalAccessException e) {
                        VoltDB.crashLocalVoltDB("Unable to instantiate NT System Procedure: " + procName);
                    }
                }

                ProcedureRunnerNTGenerator prntg = new ProcedureRunnerNTGenerator(procClass);
                builder.put(procName, prntg);
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
                }
                else {
                    String msg = String.format(LoadedProcedureSet.UNABLETOLOAD_ERROR_FMT, className);
                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }

            // The ProcedureRunnerNTGenerator has all of the dangerous and slow
            // stuff in it. Like classfinding, instantiation, and reflection.
            ProcedureRunnerNTGenerator prntg = new ProcedureRunnerNTGenerator(clz);
            runnerGeneratorMap.put(procedure.getTypeName(), prntg);
        }

        m_procs = ImmutableMap.<String, ProcedureRunnerNTGenerator>builder().putAll(runnerGeneratorMap).build();

        // reload all sysprocs
        loadSystemProcedures(false);

        // Set the system to start accepting work again now that ebertything is updated.
        // We had to stop because stats would be wonky if we called a proc while updating
        // this stuff.
        m_paused = false;

        // release all of the pending invocations into the real queue
        m_pendingInvocations
            .forEach(pi -> callProcedureNT(pi.ciHandle, pi.user, pi.ccxn, pi.isAdmin,
                     pi.ntPriority, pi.task));
        m_pendingInvocations.clear();
    }

    /**
     * Invoke an NT procedure asynchronously on one of the exec services.
     * @returns ClientResponseImpl if something goes wrong.
     */
    synchronized void callProcedureNT(final long ciHandle,
                                      final AuthUser user,
                                      final Connection ccxn,
                                      final boolean isAdmin,
                                      final boolean ntPriority,
                                      final StoredProcedureInvocation task)
    {
        // If paused, stuff a record of the invocation into a queue that gets
        // drained when un-paused. We're counting on regular upstream backpressure
        // to prevent this from getting too out of hand.
        if (m_paused) {
            PendingInvocation pi = new PendingInvocation(ciHandle, user, ccxn, isAdmin, ntPriority, task);
            m_pendingInvocations.add(pi);
            return;
        }

        String procName = task.getProcName();

        final ProcedureRunnerNTGenerator prntg;
        if (procName.startsWith("@")) {
            prntg = m_sysProcs.get(procName);
        }
        else {
            prntg = m_procs.get(procName);
        }

        final ProcedureRunnerNT runner;
        try {
            runner = prntg.generateProcedureRunnerNT(user, ccxn, isAdmin, ciHandle, task.getClientHandle(), task.getBatchTimeout());
        } catch (InstantiationException | IllegalAccessException e1) {
            // I don't expect to hit this, but it's here...
            // must be done as IRM to CI mailbox for backpressure accounting
            ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                                                 new VoltTable[0],
                                                                 "Could not create running context for " + procName + ".",
                                                                 task.getClientHandle());
            InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(ciHandle,
                                                                                           ccxn.connectionId(),
                                                                                           response);
            m_mailbox.deliver(irm);
            return;
        }
        m_outstanding.put(runner.m_id, runner);

        Runnable invocationRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    runner.call(task.getParams().toArray());
                }
                catch (Throwable ex) {
                    ex.printStackTrace();
                    throw ex;
                }
            }
        };

        try {
            // pick the executor service based on priority
            // - new (from user) txns get regular one
            // - sub tasks and sub procs generated by nt procs get
            //   immediate exec service (priority)
            if (ntPriority) {
                m_priorityExecutorService.submit(invocationRunnable);
            }
            else {
                m_primaryExecutorService.submit(invocationRunnable);
            }
        }
        catch (RejectedExecutionException e) {
            handleNTProcEnd(runner);

            // I really don't expect this to happen... but it's here.
            // must be done as IRM to CI mailbox for backpressure accounting
            ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                                                 new VoltTable[0],
                                                                 "Could not submit NT procedure " + procName + " to exec service for .",
                                                                 task.getClientHandle());
            InitiateResponseMessage irm = InitiateResponseMessage.messageForNTProcResponse(ciHandle,
                                                                                           ccxn.connectionId(),
                                                                                           response);
            m_mailbox.deliver(irm);
            return;
        }
    }

    /**
     * This absolutely must be called when a proc is done, so the set of
     * outstanding NT procs doesn't leak.
     */
    void handleNTProcEnd(ProcedureRunnerNT runner) {
        m_outstanding.remove(runner.m_id);
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
}
