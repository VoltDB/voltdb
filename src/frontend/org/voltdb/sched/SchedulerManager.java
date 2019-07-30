/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.sched;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.ClientInterfaceRepairCallback;
import org.voltdb.ParameterConverter;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SimpleClientResponseAdapter;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.ProcedureSchedule;
import org.voltdb.catalog.SchedulerParam;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.ListeningScheduledExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

/**
 * Manager for the life cycle of the current set of configured {@link Scheduler}s. Each schedule configuration is
 * represented by a {@link SchedulerHandler}. Each {@link SchedulerHandler} will hold a set of {@link SchedulerWrapper}s
 * which are used to wrap the execution of the scheduler and procedures which are scheduled.
 * <p>
 * The scheduler manager will execute procedures which are scheduled to run on each host, system if this host is the
 * leader and all locally led partitions.
 * <p>
 * All manager tasks will be executed in the {@link #m_managerExecutor} as well as {@link SchedulerHandler} methods. The
 * execution of {@link SchedulerWrapper} instances will be split between the {@link #m_managerExecutor} and
 * {@link #m_wrapperExecutor}. Pure management calls will be executed in the {@link #m_managerExecutor} while
 * scheduled procedures, results and calls to {@link Scheduler}s will be handled in the {@link #m_wrapperExecutor}
 */
public class SchedulerManager {
    static final VoltLogger log = new VoltLogger("HOST");
    static final String RUN_LOCATION_SYSTEM = "SYSTEM";
    static final String RUN_LOCATION_HOSTS = "HOSTS";
    static final String RUN_LOCATION_PARTITIONS = "PARTITIONS";
    static final String HASH_ALGO = "SHA-512";
    public static final String RUN_LOCATION_DEFAULT = RUN_LOCATION_SYSTEM;

    private Map<String, SchedulerHandler> m_handlers = Collections.emptyMap();
    private volatile boolean m_leader = false;
    private AuthSystem m_authSystem;
    private boolean m_started = false;
    private final Set<Integer> m_locallyLedPartitions = new HashSet<>();
    private final SimpleClientResponseAdapter m_adapter = new SimpleClientResponseAdapter(
            ClientInterface.SCHEDULER_MANAGER_CID, getClass().getSimpleName());

    // Local host ID
    final int m_hostId;

    // Used by the manager to perform management functions so a scheduler can not hold up the manager
    private final ListeningExecutorService m_managerExecutor = MoreExecutors
            .listeningDecorator(CoreUtils.getSingleThreadExecutor(getClass().getSimpleName()));

    // Used to execute the schedulers and scheduled procedures only used by ScheculerWrappers
    // TODO make thread count configurable for the wrapper executor
    final ListeningScheduledExecutorService m_wrapperExecutor = MoreExecutors.listeningDecorator(CoreUtils.getScheduledThreadPoolExecutor("ProcedureScheduler",
                    1, CoreUtils.SMALL_STACK_SIZE));
    final ClientInterface m_clientInterface;

    static String generateLogMessage(String name, String body) {
        return String.format("Schedule (%s): %s", name, body);
    }

    /**
     * Create a factory supplier for instances of {@link Scheduler} as defined by the provided
     * {@link ProcedureSchedule}. If an instance of {@link Scheduler} cannot be constructed using the provided
     * configuration {@code null} is returned and a detailed error message will be logged.
     *
     * @param definition  {@link ProcedureSchedule} defining the configuration of the schedule
     * @param classLoader {@link ClassLoader} to use when loading the {@link Scheduler} in {@code definition}
     * @return {@link SchedulerValidationResult} describing any problems encountered
     */
    public static SchedulerValidationResult validateScheduler(ProcedureSchedule definition, ClassLoader classLoader) {
        String schedulerClassString = definition.getSchedulerclass();
        try {
            Class<?> schedulerClass;
            try {
                schedulerClass = classLoader.loadClass(schedulerClassString);
            } catch (ClassNotFoundException e) {
                return new SchedulerValidationResult("Scheduler class does not exist: " + schedulerClassString);
            }
            if (!Scheduler.class.isAssignableFrom(schedulerClass)) {
                return new SchedulerValidationResult(String.format("Class %s is not an instance of %s",
                        schedulerClassString, Scheduler.class.getName()));
            }

            Constructor<?>[] constructors = schedulerClass.getConstructors();
            if (constructors.length != 1) {
                return new SchedulerValidationResult(String.format("Scheduler class should have 1 constructor %s has %d",
                        schedulerClassString, constructors.length));
            }

            @SuppressWarnings("unchecked")
            Constructor<Scheduler> constructor = (Constructor<Scheduler>) constructors[0];
            CatalogMap<SchedulerParam> schedulerParams = definition.getParameters();
            int actualParamCount = schedulerParams == null ? 0 : schedulerParams.size();
            int minVarArgParamCount = isLastParamaterVarArgs(constructor) ? constructor.getParameterCount() - 1
                    : Integer.MAX_VALUE;
            if (constructor.getParameterCount() != actualParamCount && minVarArgParamCount > actualParamCount) {
                return new SchedulerValidationResult(String.format(
                        "Scheduler class, %s, constructor paremeter count %d does not match provided parameter count %d",
                        schedulerClassString, constructor.getParameterCount(), schedulerParams.size()));
            }

            Object[] parameters;
            if (schedulerParams == null || schedulerParams.isEmpty()) {
                parameters = ArrayUtils.EMPTY_OBJECT_ARRAY;
            } else {
                Class<?>[] parameterTypes = constructor.getParameterTypes();
                parameters = new Object[constructor.getParameterCount()];
                String[] varArgParams = null;
                if (minVarArgParamCount < Integer.MAX_VALUE) {
                    parameters[parameters.length
                            - 1] = varArgParams = new String[actualParamCount - minVarArgParamCount];
                }
                for (SchedulerParam sp : schedulerParams) {
                    int index = sp.getIndex();
                    if (index < minVarArgParamCount) {
                        try {
                            parameters[index] = ParameterConverter.tryToMakeCompatible(parameterTypes[index],
                                    sp.getParameter());
                        } catch (Exception e) {
                            return new SchedulerValidationResult(String.format(
                                    "Could not convert parameter %d with the value \"%s\" to type %s: %s",
                                    sp.getIndex(), sp.getParameter(), parameterTypes[index].getName(), e.getMessage()));
                        }
                    } else {
                        varArgParams[index - minVarArgParamCount] = sp.getParameter();
                    }
                }
            }

            byte[] hash = null;
            if (classLoader instanceof InMemoryJarfile.JarLoader) {
                InMemoryJarfile jarFile = ((InMemoryJarfile.JarLoader) classLoader).getInMemoryJarfile();
                hash = jarFile.getClassHash(schedulerClassString, HASH_ALGO);
            }

            return new SchedulerValidationResult(new SchedulerFactory(constructor, parameters, hash));
        } catch (Exception e) {
            return new SchedulerValidationResult(
                    String.format("Could not load and construct class: %s", schedulerClassString), e);
        }
    }

    private static boolean isLastParamaterVarArgs(Constructor<Scheduler> constructor) {
        if (constructor.getParameterCount() == 0) {
            return false;
        }
        Parameter[] params = constructor.getParameters();
        Parameter lastParam = params[params.length - 1];
        return lastParam.getType() == String[].class || lastParam.getType() == Object[].class;
    }

    public SchedulerManager(ClientInterface clientInterface, int hostId) {
        m_clientInterface = clientInterface;
        m_hostId = hostId;

        m_clientInterface.bindAdapter(m_adapter, new ClientInterfaceRepairCallback() {
            Map<Integer, Future<Boolean>> m_migratingPartitions = Collections.synchronizedMap(new HashMap<>());

            @Override
            public void repairCompleted(int partitionId, long initiatorHSId) {
                promoteIfLocal(partitionId, initiatorHSId);
            }

            @Override
            public void leaderMigrationStarted(int partitionId, long initiatorHSId) {
                if (!isLocalHost(initiatorHSId)) {
                    m_migratingPartitions.put(partitionId, demotedPartition(partitionId));
                }
            }

            @Override
            public void leaderMigrationFailed(int partitionId, long initiatorHSId) {
                try {
                    if (m_migratingPartitions.remove(partitionId).get().booleanValue()) {
                        promotedPartition(partitionId);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("Demote future encountered an enexpected error", e);

                    if (isLocalHost(VoltDB.instance().getCartographer().getHSIdForMaster(partitionId))) {
                        promotedPartition(partitionId);
                    }
                }
            }

            @Override
            public void leaderMigrated(int partitionId, long initiatorHSId) {
                m_migratingPartitions.remove(partitionId);
                promoteIfLocal(partitionId, initiatorHSId);
            }

            private void promoteIfLocal(int partitionId, long initiatorHSId) {
                if (partitionId == MpInitiator.MP_INIT_PID) {
                    return;
                }
                if (isLocalHost(initiatorHSId)) {
                    promotedPartition(partitionId);
                }
            }

            private boolean isLocalHost(long hsId) {
                return CoreUtils.getHostIdFromHSId(hsId) == m_hostId;
            }
        });
    }

    /**
     * Asynchronously start the scheduler manager and any configured schedules which are eligible to be run on this
     * host.
     *
     * @param context {@link CatalogContext} instance
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> start(CatalogContext context) {
        return start(context.database.getProcedureschedules(), context.authSystem, context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously start the scheduler manager and any configured schedules which are eligible to be run on this
     * host.
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load configured {@link Scheduler}s
     *
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> start(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            ClassLoader classLoader) {
        return execute(() -> {
            m_started = true;
            processCatalogInline(procedureSchedules, authSystem, classLoader);
        });
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param context {@link CatalogContext} instance
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> promoteToLeader(CatalogContext context) {
        return promoteToLeader(context.database.getProcedureschedules(), context.authSystem,
                context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load configured {@link Scheduler}s
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> promoteToLeader(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            ClassLoader classLoader) {
        return execute(() -> {
            m_leader = true;
            processCatalogInline(procedureSchedules, authSystem, classLoader);
        });
    }

    /**
     * Asynchronously process an update to the scheduler configuration
     *
     * @param context     {@link CatalogContext} instance
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> processUpdate(CatalogContext context) {
        return processUpdate(context.database.getProcedureschedules(), context.authSystem,
                context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously process an update to the scheduler configuration
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load configured {@link Scheduler}s
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> processUpdate(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            ClassLoader classLoader) {
        return execute(() -> processCatalogInline(procedureSchedules, authSystem, classLoader));
    }

    /**
     * Notify the manager that some local partitions have been promoted to leader. Any PARTITION schedules will be
     * asynchronously started for these partitions.
     *
     * @param partitionId which was promoted
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> promotedPartition(int partitionId) {
        return execute(() -> {
            for (SchedulerHandler sd : m_handlers.values()) {
                sd.promotedPartition(partitionId);
            }
            m_locallyLedPartitions.add(partitionId);
        });
    }

    /**
     * Notify the manager that a local partition has been demoted from leader. Any PARTITION schedules will be
     * asynchronously stopped for these partitions.
     *
     * @param partitionId which was demoted
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<Boolean> demotedPartition(int partitionId) {
        return execute(() -> {
            if (!m_locallyLedPartitions.remove(partitionId)) {
                return false;
            }
            for (SchedulerHandler sd : m_handlers.values()) {
                sd.demotedPartition(partitionId);
            }
            return true;
        });
    }

    /**
     * Asynchronously shutdown the scheduler manager and cancel any outstanding scheduled procedures
     *
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> shutdown() {
        try {
            return m_managerExecutor.submit(() -> {
                m_managerExecutor.shutdown();
                m_started = false;
                Map<String, SchedulerHandler> handlers = m_handlers;
                m_handlers = Collections.emptyMap();
                handlers.values().stream().forEach(SchedulerHandler::cancel);
                m_wrapperExecutor.shutdown();
            });
        } catch (RejectedExecutionException e) {
            return Futures.immediateFuture(null);
        }
    }

    ListenableFuture<?> execute(Runnable runnable) {
        try {
            return addExceptionListener(m_managerExecutor.submit(runnable));
        } catch (RejectedExecutionException e) {
            if (log.isDebugEnabled()) {
                log.debug(generateLogMessage("NONE", "Could not execute " + runnable), e);
            }
            return Futures.immediateFailedFuture(e);
        }
    }

    <T> ListenableFuture<T> execute(Callable<T> callable) {
        try {
            return addExceptionListener(m_managerExecutor.submit(callable));
        } catch (RejectedExecutionException e) {
            if (log.isDebugEnabled()) {
                log.debug(generateLogMessage("NONE", "Could not execute " + callable), e);
            }
            return Futures.immediateFailedFuture(e);
        }
    }

    AuthUser getUser(String userName) {
        return m_authSystem.getUser(userName);
    }

    <T> ListenableFuture<T> addExceptionListener(ListenableFuture<T> future) {
        future.addListener(() -> {
            try {
                if (!future.isCancelled()) {
                    future.get();
                }
            } catch (Exception e) {
                log.error(generateLogMessage("NONE", "Unexected exception encountered"), e);
            }
        }, MoreExecutors.newDirectExecutorService());
        return future;
    }

    /**
     * Process any potential scheduler changes. Any modified schedules will be stopped and restarted with their new
     * configuration. If a schedule was not modified it will be left running.
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load {@link Scheduler} classes
     */
    private void processCatalogInline(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            ClassLoader classLoader) {
        if (!m_started) {
            return;
        }

        Map<String, SchedulerHandler> newHandlers = new HashMap<>();
        m_authSystem = authSystem;

        for (ProcedureSchedule procedureSchedule : procedureSchedules) {
            SchedulerHandler handler = m_handlers.remove(procedureSchedule.getName());
            SchedulerValidationResult result = validateScheduler(procedureSchedule, classLoader);

            if (handler != null) {
                // Do not restart a schedule if it has not changed
                if (handler.isSameSchedule(procedureSchedule, result.m_factory)) {
                    newHandlers.put(procedureSchedule.getName(), handler);
                    continue;
                }
                handler.cancel();
            }

            String runLocation = procedureSchedule.getRunlocation().toUpperCase();
            if (procedureSchedule.getEnabled()
                    && (m_leader || !RUN_LOCATION_SYSTEM.equals(runLocation))) {
                if (!result.isValid()) {
                    log.warn(generateLogMessage(procedureSchedule.getName(), result.getErrorMessage()),
                            result.getException());
                    continue;
                }

                SchedulerHandler definition;
                switch (runLocation) {
                case RUN_LOCATION_HOSTS:
                case RUN_LOCATION_SYSTEM:
                    definition = new SingleSchedulerHandler(procedureSchedule, runLocation, result.m_factory);
                    break;
                case RUN_LOCATION_PARTITIONS:
                    definition = new PartitionedScheduleHandler(procedureSchedule, result.m_factory);
                    m_locallyLedPartitions.forEach(definition::promotedPartition);
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported run location: " + procedureSchedule.getRunlocation());
                }
                newHandlers.put(procedureSchedule.getName(), definition);
            }
        }

        // Cancel all removed schedules
        for (SchedulerHandler handler : m_handlers.values()) {
            handler.cancel();
        }

        // Start all current schedules. This is a no-op for already started schedules
        for (SchedulerHandler handler : newHandlers.values()) {
            handler.start();
        }

        m_handlers = newHandlers;
    }

    /**
     * Result object returned by {@link SchedulerManager#validateScheduler(ProcedureSchedule, ClassLoader)}. Used to
     * determine if the {@link Scheduler} and {@link ProcedureSchedule#getParameters()} in a {@link ProcedureSchedule}
     * are valid. If they are not valid then an error message and potential exception are contained within this result
     * describing the problem.
     */
    public static final class SchedulerValidationResult {
        final String m_errorMessage;
        final Exception m_exception;
        final SchedulerFactory m_factory;

        SchedulerValidationResult(String errorMessage) {
            this(errorMessage, null);
        }

        SchedulerValidationResult(String errorMessage, Exception exception) {
            m_errorMessage = errorMessage;
            m_exception = exception;
            m_factory = null;
        }

        SchedulerValidationResult(SchedulerFactory factory) {
            m_errorMessage = null;
            m_exception = null;
            m_factory = factory;
        }

        /**
         * @return {@code true} if the scheduler and parameters which were tested are valid
         */
        public boolean isValid() {
            return m_factory != null;
        }

        /**
         * @return Description of invalid scheduler and parameters
         */
        public String getErrorMessage() {
            return m_errorMessage;
        }

        /**
         * @return Any unexpected exception which was caught while validating scheduler and parameters
         */
        public Exception getException() {
            return m_exception;
        }
    }

    /**
     * Base class for wrapping a single scheduler configuration.
     */
    private abstract class SchedulerHandler {
        private final ProcedureSchedule m_definition;
        private final SchedulerFactory m_factory;

        SchedulerHandler(ProcedureSchedule definition, SchedulerFactory factory) {
            m_definition = definition;
            m_factory = factory;
        }

        /**
         * @param definition {@link ProcedureSchedule} defining the schedule configuration
         * @param factory    {@link SchedulerFactory} derived from {@code definition}
         * @return {@code true} if both {@code definition} and {@code factory} match those in this handler
         */
        boolean isSameSchedule(ProcedureSchedule definition, SchedulerFactory factory) {
            return m_definition.equals(definition) && m_factory.hashesMatch(factory);
        }

        String getName() {
            return m_definition.getName();
        }

        String getUser() {
            return m_definition.getUser();
        }

        /**
         * Start executing this configured scheduler
         */
        abstract void start();

        @Override
        public String toString() {
            return m_definition.getName();
        }

        /**
         * Cancel any pending executions of this scheduler and end its life cycle
         */
        abstract void cancel();

        /**
         * Notify this scheduler configuration of partitions which were locally promoted to leader
         *
         * @param partition which was promoted
         */
        abstract void promotedPartition(int partitionId);

        /**
         * Notify this scheduler configuration of partitions which were locally demoted from leader
         *
         * @param partition which was demoted
         */
        abstract void demotedPartition(int partitionId);

        Scheduler constructScheduler() {
            return m_factory.construct();
        }

        String generateLogMessage(String body) {
            return SchedulerManager.generateLogMessage(m_definition.getName(), body);
        }
    }

    /**
     * An instance of {@link SchedulerHandler} which contains a single {@link SchedulerWrapper}. This is used for
     * schedules which are configured for {@link SchedulerManager#RUN_LOCATION_SYSTEM} or
     * {@link SchedulerManager#RUN_LOCATION_HOSTS}.
     */
    private class SingleSchedulerHandler extends SchedulerHandler {
        private final SchedulerWrapper<? extends SingleSchedulerHandler> m_wrapper;

        SingleSchedulerHandler(ProcedureSchedule definition, String runLocation, SchedulerFactory factory) {
            super(definition, factory);

            Scheduler scheduler = constructScheduler();
            switch (runLocation) {
            case RUN_LOCATION_HOSTS:
                m_wrapper = new HostSchedulerWrapper(this, scheduler);
                break;
            case RUN_LOCATION_SYSTEM:
                m_wrapper = new SystemSchedulerWrapper(this, scheduler);
                break;
            default:
                throw new IllegalArgumentException("Invalid run location: " + runLocation);
            }
        }

        @Override
        void cancel() {
            m_wrapper.cancel();
        }

        @Override
        void promotedPartition(int partitionId) {}

        @Override
        void demotedPartition(int partitionId) {}

        @Override
        void start() {
            m_wrapper.start();
        }
    }

    /**
     * An instance of {@link SchedulerHandler} which contains a {@link SchedulerWrapper} for each locally led partition.
     * This is used for schedules which are configured for {@link SchedulerManager#RUN_LOCATION_PARTITIONS}.
     */
    private class PartitionedScheduleHandler extends SchedulerHandler {
        private final Map<Integer, PartitionSchedulerWrapper> m_wrappers = new HashMap<>();
        private boolean m_started = false;

        PartitionedScheduleHandler(ProcedureSchedule definition, SchedulerFactory factory) {
            super(definition, factory);
        }

        @Override
        void cancel() {
            for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                wrapper.cancel();
            }
            m_wrappers.clear();
        }

        @Override
        void promotedPartition(int partitionId) {
            assert !m_wrappers.containsKey(partitionId);
            PartitionSchedulerWrapper wrapper = new PartitionSchedulerWrapper(this, constructScheduler(), partitionId);

            m_wrappers.put(partitionId, wrapper);

            if (m_started) {
                wrapper.start();
            }
        }

        @Override
        void demotedPartition(int partitionId) {
            PartitionSchedulerWrapper wrapper;
            wrapper = m_wrappers.remove(partitionId);
            if (wrapper != null) {
                wrapper.cancel();
            }
        }

        @Override
        void start() {
            if (!m_started) {
                m_started = true;

                for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                    wrapper.start();
                }
            }
        }
    }

    /**
     * Enum to represent the current state of a {@link SchedulerWrapper}
     */
    private enum SchedulerWrapperState {
        /** Scheduler wrapper initialized but not started yet */
        INITIALIZED,
        /** Scheduler is currently active and running */
        RUNNING,
        /** Scheduler has encountered an unrecoverable error and exited */
        ERROR,
        /** Scheduler has exited gracefully */
        EXITED,
        /** Scheduler was cancelled by the manager either because of shutdown or configuration modification */
        CANCELED;
    }

    /**
     * Base class which wraps the execution and handling of a single {@link Scheduler} instance.
     * <p>
     * On start {@link Scheduler#nextRun(ScheduledProcedure)} is invoked with a null {@link ScheduledProcedure}
     * argument. If the result has a status of {@link SchedulerResult.Status#PROCEDURE} then the provided procedure will
     * be scheduled and executed after the delay. Once a response is received
     * {@link Scheduler#nextRun(ScheduledProcedure)} will be invoked again with the {@link ScheduledProcedure}
     * previously returned and {@link ScheduledProcedure#getResponse()} updated with response. This repeats until this
     * wrapper is cancelled or {@link Scheduler#nextRun(ScheduledProcedure)} returns a non schedule result.
     * <p>
     * This class needs to be thread safe since it's execution is split between the
     * {@link SchedulerManager#m_managerExecutor} and the {@link SchedulerManager#m_wrapperExecutor}
     *
     * @param <H> Type of {@link SchedulerHandler} which created this wrapper
     */
    private abstract class SchedulerWrapper<H extends SchedulerHandler> {
        ScheduledProcedure m_procedure;

        final H m_handler;

        private final Scheduler m_scheduler;
        private Future<?> m_scheduledFuture;
        private volatile SchedulerWrapperState m_state = SchedulerWrapperState.INITIALIZED;

        SchedulerWrapper(H handler, Scheduler scheduler) {
            m_handler = handler;
            m_scheduler = scheduler;
        }

        /**
         * Start running the scheduler
         */
        synchronized void start() {
            if (m_state != SchedulerWrapperState.INITIALIZED) {
                return;
            }
            m_state = SchedulerWrapperState.RUNNING;
            submitHandleNextRun();
        }

        /**
         * Call {@link Scheduler#nextRun(ScheduledProcedure)} and process the result including scheduling the next
         * procedure to run
         * <p>
         * NOTE: method is not synchronized so the lock is not held during the scheduler execution
         */
        private void handleNextRun() {
            if (m_state != SchedulerWrapperState.RUNNING) {
                return;
            }

            SchedulerResult result;
            try {
                result = m_scheduler.nextRun(m_procedure);
            } catch (RuntimeException e) {
                errorOccurred("Scheduler encountered unexpected error", e);
                return;
            }

            if (result == null) {
                errorOccurred("Scheduler returned a null result");
                return;
            }

            synchronized (this) {
                if (m_state != SchedulerWrapperState.RUNNING) {
                    return;
                }

                Runnable runnable;
                switch (result.getStatus()) {
                case EXIT:
                    exitRequested(result.getMessage());
                    return;
                case ERROR:
                    if (result.hasMessage()) {
                        log.warn(generateLogMessage(result.getMessage()));
                    }
                    errorOccurred(null);
                    return;
                case RERUN:
                    runnable = this::handleNextRun;
                    break;
                case PROCEDURE:
                    runnable = this::executeProcedure;
                    break;
                default:
                    throw new IllegalStateException("Unknown status: " + result.getStatus());
                }

                m_procedure = result.getScheduledProcedure();

                try {
                    m_scheduledFuture = addExceptionListener(
                            m_wrapperExecutor.schedule(runnable, calculateDelay(), TimeUnit.NANOSECONDS));
                    m_procedure.scheduled();
                } catch (RejectedExecutionException e) {
                    if (log.isDebugEnabled()) {
                        log.debug(generateLogMessage(
                                "Could not schedule next procedure scheduler shutdown: " + m_procedure.getProcedure()));
                    }
                }
            }
        }

        /**
         * Execute a scheduled procedure
         */
        private synchronized void executeProcedure() {
            Procedure procedure = getProcedureDefinition();
            if (procedure == null) {
                return;
            }

            Object[] procedureParameters = getProcedureParameters(procedure);
            if (procedureParameters == null) {
                return;
            }

            String userName = m_handler.getUser();
            AuthUser user = getUser(userName);
            if (user == null) {
                errorOccurred("User %s does not exist", userName);
                return;
            }

            m_procedure.setStarted();
            if (!m_clientInterface.getInternalConnectionHandler().callProcedure(user, false,
                    BatchTimeoutOverrideType.NO_TIMEOUT, this::handleResponse, m_procedure.getProcedure(),
                    procedureParameters)) {
                errorOccurred("Could not call procedure %s", m_procedure.getProcedure());
            }
        }

        private synchronized void handleResponse(ClientResponse response) {
            if (m_state != SchedulerWrapperState.RUNNING) {
                return;
            }
            m_procedure.setResponse(response);
            submitHandleNextRun();
        }

        private synchronized void submitHandleNextRun() {
            try {
                addExceptionListener(m_wrapperExecutor.submit(this::handleNextRun));
            } catch (RejectedExecutionException e) {
                if (log.isDebugEnabled()) {
                    log.debug(generateLogMessage("Execution of response handler rejected"), e);
                }
            }
        }

        /**
         * Test if the procedure is valid to be run by this wrapper
         *
         * @param procedure to be run
         * @return {@code true} if the procedure can be run
         */
        abstract boolean isValidProcedure(Procedure procedure);

        /**
         * Shutdown the scheduler with a cancel state
         */
        void cancel() {
            shutdown(SchedulerWrapperState.CANCELED);
        }

        /**
         * Generate the parameters to pass to the procedure during execution
         *
         * @param procedure being executed
         * @return Parameters to use with the procedure to execute or {@code null} if there was an error generating the
         *         parameters
         */
        Object[] getProcedureParameters(Procedure procedure) {
            return m_procedure.getProcedureParameters();
        }

        /**
         * @return The {@link Procedure} definition for the procedure in {@link #m_procedure} or {@code null} if an
         *         error was encountered.
         */
        private Procedure getProcedureDefinition() {
            String procedureName = m_procedure.getProcedure();
            Procedure procedure = m_clientInterface.getProcedureFromName(procedureName);

            if (procedure == null) {
                errorOccurred("Procedure does not exist: %s", procedureName);
                return null;
            }

            return isValidProcedure(procedure) ? procedure : null;
        }

        /**
         * Log an error message and shutdown the scheduler with the error state
         *
         * @param errorMessage Format string error message to log
         * @param args         to pass to the string formatter
         */
        void errorOccurred(String errorMessage, Object... args) {
            errorOccurred(errorMessage, (Throwable) null, args);
        }

        /**
         * Log an error message and shutdown the scheduler with the error state
         *
         * @param errorMessage Format string error message to log
         * @param t            Throwable to log with the error message
         * @param args         to pass to the string formatter
         */
        void errorOccurred(String errorMessage, Throwable t, Object... args) {
            if (errorMessage != null) {
                log.error(generateLogMessage(args.length == 0 ? errorMessage : String.format(errorMessage, args)), t);
            }
            log.info(generateLogMessage(
                    "Schedule is terminating because of an error. "
                            + "Please resolve the error and either drop and recreate the schedule "
                            + "or disable and reenable it."));
            shutdown(SchedulerWrapperState.ERROR);
        }

        /**
         * Log a message and shutdown the scheduler with the exited state
         *
         * @param message Optional message to log. May be {@code null}
         */
        void exitRequested(String message) {
            if (message != null) {
                log.info(generateLogMessage(message));
            }
            shutdown(SchedulerWrapperState.EXITED);
        }

        private synchronized void shutdown(SchedulerWrapperState state) {
            if (!(m_state == SchedulerWrapperState.INITIALIZED || m_state == SchedulerWrapperState.RUNNING)) {
                return;
            }
            m_state = state;

            m_procedure = null;

            if (m_scheduledFuture != null) {
                m_scheduledFuture.cancel(false);
                m_scheduledFuture = null;
            }
        }

        private synchronized long calculateDelay() {
            // TODO include logic for min delay and max frequency
            return m_procedure.getDelay(TimeUnit.NANOSECONDS);
        }

        /**
         * Generate a message to be logged which includes information about scheduler
         *
         * @param body of message
         * @return Message to log
         */
        String generateLogMessage(String body) {
            return m_handler.generateLogMessage(body);
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#RUN_LOCATION_SYSTEM}
     */
    private class SystemSchedulerWrapper extends SchedulerWrapper<SingleSchedulerHandler> {
        SystemSchedulerWrapper(SingleSchedulerHandler definition, Scheduler scheduler) {
            super(definition, scheduler);
        }

        @Override
        boolean isValidProcedure(Procedure procedure) {
            return true;
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#RUN_LOCATION_HOSTS}
     */
    private class HostSchedulerWrapper extends SchedulerWrapper<SingleSchedulerHandler> {
        HostSchedulerWrapper(SingleSchedulerHandler handler, Scheduler scheduler) {
            super(handler, scheduler);
        }

        /**
         * Procedure must be an NT procedure
         */
        @Override
        boolean isValidProcedure(Procedure procedure) {
            if (procedure.getTransactional()) {
                errorOccurred("Procedure %s is a transactional procedure. Cannot be scheduled on a host.",
                        procedure.getTypeName());
                return false;
            }
            return true;
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#RUN_LOCATION_PARTITIONS}
     */
    private class PartitionSchedulerWrapper extends SchedulerWrapper<PartitionedScheduleHandler> {
        private final int m_partition;

        PartitionSchedulerWrapper(PartitionedScheduleHandler handler, Scheduler scheduler, int partition) {
            super(handler, scheduler);
            m_partition = partition;
        }

        /**
         * Procedure must be a single partition procedure with the first parameter as the partition parameter
         */
        @Override
        boolean isValidProcedure(Procedure procedure) {
            if (!procedure.getSinglepartition()) {
                errorOccurred("Procedure %s is not single partitioned. Cannot be scheduled on a partition.",
                        procedure.getTypeName());
                return false;
            }

            if (procedure.getPartitionparameter() != 0) {
                errorOccurred(
                        "Procedure %s partition parameter is not the first parameter. Cannot be scheduled on a partition.",
                        procedure.getTypeName());
                return false;
            }
            return true;
        }

        /**
         * Behaves like run {@link Client#callAllPartitionProcedure(String, Object...)} where the first argument to the
         * procedure is just there to route the procedure call to the desired partition.
         */
        @Override
        Object[] getProcedureParameters(Procedure procedure) {
            Object[] baseParams = super.getProcedureParameters(procedure);
            Object[] partitionedParams = new Object[baseParams.length + 1];

            VoltType keyType = VoltType.get((byte) procedure.getPartitioncolumn().getType());
            // BIGINT isn't supported so just use the INTEGER keys since they are compatible
            VoltTable keys = TheHashinator.getPartitionKeys(keyType == VoltType.BIGINT ? VoltType.INTEGER : keyType);
            if (keys == null) {
                errorOccurred("Unsupported partition key type %s for procedure %s", keyType, procedure.getTypeName());
                return null;
            }

            VoltTable copy = PrivateVoltTableFactory.createVoltTableFromBuffer(keys.getBuffer(), true);

            // Find the key for partition destination
            copy.resetRowPosition();
            while (copy.advanceRow()) {
                if (m_partition == copy.getLong(0)) {
                    partitionedParams[0] = copy.get(1, keyType);
                    break;
                }
            }

            if (partitionedParams[0] == null) {
                errorOccurred("Unable to find a key for partition %d", m_partition);
                return null;
            }

            System.arraycopy(baseParams, 0, partitionedParams, 1, baseParams.length);
            return partitionedParams;
        }

        @Override
        String generateLogMessage(String body) {
            return SchedulerManager.generateLogMessage(m_handler.getName() + " P" + m_partition, body);
        }
    }

    private static class SchedulerFactory {
        private final Constructor<Scheduler> m_constructor;
        private final Object[] m_parameters;
        private final byte[] m_classHash;
        Collection<String> m_classDeps = null;

        SchedulerFactory(Constructor<Scheduler> constructor, Object[] parameters, byte[] classHash) {
            super();
            this.m_constructor = constructor;
            this.m_parameters = parameters;
            this.m_classHash = classHash;
        }

        Scheduler construct() {
            try {
                Scheduler scheduler = m_constructor.newInstance(m_parameters);
                if (m_classDeps == null) {
                    m_classDeps = scheduler.getDependencies();
                }
                return scheduler;
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException(e);
            }
        }

        boolean hashesMatch(SchedulerFactory other) {
            if (other == null || !Arrays.equals(m_classHash, other.m_classHash)) {
                return false;
            }

            if (m_classHash == null) {
                // Not loaded by InMemoryJarFile so it cannot have deps
                return true;
            }

            Collection<String> deps = m_classDeps == null ? other.m_classDeps : m_classDeps;
            if (deps == null) {
                // No deps declared so force reload
                return false;
            }

            try {
                return Arrays.equals(hashDeps(deps), other.hashDeps(deps));
            } catch (NoSuchAlgorithmException e) {
                log.error("Failed to hash dependencies", e);
                return false;
            }
        }

        byte[] hashDeps(Collection<String> deps) throws NoSuchAlgorithmException {
            ClassLoader classLoader = m_constructor.getDeclaringClass().getClassLoader();
            if (classLoader instanceof InMemoryJarfile.JarLoader) {
                InMemoryJarfile jarFile = ((InMemoryJarfile.JarLoader) classLoader).getInMemoryJarfile();
                return jarFile.getClassesHash(deps, HASH_ALGO);
            }
            return null;
        }
    }
}
