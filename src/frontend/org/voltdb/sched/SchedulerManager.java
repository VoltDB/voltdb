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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang3.ArrayUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.AuthSystem;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.ClientInterface;
import org.voltdb.ParameterConverter;
import org.voltdb.SimpleClientResponseAdapter;
import org.voltdb.TheHashinator;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.ProcedureSchedule;
import org.voltdb.catalog.SchedulerParam;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

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
    static final String RUN_LOCATION_HOST = "HOST";
    static final String RUN_LOCATION_PARTITION = "PARTITION";

    private Map<String, SchedulerHandler> m_handlers = Collections.emptyMap();
    private volatile boolean m_leader = false;
    private AuthSystem m_authSystem;
    private boolean m_started = false;
    private final Set<Integer> m_locallyLedPartitions = new HashSet<>();

    // Used by the manager to perform management functions so a scheduler can not hold up the manager
    private final ListeningExecutorService m_managerExecutor = MoreExecutors
            .listeningDecorator(CoreUtils.getSingleThreadExecutor(getClass().getSimpleName()));

    // Used to execute the schedulers and scheduled procedures only used by ScheculerWrappers
    // TODO make thread count configurable for the wrapper executor
    final ListeningScheduledExecutorService m_wrapperExecutor = MoreExecutors.listeningDecorator(CoreUtils.getScheduledThreadPoolExecutor("ProcedureScheduler",
                    1, CoreUtils.SMALL_STACK_SIZE));
    final ClientInterface m_clientInterface;
    final SimpleClientResponseAdapter m_adapter = new SimpleClientResponseAdapter(
            ClientInterface.SCHEDULER_MANAGER_CID, getClass().getSimpleName());

    private static String generateLogMessage(String name, String body) {
        return String.format("Schedule (%s): %s", name, body);
    }

    /**
     * Create a factory supplier for instances of {@link Scheduler} as defined by the provided
     * {@link ProcedureSchedule}. If an instance of {@link Scheduler} cannot be constructed using the provided
     * configuration {@code null} is returned and a detailed error message will be logged.
     *
     * @param definition {@link ProcedureSchedule} defining the configuration of the schedule
     * @return {@link Supplier} for {@link Scheduler} instances or {@code null} if there was an error
     */
    private static Supplier<Scheduler> createSchedulerSupplier(ProcedureSchedule definition) {
        String schedulerClassString = definition.getSchedulerclass();
        try {
            Class<?> schedulerClass = SchedulerHandler.class.getClassLoader().loadClass(schedulerClassString);
            if (!Scheduler.class.isAssignableFrom(schedulerClass)) {
                log.error(generateLogMessage(definition.getName(), String.format("Class %s is not an instance of %s",
                        schedulerClassString, Scheduler.class.getName())));
                return null;
            }

            Constructor<?>[] constructors = schedulerClass.getConstructors();
            if (constructors.length != 1) {
                log.error(generateLogMessage(definition.getName(),
                        String.format("Scheduler class should have 1 constructor %s has %d",
                                schedulerClassString, constructors.length)));
                return null;
            }

            @SuppressWarnings("unchecked")
            Constructor<Scheduler> constructor = (Constructor<Scheduler>) constructors[0];
            CatalogMap<SchedulerParam> schedulerParams = definition.getParameters();
            if ((schedulerParams == null ? 0 : schedulerParams.size()) != constructor.getParameterCount()) {
                log.error(generateLogMessage(definition.getName(), String.format(
                        "Scheduler class, %s, constructor paremeter count %d does not match provided parameter count %d",
                        schedulerClassString, constructor.getParameterCount(), schedulerParams.size())));
                return null;
            }

            Object[] parameters;
            if (schedulerParams == null || schedulerParams.isEmpty()) {
                parameters = ArrayUtils.EMPTY_OBJECT_ARRAY;
            } else {
                Class<?>[] parameterTypes = constructor.getParameterTypes();
                parameters = new Object[schedulerParams.size()];
                for (SchedulerParam sp : schedulerParams) {
                    int index = sp.getIndex();
                    try {
                        parameters[index] = ParameterConverter.tryToMakeCompatible(parameterTypes[index],
                                sp.getParameter());
                    } catch (Exception e) {
                        log.warn(generateLogMessage(definition.getName(),
                                String.format("Could not convert parameter %d with the value \"%s\" to type %s: %s",
                                        sp.getIndex(), sp.getParameter(), parameterTypes[index].getName(),
                                        e.getMessage())));
                        return null;
                    }
                }
            }

            return () -> {
                try {
                    return constructor.newInstance(parameters);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    throw new IllegalArgumentException(e);
                }
            };
        } catch (Exception e) {
            log.error(generateLogMessage(definition.getName(),
                    String.format("Could not load and construct class: %s", schedulerClassString)), e);
            return null;
        }
    }

    public SchedulerManager(ClientInterface clientInterface) {
        m_clientInterface = clientInterface;
    }

    /**
     * Asynchronously start the scheduler manager and any configured schedules which are eligible to be run on this
     * host.
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param leader             whether or not this host is the global leader
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> start(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            boolean leader) {
        return execute(() -> {
            m_started = true;
            processCatalogInline(procedureSchedules, authSystem, leader);
        });
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> promoteToLeader(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem) {
        return processSchedules(procedureSchedules, authSystem, Boolean.TRUE);
    }

    /**
     * Asynchronously process an update to the scheduler configuration
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> processUpdate(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem) {
        return processSchedules(procedureSchedules, authSystem, null);
    }

    /**
     * Notify the manager that some local partitions have been promoted to leader. Any PARTITION schedules will be
     * asynchronously started for these partitions.
     *
     * @param partitions which were promoted
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> promotedPartitions(Collection<Integer> partitions) {
        return execute(() -> {
            for (SchedulerHandler sd : m_handlers.values()) {
                sd.promotedPartitions(partitions);
            }
            m_locallyLedPartitions.addAll(partitions);
        });
    }

    /**
     * Notify the manager that some local partitions have been demoted from leader. Any PARTITION schedules will be
     * asynchronously stopped for these partitions.
     *
     * @param partitions which were demoted
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> demotedPartitions(Collection<Integer> partitions) {
        return execute(() -> {
            for (SchedulerHandler sd : m_handlers.values()) {
                sd.demotedPartitions(partitions);
            }
            m_locallyLedPartitions.removeAll(partitions);
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

    private ListenableFuture<?> processSchedules(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            Boolean leader) {
        return execute(() -> processCatalogInline(procedureSchedules, authSystem, leader));
    }

    /**
     * Process any potential scheduler changes. Any modified schedules will be stopped and restarted with their new
     * configuration. If a schedule was not modified it will be left running.
     *
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param leader             whether or not this host is the global leader
     */
    private void processCatalogInline(Iterable<ProcedureSchedule> procedureSchedules, AuthSystem authSystem,
            Boolean leader) {
        if (leader != null) {
            m_leader = leader.booleanValue();
        }

        if (!m_started) {
            return;
        }

        Map<String, SchedulerHandler> newHandlers = new HashMap<>();
        m_authSystem = authSystem;

        for (ProcedureSchedule procedureSchedule : procedureSchedules) {
            SchedulerHandler handler = m_handlers.remove(procedureSchedule.getName());
            if (handler != null) {
                if (handler.isSameDefintion(procedureSchedule)) {
                    newHandlers.put(procedureSchedule.getName(), handler);
                    continue;
                }
                handler.cancel();
            }

            String runLocation = procedureSchedule.getRunlocation().toUpperCase();
            if (procedureSchedule.getEnabled()
                    && (m_leader || !RUN_LOCATION_SYSTEM.equals(runLocation))) {
                Supplier<Scheduler> schedulerSupplier = createSchedulerSupplier(procedureSchedule);
                if (schedulerSupplier == null) {
                    continue;
                }

                SchedulerHandler definition;
                switch (runLocation) {
                case RUN_LOCATION_HOST:
                case RUN_LOCATION_SYSTEM:
                    definition = new SingleSchedulerHandler(procedureSchedule, runLocation, schedulerSupplier);
                    break;
                case RUN_LOCATION_PARTITION:
                    definition = new PartitionedScheduleHandler(procedureSchedule, schedulerSupplier);
                    definition.promotedPartitions(m_locallyLedPartitions);
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
     * Base class for wrapping a single scheduler configuration.
     */
    private abstract class SchedulerHandler {
        private final ProcedureSchedule m_definition;

        SchedulerHandler(ProcedureSchedule definition) {
            m_definition = definition;
        }

        boolean isSameDefintion(ProcedureSchedule defintion) {
            return m_definition.equals(defintion);
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
         * @param partitions which were promoted
         */
        abstract void promotedPartitions(Iterable<Integer> partitions);

        /**
         * Notify this scheduler configuration of partitions which were locally demoted from leader
         *
         * @param partitions which were demoted
         */
        abstract void demotedPartitions(Iterable<Integer> partitions);

        String generateLogMessage(String body) {
            return SchedulerManager.generateLogMessage(m_definition.getName(), body);
        }
    }

    /**
     * An instance of {@link SchedulerHandler} which contains a single {@link SchedulerWrapper}. This is used for
     * schedules which are configured for {@link SchedulerManager#RUN_LOCATION_SYSTEM} or
     * {@link SchedulerManager#RUN_LOCATION_HOST}.
     */
    private class SingleSchedulerHandler extends SchedulerHandler {
        private final SchedulerWrapper<? extends SingleSchedulerHandler> m_wrapper;

        SingleSchedulerHandler(ProcedureSchedule definition, String runLocation,
                Supplier<Scheduler> schedulerSupplier) {
            super(definition);

            Scheduler scheduler = schedulerSupplier.get();
            switch (runLocation) {
            case RUN_LOCATION_HOST:
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
        void promotedPartitions(Iterable<Integer> partitions) {}

        @Override
        void demotedPartitions(Iterable<Integer> partitions) {}

        @Override
        void start() {
            m_wrapper.start();
        }
    }

    /**
     * An instance of {@link SchedulerHandler} which contains a {@link SchedulerWrapper} for each locally led partition.
     * This is used for schedules which are configured for {@link SchedulerManager#RUN_LOCATION_PARTITION}.
     */
    private class PartitionedScheduleHandler extends SchedulerHandler {
        private final Map<Integer, PartitionSchedulerWrapper> m_wrappers = new HashMap<>();
        private final Supplier<Scheduler> m_schedulerSupplier;
        private boolean m_started = false;

        PartitionedScheduleHandler(ProcedureSchedule definition, Supplier<Scheduler> schedulerSupplier) {
            super(definition);
            m_schedulerSupplier = schedulerSupplier;
        }

        @Override
        void cancel() {
            for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                wrapper.cancel();
            }
            m_wrappers.clear();
        }

        @Override
        void promotedPartitions(Iterable<Integer> partitions) {
            for (Integer partition : partitions) {
                assert !m_wrappers.containsKey(partition);
                PartitionSchedulerWrapper wrapper = new PartitionSchedulerWrapper(this, m_schedulerSupplier.get(),
                        partition);

                m_wrappers.put(partition, wrapper);

                if (m_started) {
                    wrapper.start();
                }
            }
        }

        @Override
        void demotedPartitions(Iterable<Integer> partitions) {
            for (Integer partition : partitions) {
                PartitionSchedulerWrapper wrapper;
                wrapper = m_wrappers.remove(partition);
                if (wrapper != null) {
                    wrapper.cancel();
                }
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
    enum SchedulerWrapperState {
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
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#RUN_LOCATION_HOST}
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
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#RUN_LOCATION_PARTITION}
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

            // Find the key for partition destination
            while (keys.advanceRow()) {
                if (m_partition == keys.getLong(0)) {
                    partitionedParams[0] = keys.get(1, keyType);
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
}
