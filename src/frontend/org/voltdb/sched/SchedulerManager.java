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
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.math.RoundingMode;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.voltcore.logging.Level;
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
import org.voltdb.StatsAgent;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.ProcedureSchedule;
import org.voltdb.catalog.SchedulerParam;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.SchedulerType;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.math.DoubleMath;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.ListeningScheduledExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import com.google_voltpatches.common.util.concurrent.UnsynchronizedRateLimiter;

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
public final class SchedulerManager {
    static final VoltLogger log = new VoltLogger("SCHEDULE");
    static final String SCOPE_SYSTEM = "SYSTEM";
    static final String SCOPE_HOSTS = "HOSTS";
    static final String SCOPE_PARTITIONS = "PARTITIONS";
    static final String HASH_ALGO = "SHA-512";
    public static final String SCOPE_DEFAULT = SCOPE_SYSTEM;

    private Map<String, SchedulerHandler> m_handlers = Collections.emptyMap();
    private volatile boolean m_leader = false;
    private AuthSystem m_authSystem;
    private boolean m_started = false;
    private final Set<Integer> m_locallyLedPartitions = new HashSet<>();
    private final SimpleClientResponseAdapter m_adapter = new SimpleClientResponseAdapter(
            ClientInterface.SCHEDULER_MANAGER_CID, getClass().getSimpleName());

    // Global configuration values
    volatile long m_minDelayNs = 0;
    volatile double m_maxRunFrequency = 0;

    // Local host ID
    final int m_hostId;

    // Used by the manager to perform management functions so a scheduler can not hold up the manager
    private final ListeningExecutorService m_managerExecutor = MoreExecutors
            .listeningDecorator(CoreUtils.getSingleThreadExecutor(getClass().getSimpleName()));

    // Used to execute the schedulers and scheduled procedures for system or host schedules
    private final ScheduledExecutorHolder m_singleExecutor = new ScheduledExecutorHolder("HOST");
    // Used to execute the schedulers and scheduled procedures for partitioned schedules
    private final ScheduledExecutorHolder m_partitionedExecutor = new ScheduledExecutorHolder("PARTITIONED");

    final ClientInterface m_clientInterface;
    final StatsAgent m_statsAgent;

    static String generateLogMessage(String name, String body) {
        return String.format("%s: %s", name, body);
    }

    private static boolean isLastParamaterVarArgs(Constructor<Scheduler> constructor) {
        if (constructor.getParameterCount() == 0) {
            return false;
        }
        Parameter[] params = constructor.getParameters();
        Parameter lastParam = params[params.length - 1];
        return lastParam.getType() == String[].class || lastParam.getType() == Object[].class;
    }


    public SchedulerManager(ClientInterface clientInterface, StatsAgent statsAgent, int hostId) {
        m_clientInterface = clientInterface;
        m_statsAgent = statsAgent;
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
                    Future<Boolean> demoteFuture = m_migratingPartitions.remove(partitionId);
                    if (demoteFuture != null && demoteFuture.get().booleanValue()) {
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
        return start(context.getDeployment().getScheduler(), context.database.getProcedureschedules(),
                context.authSystem, context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously start the scheduler manager and any configured schedules which are eligible to be run on this
     * host.
     *
     * @param configuration      Global configuration for all schedules
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load configured {@link Scheduler}s
     *
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> start(SchedulerType configuration, Iterable<ProcedureSchedule> procedureSchedules,
            AuthSystem authSystem, ClassLoader classLoader) {
        return execute(() -> {
            m_started = true;

            // Create a dummy stats source so something is always reported
            ScheduleStatsSource.createDummy().register(m_statsAgent);

            processCatalogInline(configuration, procedureSchedules, authSystem, classLoader, false);
        });
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param context {@link CatalogContext} instance
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> promoteToLeader(CatalogContext context) {
        return promoteToLeader(context.getDeployment().getScheduler(), context.database.getProcedureschedules(),
                context.authSystem,
                context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param configuration      Global configuration for all schedules
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load configured {@link Scheduler}s
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> promoteToLeader(SchedulerType configuration, Iterable<ProcedureSchedule> procedureSchedules,
            AuthSystem authSystem, ClassLoader classLoader) {
        return execute(() -> {
            m_leader = true;
            processCatalogInline(configuration, procedureSchedules, authSystem, classLoader, false);
        });
    }

    /**
     * Asynchronously process an update to the scheduler configuration
     *
     * @param context        {@link CatalogContext} instance
     * @param classesUpdated If {@code true} handle classes being updated in the system jar
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> processUpdate(CatalogContext context, boolean classesUpdated) {
        return processUpdate(context.getDeployment().getScheduler(), context.database.getProcedureschedules(),
                context.authSystem, context.getCatalogJar().getLoader(), classesUpdated);
    }

    /**
     * Asynchronously process an update to the scheduler configuration
     *
     * @param configuration      Global configuration for all schedules
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load configured {@link Scheduler}s
     * @param classesUpdated     If {@code true} handle classes being updated in the system jar
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> processUpdate(SchedulerType configuration, Iterable<ProcedureSchedule> procedureSchedules,
            AuthSystem authSystem, ClassLoader classLoader, boolean classesUpdated) {
        return execute(
                () -> processCatalogInline(configuration, procedureSchedules, authSystem, classLoader, classesUpdated));
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
            if (m_locallyLedPartitions.add(partitionId)) {
                updatePartitionedThreadPoolSize();
                for (SchedulerHandler sd : m_handlers.values()) {
                    sd.promotedPartition(partitionId);
                }
            }
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
            updatePartitionedThreadPoolSize();
            for (SchedulerHandler sd : m_handlers.values()) {
                sd.demotedPartition(partitionId);
            }
            return true;
        });
    }

    private void updatePartitionedThreadPoolSize() {
        if (m_handlers.values().stream().filter(h -> h instanceof PartitionedScheduleHandler).findAny().isPresent()) {
            m_partitionedExecutor.setDynamicThreadCount(calculatePartitionedThreadPoolSize());
        }
    }

    private int calculatePartitionedThreadPoolSize() {
        return DoubleMath.roundToInt(m_locallyLedPartitions.size() / 2.0, RoundingMode.UP);
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
                m_singleExecutor.getExecutor().shutdown();
                m_partitionedExecutor.getExecutor().shutdown();
            });
        } catch (RejectedExecutionException e) {
            return Futures.immediateFuture(null);
        }
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
    public SchedulerValidationResult validateScheduler(ProcedureSchedule definition, ClassLoader classLoader) {
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
                return new SchedulerValidationResult(
                        String.format("Scheduler class should have 1 constructor %s has %d", schedulerClassString,
                                constructors.length));
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

            String parameterErrors = validateSchedulerParameters(definition, constructor, parameters);
            if (parameterErrors != null) {
                return new SchedulerValidationResult("Error validating scheduler parameters: " + parameterErrors);
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
     * @param configuration      Global configuration for all schedules
     * @param procedureSchedules {@link Collection} of configured {@link ProcedureSchedule}
     * @param authSystem         Current {@link AuthSystem} for the system
     * @param classLoader        {@link ClassLoader} to use to load {@link Scheduler} classes
     */
    private void processCatalogInline(SchedulerType configuration, Iterable<ProcedureSchedule> procedureSchedules,
            AuthSystem authSystem, ClassLoader classLoader, boolean classesUpdated) {
        if (!m_started) {
            return;
        }

        Map<String, SchedulerHandler> newHandlers = new HashMap<>();
        m_authSystem = authSystem;

        if (configuration == null) {
            // No configuration provided so use defaults
            configuration = new SchedulerType();
        }

        m_minDelayNs = TimeUnit.MILLISECONDS.toNanos(configuration.getMinDelayMs());
        double originalFrequency = m_maxRunFrequency;
        m_maxRunFrequency = configuration.getMaxRunFrequency() / 60.0;
        boolean frequencyChanged = m_maxRunFrequency != originalFrequency;

        // Set the explicitly defined thread counts
        m_singleExecutor.setThreadCount(configuration.getHostThreadCount());
        m_partitionedExecutor.setThreadCount(configuration.getPartitionedThreadCount());

        boolean hasNonPartitionedSchedule = false;
        boolean hasPartitionedSchedule = false;

        for (ProcedureSchedule procedureSchedule : procedureSchedules) {
            SchedulerHandler handler = m_handlers.remove(procedureSchedule.getName());
            SchedulerValidationResult result = validateScheduler(procedureSchedule, classLoader);

            if (handler != null) {
                // Do not restart a schedule if it has not changed
                if (handler.isSameSchedule(procedureSchedule, result.m_factory, classesUpdated)) {
                    newHandlers.put(procedureSchedule.getName(), handler);
                    handler.updateDefinition(procedureSchedule);
                    if (frequencyChanged) {
                        handler.setMaxRunFrequency(m_maxRunFrequency);
                    }
                    if (SCOPE_PARTITIONS.equalsIgnoreCase(procedureSchedule.getScope())) {
                        hasPartitionedSchedule = true;
                    } else {
                        hasNonPartitionedSchedule = true;
                    }
                    continue;
                }
                handler.cancel();
            }

            String scope = procedureSchedule.getScope();
            if (procedureSchedule.getEnabled()
                    && (m_leader || !SCOPE_SYSTEM.equals(scope))) {
                if (!result.isValid()) {
                    log.warn(generateLogMessage(procedureSchedule.getName(), result.getErrorMessage()),
                            result.getException());
                    continue;
                }

                SchedulerHandler definition;
                switch (scope) {
                case SCOPE_HOSTS:
                case SCOPE_SYSTEM:
                    definition = new SingleSchedulerHandler(procedureSchedule, scope, result.m_factory,
                            m_singleExecutor.getExecutor());
                    hasNonPartitionedSchedule = true;
                    break;
                case SCOPE_PARTITIONS:
                    definition = new PartitionedScheduleHandler(procedureSchedule, result.m_factory,
                            m_partitionedExecutor.getExecutor());
                    m_locallyLedPartitions.forEach(definition::promotedPartition);
                    hasPartitionedSchedule = true;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported run location: " + procedureSchedule.getScope());
                }
                newHandlers.put(procedureSchedule.getName(), definition);
            }
        }

        // Cancel all removed schedules
        for (SchedulerHandler handler : m_handlers.values()) {
            handler.cancel();
        }

        // Set the dynamic thread counts based on whether or not schedules exist and partition counts
        m_singleExecutor.setDynamicThreadCount(hasNonPartitionedSchedule ? 1 : 0);
        m_partitionedExecutor.setDynamicThreadCount(hasPartitionedSchedule ? calculatePartitionedThreadPoolSize() : 0);

        // Start all current schedules. This is a no-op for already started schedules
        for (SchedulerHandler handler : newHandlers.values()) {
            handler.start();
        }

        m_handlers = newHandlers;
    }

    /**
     * Try to find the optional static method {@code validateParameters} and call it if it is compatible to see if the
     * parameters to be passed to the scheduler constructor are valid for the scheduler.
     *
     * @param definition  Instance of {@link ProcedureSchedule} defining the schedule
     * @param constructor {@link Constructor} instance for the {@link Scheduler}
     * @param parameters  that are going to be passed to the constructor
     * @return error message if the parameters are not valid or {@code null} if they are
     */
    private String validateSchedulerParameters(ProcedureSchedule definition, Constructor<Scheduler> constructor,
            Object[] parameters) {
        Class<Scheduler> schedulerClass = constructor.getDeclaringClass();

        for (Method m : schedulerClass.getMethods()) {
            // Find method declared as: public static void validateParameters
            if (!Modifier.isStatic(m.getModifiers()) || !"validateParameters".equals(m.getName())) {
                continue;
            }

            if (m.getReturnType() != String.class) {
                log.warn(generateLogMessage(definition.getName(),
                        schedulerClass.getName()
                                + " defines a 'validateParameters' method but it does not return a String"));
            }

            Class<?>[] methodParameterTypes = m.getParameterTypes();

            Class<?>[] constructorParameterTypes = constructor.getParameterTypes();
            boolean takesHelper = SchedulerValidationHelper.class.isAssignableFrom(methodParameterTypes[0]);
            int expectedParameterCount = constructorParameterTypes.length + (takesHelper ? 1 : 0);

            if (methodParameterTypes.length != expectedParameterCount) {
                log.warn(generateLogMessage(definition.getName(), schedulerClass.getName()
                        + " defines a 'validateParameters' method but parameter count is not correct. It should be the same as constructor with possibly an optional "
                        + SchedulerValidationHelper.class.getSimpleName() + " first"));
                continue;
            }
            Class<?> validatorParameterTypes[] = new Class<?>[expectedParameterCount];
            System.arraycopy(constructorParameterTypes, 0, validatorParameterTypes,
                    expectedParameterCount - constructorParameterTypes.length, constructorParameterTypes.length);
            if (takesHelper) {
                validatorParameterTypes[0] = SchedulerValidationHelper.class;
            }

            if (!Arrays.equals(validatorParameterTypes, methodParameterTypes)) {
                log.warn(generateLogMessage(definition.getName(), schedulerClass.getName()
                        + " defines a 'validateParameters' method but parameters do not match constructor parameters"));
                continue;
            }

            // Construct the parameter array to be passed to the validate method
            Object[] validatorParameters = new Object[expectedParameterCount];
            System.arraycopy(parameters, 0, validatorParameters, expectedParameterCount - parameters.length,
                    parameters.length);

            if (takesHelper) {
                validatorParameters[0] = (SchedulerValidationHelper) (errors, restrictProcedureByScope, procedureName,
                        procedureParameters) -> validateProcedureWithParameters(errors, definition.getScope(),
                                restrictProcedureByScope, procedureName, procedureParameters);
            }

            try {
                return (String) m.invoke(null, validatorParameters);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                log.warn(generateLogMessage(definition.getName(), ""), e);
                return null;
            }
        }

        return null;
    }

    /**
     * @see SchedulerValidationHelper#validateProcedureAndParams(SchedulerValidationErrors, String, Object[])
     * @param errors        {@link SchedulerValidationErrors} instance to collect errors
     * @param procedureName Name of procedure to validate
     * @param parameters    that will be passed to {@code procedureName}
     */
    private void validateProcedureWithParameters(SchedulerValidationErrors errors, String scope,
            boolean restrictProcedureByScope, String procedureName,
            Object[] parameters) {
        Procedure procedure = m_clientInterface.getProcedureFromName(procedureName);
        if (procedure == null) {
            errors.addErrorMessage("Procedure does not exist: " + procedureName);
            return;
        }

        if (procedure.getSystemproc()) {
            // System procedures do not have parameter types in the procedure definition
            return;
        }

        if (restrictProcedureByScope) {
            String error = isProcedureValidForScope(scope, procedure);
            if (error != null) {
                errors.addErrorMessage(error);
                return;
            }
        }

        CatalogMap<ProcParameter> parameterTypes = procedure.getParameters();

        if (procedure.getSinglepartition() && parameterTypes.size() == parameters.length + 1) {
            if (procedure.getPartitionparameter() != 0) {
                errors.addErrorMessage(String.format(
                        "Procedure %s is a partitioned procedure but the partition parameter is not the first",
                        procedureName));
                return;
            }

            Object[] newParameters = new Object[parameters.length + 1];
            newParameters[0] = 0;
            System.arraycopy(parameters, 0, newParameters, 1, parameters.length);
            parameters = newParameters;
        }

        if (parameterTypes.size() != parameters.length) {
            errors.addErrorMessage(String.format("Procedure %s takes %d parameters but %d were given", procedureName,
                    procedure.getParameters().size(), parameters.length));
            return;
        }

        for (ProcParameter pp : parameterTypes) {
            Class<?> parameterClass = VoltType.classFromByteValue((byte) pp.getType());
            try {
            ParameterConverter.tryToMakeCompatible(parameterClass, parameters[pp.getIndex()]);
            } catch (Exception e) {
                errors.addErrorMessage(
                        String.format("Could not convert parameter %d with the value \"%s\" to type %s: %s",
                                pp.getIndex(), parameters[pp.getIndex()], parameterClass.getName(), e.getMessage()));
            }
        }
    }

    /**
     * When procedure is being restricted by the scope validate that the procedure can be executed within that scope.
     *
     * @see Scheduler#restrictProcedureByScope()
     * @param scope     of the schedule being validated
     * @param procedure {@link Procedure} instance to validate
     * @return {@code null} if procedure is valid for scope otherwise a detailed error message will be returned
     */
    static String isProcedureValidForScope(String scope, Procedure procedure) {
        switch (scope) {
        case SCOPE_SYSTEM:
            break;
        case SCOPE_HOSTS:
            if (procedure.getTransactional()) {
                return String.format("Procedure %s is a transactional procedure. Cannot be scheduled on a host.",
                        procedure.getTypeName());
            }
            break;
        case SCOPE_PARTITIONS:
            if (!procedure.getSinglepartition()) {
                return String.format("Procedure %s is not a partitioned procedure. Cannot be scheduled on a partition.",
                        procedure.getTypeName());
            }
            if (procedure.getPartitionparameter() != 0) {
                return String.format("Procedure %s partition parameter is not the first parameter. Cannot be scheduled on a partition.",
                        procedure.getTypeName());
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown scope: " + scope);
        }
        return null;
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
         * Does not include {@link ProcedureSchedule#getEnabled()} or {@link ProcedureSchedule#getOnerror()} in
         * {@code definition} comparison
         *
         * @param definition {@link ProcedureSchedule} defining the schedule configuration
         * @param factory    {@link SchedulerFactory} derived from {@code definition}
         * @return {@code true} if both {@code definition} and {@code factory} match those in this handler
         */
        boolean isSameSchedule(ProcedureSchedule definition, SchedulerFactory factory, boolean checkHashes) {
            return isSameDefinition(definition) && (!checkHashes || m_factory.hashesMatch(factory));
        }

        private boolean isSameDefinition(ProcedureSchedule definition) {
            return Objects.equals(m_definition.getName(), definition.getName())
                    && Objects.equals(m_definition.getScope(), definition.getScope())
                    && Objects.equals(m_definition.getSchedulerclass(), definition.getSchedulerclass())
                    && Objects.equals(m_definition.getUser(), definition.getUser())
                    && Objects.equals(m_definition.getParameters(), definition.getParameters());
        }

        String getName() {
            return m_definition.getName();
        }

        String getUser() {
            return m_definition.getUser();
        }

        String getOnError() {
            return m_definition.getOnerror();
        }

        /**
         * Start executing this configured scheduler
         */
        final void start() {
            if (m_definition.getEnabled()) {
                startImpl();
            }
        }

        abstract void startImpl();

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
         * Handle allowed definition updates. The only allowed changes are those ignored by
         * {@link #isSameSchedule(ProcedureSchedule, SchedulerFactory, boolean)}
         *
         * @param newDefintion Updated definition
         */
        void updateDefinition(ProcedureSchedule newDefintion) {
            assert isSameDefinition(newDefintion);
            m_definition.setEnabled(newDefintion.getEnabled());
            m_definition.setOnerror(newDefintion.getOnerror());
        }

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

        abstract void setMaxRunFrequency(double frequency);
    }

    /**
     * An instance of {@link SchedulerHandler} which contains a single {@link SchedulerWrapper}. This is used for
     * schedules which are configured for {@link SchedulerManager#SCOPE_SYSTEM} or
     * {@link SchedulerManager#SCOPE_HOSTS}.
     */
    private class SingleSchedulerHandler extends SchedulerHandler {
        private final SchedulerWrapper<? extends SingleSchedulerHandler> m_wrapper;

        SingleSchedulerHandler(ProcedureSchedule definition, String scope, SchedulerFactory factory,
                ListeningScheduledExecutorService executor) {
            super(definition, factory);

            switch (scope) {
            case SCOPE_HOSTS:
                m_wrapper = new HostSchedulerWrapper(this, executor);
                break;
            case SCOPE_SYSTEM:
                m_wrapper = new SystemSchedulerWrapper(this, executor);
                break;
            default:
                throw new IllegalArgumentException("Invalid run location: " + scope);
            }
        }

        @Override
        void cancel() {
            m_wrapper.cancel();
        }

        @Override
        void updateDefinition(ProcedureSchedule newDefintion) {
            super.updateDefinition(newDefintion);
            m_wrapper.setEnabled(newDefintion.getEnabled());
        }

        @Override
        void promotedPartition(int partitionId) {}

        @Override
        void demotedPartition(int partitionId) {}

        @Override
        void startImpl() {
            m_wrapper.start();
        }

        @Override
        void setMaxRunFrequency(double frequency) {
            m_wrapper.setMaxRunFrequency(frequency);
        }
    }

    /**
     * An instance of {@link SchedulerHandler} which contains a {@link SchedulerWrapper} for each locally led partition.
     * This is used for schedules which are configured for {@link SchedulerManager#SCOPE_PARTITIONS}.
     */
    private class PartitionedScheduleHandler extends SchedulerHandler {
        private final Map<Integer, PartitionSchedulerWrapper> m_wrappers = new HashMap<>();
        private final ListeningScheduledExecutorService m_executor;
        private boolean m_handlerStarted = false;

        PartitionedScheduleHandler(ProcedureSchedule definition, SchedulerFactory factory,
                ListeningScheduledExecutorService executor) {
            super(definition, factory);
            m_executor = executor;
        }

        @Override
        void cancel() {
            for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                wrapper.cancel();
            }
            m_wrappers.clear();
        }

        @Override
        void updateDefinition(ProcedureSchedule newDefintion) {
            super.updateDefinition(newDefintion);
            boolean enabled = newDefintion.getEnabled();
            for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                wrapper.setEnabled(enabled);
            }
            if (!enabled) {
                m_handlerStarted = false;
            }
        }

        @Override
        void promotedPartition(int partitionId) {
            assert !m_wrappers.containsKey(partitionId);
            PartitionSchedulerWrapper wrapper = new PartitionSchedulerWrapper(this, partitionId, m_executor);

            m_wrappers.put(partitionId, wrapper);

            if (m_handlerStarted) {
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
        void startImpl() {
            if (!m_handlerStarted) {
                m_handlerStarted = true;

                for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                    wrapper.start();
                }
            }
        }

        @Override
        void setMaxRunFrequency(double frequency) {
            for (PartitionSchedulerWrapper wrapper : m_wrappers.values()) {
                wrapper.setMaxRunFrequency(frequency);
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
        CANCELED,
        /** Scheduler was disabled by the user */
        DISABLED;
    }

    /**
     * Base class which wraps the execution and handling of a single {@link Scheduler} instance.
     * <p>
     * On start {@link Scheduler#getNextAction(ScheduledAction)} is invoked with a null {@link ScheduledAction}
     * argument. If the result has a status of {@link Action.Type#PROCEDURE} then the provided procedure will be
     * scheduled and executed after the delay. Once a response is received
     * {@link Scheduler#getNextAction(ScheduledAction)} will be invoked again with the {@link ScheduledAction}
     * previously returned and {@link ScheduledAction#getResponse()} updated with response. This repeats until this
     * wrapper is cancelled or {@link Scheduler#getNextAction(ScheduledAction)} returns a non schedule result.
     * <p>
     * This class needs to be thread safe since it's execution is split between the
     * {@link SchedulerManager#m_managerExecutor} and the schedule executors
     *
     * @param <H> Type of {@link SchedulerHandler} which created this wrapper
     */
    private abstract class SchedulerWrapper<H extends SchedulerHandler> {
        ScheduledAction m_procedure;

        final H m_handler;

        private final ListeningScheduledExecutorService m_executor;
        private Scheduler m_scheduler;
        private Future<?> m_scheduledFuture;
        private volatile SchedulerWrapperState m_state = SchedulerWrapperState.INITIALIZED;
        private ScheduleStatsSource m_stats;
        private UnsynchronizedRateLimiter m_rateLimiter;

        // Time at which the handleNextRun was enqueued or should be eligible to execute after a delay
        private volatile long m_expectedExecutionTime;

        SchedulerWrapper(H handler, ListeningScheduledExecutorService executor) {
            m_handler = handler;
            m_executor = executor;
        }

        /**
         * Start running the scheduler
         */
        synchronized void start() {
            if (m_state != SchedulerWrapperState.INITIALIZED) {
                return;
            }
            m_scheduler = m_handler.constructScheduler();
            if (m_stats == null) {
                m_stats = ScheduleStatsSource.create(m_handler.getName(), getScope(), getSiteId());
                m_stats.register(m_statsAgent);
            }
            setMaxRunFrequency(m_maxRunFrequency);
            setState(SchedulerWrapperState.RUNNING);
            submitHandleNextRun();
        }

        /**
         * Call {@link Scheduler#getNextAction(ScheduledAction)} and process the result including scheduling the next
         * procedure to run
         * <p>
         * NOTE: method is not synchronized so the lock is not held during the scheduler execution
         */
        private void handleNextRun() {
            Scheduler scheduler;
            synchronized (this) {
                if (m_state != SchedulerWrapperState.RUNNING) {
                    return;
                }
                scheduler = m_scheduler;
            }
            long startTime = System.nanoTime();
            long waitTime = startTime - m_expectedExecutionTime;

            Action action;
            try {
                action = m_procedure == null ? scheduler.getFirstAction() : scheduler.getNextAction(m_procedure);
            } catch (RuntimeException e) {
                errorOccurred("Scheduler encountered unexpected error", e);
                return;
            }

            if (action == null) {
                errorOccurred("Scheduler returned a null result");
                return;
            }

            m_stats.addSchedulerCall(System.nanoTime() - startTime, waitTime, action.getStatusMessage());

            synchronized (this) {
                if (m_state != SchedulerWrapperState.RUNNING) {
                    return;
                }

                Runnable runnable;
                switch (action.getType()) {
                case EXIT:
                    exitRequested(action.getStatusMessage());
                    return;
                case ERROR:
                    errorOccurred(Level.WARN, action.getStatusMessage(), null);
                    return;
                case RERUN:
                    runnable = this::handleNextRun;
                    break;
                case PROCEDURE:
                    runnable = this::executeProcedure;
                    break;
                default:
                    throw new IllegalStateException("Unknown status: " + action.getType());
                }

                m_procedure = action.getScheduledAction();

                try {
                    long delay = calculateDelay();
                    m_expectedExecutionTime = System.nanoTime() + delay;
                    m_procedure.setExpectedExecutionTime(m_expectedExecutionTime);
                    m_scheduledFuture = addExceptionListener(
                            m_executor.schedule(runnable, delay, TimeUnit.NANOSECONDS));
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

            boolean failed = response.getStatus() != ClientResponse.SUCCESS;
            m_procedure.setResponse(response);
            m_stats.addProcedureCall(m_procedure.getExecutionTime(), m_procedure.getWaitTime(), failed);

            if (failed) {
                String onError = m_handler.getOnError();

                boolean isIgnore = "IGNORE".equalsIgnoreCase(onError);
                if (!isIgnore || log.isDebugEnabled()) {
                    String message = "Procedure " + m_procedure.getProcedure() + " with parameters "
                            + Arrays.toString(m_procedure.getProcedureParameters()) + " failed: "
                            + m_procedure.getResponse().getStatusString();

                    if (isIgnore || "LOG".equalsIgnoreCase(onError)) {
                        log.log(isIgnore ? Level.DEBUG : Level.INFO, generateLogMessage(message), null);
                    } else {
                        errorOccurred(message);
                        return;
                    }
                }
            }

            submitHandleNextRun();
        }

        private synchronized void submitHandleNextRun() {
            try {
                m_expectedExecutionTime = System.nanoTime();
                addExceptionListener(m_executor.submit(this::handleNextRun));
            } catch (RejectedExecutionException e) {
                if (log.isDebugEnabled()) {
                    log.debug(generateLogMessage("Execution of response handler rejected"), e);
                }
            }
        }

        /**
         * Shutdown the scheduler with a cancel state
         */
        void cancel() {
            shutdown(SchedulerWrapperState.CANCELED);
            m_stats.deregister(m_statsAgent);
        }

        synchronized void setEnabled(boolean enabled) {
            if (enabled) {
                if (m_state == SchedulerWrapperState.DISABLED) {
                    setState(SchedulerWrapperState.INITIALIZED);
                }
            } else if (m_state != SchedulerWrapperState.DISABLED) {
                shutdown(SchedulerWrapperState.DISABLED);
            }
        }

        synchronized void setMaxRunFrequency(double frequency) {
            if (frequency > 0) {
                if (m_rateLimiter == null) {
                    m_rateLimiter = UnsynchronizedRateLimiter.create(frequency);
                } else {
                    m_rateLimiter.setRate(frequency);
                }
            } else {
                m_rateLimiter = null;
            }
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

            if (m_scheduler.restrictProcedureByScope()) {
                String error = isProcedureValidForScope(getScope(), procedure);
                if (error != null) {
                    errorOccurred(error);
                    return null;
                }
            }

            return procedure;
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
            errorOccurred(Level.ERROR, errorMessage, t, args);
        }

        /**
         * Log an error message and shutdown the scheduler with the error state
         *
         * @param level        Log level at which to log {@code errorMessage}
         * @param errorMessage Format string error message to log
         * @param t            Throwable to log with the error message
         * @param args         to pass to the string formatter
         */
        private void errorOccurred(Level level, String errorMessage, Throwable t, Object... args) {
            String message = null;
            if (errorMessage != null) {
                message = args.length == 0 ? errorMessage : String.format(errorMessage, args);
                log.log(level, generateLogMessage(message), t);
            }
            m_stats.setSchedulerStatus(message);
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
            m_stats.setSchedulerStatus(message);
            shutdown(SchedulerWrapperState.EXITED);
        }

        private synchronized void shutdown(SchedulerWrapperState state) {
            if (!(m_state == SchedulerWrapperState.INITIALIZED || m_state == SchedulerWrapperState.RUNNING
                    || state == SchedulerWrapperState.DISABLED)) {
                return;
            }
            setState(state);

            m_scheduler = null;
            m_procedure = null;

            if (m_scheduledFuture != null) {
                m_scheduledFuture.cancel(false);
                m_scheduledFuture = null;
            }
        }

        private synchronized long calculateDelay() {
            long minDelayNs = m_minDelayNs;
            if (m_rateLimiter != null) {
                long rateLimitDelayNs = TimeUnit.MICROSECONDS.toNanos(m_rateLimiter.reserve(1));
                minDelayNs = Math.max(minDelayNs, rateLimitDelayNs);
            }
            return Math.max(m_procedure.getDelay(TimeUnit.NANOSECONDS), minDelayNs);
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

        abstract String getScope();

        abstract int getSiteId();

        private void setState(SchedulerWrapperState state) {
            m_state = state;
            m_stats.setState(m_state.name());
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#SCOPE_SYSTEM}
     */
    private class SystemSchedulerWrapper extends SchedulerWrapper<SingleSchedulerHandler> {
        SystemSchedulerWrapper(SingleSchedulerHandler definition, ListeningScheduledExecutorService executor) {
            super(definition, executor);
        }

        @Override
        String getScope() {
            return SCOPE_SYSTEM;
        }

        @Override
        int getSiteId() {
            return -1;
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#SCOPE_HOSTS}
     */
    private class HostSchedulerWrapper extends SchedulerWrapper<SingleSchedulerHandler> {
        HostSchedulerWrapper(SingleSchedulerHandler handler, ListeningScheduledExecutorService executor) {
            super(handler, executor);
        }

        @Override
        String getScope() {
            return SCOPE_HOSTS;
        }

        @Override
        int getSiteId() {
            return -1;
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link SchedulerManager#SCOPE_PARTITIONS}
     */
    private class PartitionSchedulerWrapper extends SchedulerWrapper<PartitionedScheduleHandler> {
        private final int m_partition;

        PartitionSchedulerWrapper(PartitionedScheduleHandler handler, int partition,
                ListeningScheduledExecutorService executor) {
            super(handler, executor);
            m_partition = partition;
        }

        /**
         * Behaves like run {@link Client#callAllPartitionProcedure(String, Object...)} where the first argument to the
         * procedure is just there to route the procedure call to the desired partition.
         */
        @Override
        Object[] getProcedureParameters(Procedure procedure) {
            if (!procedure.getSinglepartition()) {
                return super.getProcedureParameters(procedure);
            }

            Object[] baseParams = super.getProcedureParameters(procedure);
            CatalogMap<ProcParameter> procParams = procedure.getParameters();
            if (procParams == null
                    || !(procParams.size() == baseParams.length + 1 && procedure.getPartitionparameter() == 0)) {
                return baseParams;
            }

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

        @Override
        String getScope() {
            return SCOPE_PARTITIONS;
        }

        @Override
        int getSiteId() {
            return m_partition;
        }
    }

    /**
     * Factory which is used to construct a {@link Scheduler} with a fixed set of parameters. Also, is used to track the
     * hash of the {@Link Scheduler} class and any explicitly declared dependencies.
     */
    private static final class SchedulerFactory {
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

    /**
     * Utility class to wrap a {@link ListeningScheduledExecutorService} so that it can have either an explicit thread
     * count or a dynamic thread count.
     */
    static private final class ScheduledExecutorHolder {
        private final ScheduledThreadPoolExecutor m_rawExecutor;
        private final ListeningScheduledExecutorService m_executor;
        private boolean m_dynamicThreadCount = true;

        ScheduledExecutorHolder(String name) {
            super();
            m_rawExecutor = CoreUtils.getScheduledThreadPoolExecutor("Scheduler-" + name, 0,
                    CoreUtils.SMALL_STACK_SIZE);
            m_executor = MoreExecutors.listeningDecorator(m_rawExecutor);
        }

        /**
         * @return {@link ListeningScheduledExecutorService} instance
         */
        ListeningScheduledExecutorService getExecutor() {
            return m_executor;
        }

        /**
         * Set the thread count for this executor. If {@code threadCount <= 0} the thread count is considered dynamic
         * and not applied.
         *
         * @param threadCount to apply
         */
        void setThreadCount(int threadCount) {
            if (threadCount > 0) {
                m_dynamicThreadCount = false;
                setCorePoolSize(threadCount);
            } else {
                m_dynamicThreadCount = true;
            }
        }

        /**
         * If this executor is in dynamic mode set the thread count to {@code threadCount}
         *
         * @param threadCount to apply
         * @see #setThreadCount(int)
         */
        void setDynamicThreadCount(int threadCount) {
            if (m_dynamicThreadCount) {
                setCorePoolSize(threadCount);
            }
        }

        private void setCorePoolSize(int threadCount) {
            if (threadCount != m_rawExecutor.getCorePoolSize()) {
                m_rawExecutor.setCorePoolSize(threadCount);
            }
        }
    }
}
