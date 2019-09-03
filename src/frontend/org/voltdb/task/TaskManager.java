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

package org.voltdb.task;

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
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.server.handler.ScopedHandler;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.AuthSystem;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.ClientInterfaceRepairCallback;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterConverter;
import org.voltdb.SimpleClientResponseAdapter;
import org.voltdb.StatsAgent;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltDB;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Task;
import org.voltdb.catalog.TaskParameter;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.TaskSettingsType;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.utils.InMemoryJarfile;

import com.google_voltpatches.common.base.MoreObjects;
import com.google_voltpatches.common.base.MoreObjects.ToStringHelper;
import com.google_voltpatches.common.math.DoubleMath;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.ListeningScheduledExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;
import com.google_voltpatches.common.util.concurrent.UnsynchronizedRateLimiter;

/**
 * Manager for the life cycle of the current set of configured {@link Task}s. Each task configuration is represented by
 * a {@link TaskHandler}. Each {@link TaskHandler} will hold a set of {@link SchedulerWrapper}s which are used to wrap
 * the execution of the scheduler and procedures which are scheduled.
 * <p>
 * The task manager will execute procedures which are scheduled to run on each host, database if this host is the leader
 * and all locally led partitions.
 * <p>
 * All manager operations will be executed in the {@link #m_managerExecutor} as well as {@link TaskHandler} methods. The
 * execution of {@link SchedulerWrapper} instances will be split between the {@link #m_managerExecutor} and and the
 * handlers assigned executor. Pure management calls will be executed in the {@link #m_managerExecutor} while scheduled
 * procedures, results and calls to {@link ActionScheduler}s will be handled in the assigned executor.
 */
public final class TaskManager {
    static final VoltLogger log = new VoltLogger("TASK");
    static final String SCOPE_DATABASE = "DATABASE";
    static final String SCOPE_HOSTS = "HOSTS";
    static final String SCOPE_PARTITIONS = "PARTITIONS";
    static final String HASH_ALGO = "SHA-512";
    public static final String SCOPE_DEFAULT = SCOPE_DATABASE;

    private Map<String, TaskHandler> m_handlers = Collections.emptyMap();
    private volatile boolean m_leader = false;
    private AuthSystem m_authSystem;
    private boolean m_started = false;
    private final Set<Integer> m_locallyLedPartitions = new HashSet<>();
    private final SimpleClientResponseAdapter m_adapter = new SimpleClientResponseAdapter(
            ClientInterface.TASK_MANAGER_CID, getClass().getSimpleName());

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

    private static boolean isLastParamaterVarArgs(Method method) {
        if (method.getParameterCount() == 0) {
            return false;
        }
        Parameter[] params = method.getParameters();
        Parameter lastParam = params[params.length - 1];
        return lastParam.getType() == String[].class || lastParam.getType() == Object[].class;
    }

    public TaskManager(ClientInterface clientInterface, StatsAgent statsAgent, int hostId) {
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
        return start(context.getDeployment().getTasks(), context.database.getTasks(), context.authSystem,
                context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously start the scheduler manager and any configured schedules which are eligible to be run on this
     * host.
     *
     * @param configuration Global configuration for all tasks
     * @param tasks         {@link Collection} of configured {@link Task}s
     * @param authSystem    Current {@link AuthSystem} for the system
     * @param classLoader   {@link ClassLoader} to use to load configured classes
     *
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> start(TaskSettingsType configuration, Iterable<Task> tasks, AuthSystem authSystem,
            ClassLoader classLoader) {
        return execute(() -> {
            m_started = true;

            // Create a dummy stats source so something is always reported
            TaskStatsSource.createDummy().register(m_statsAgent);

            processCatalogInline(configuration, tasks, authSystem, classLoader, false);
        });
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param context {@link CatalogContext} instance
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    public ListenableFuture<?> promoteToLeader(CatalogContext context) {
        return promoteToLeader(context.getDeployment().getTasks(), context.database.getTasks(), context.authSystem,
                context.getCatalogJar().getLoader());
    }

    /**
     * Asynchronously promote this host to be the global leader
     *
     * @param configuration Global configuration for all tasks
     * @param tasks         {@link Collection} of configured {@link Task}s
     * @param authSystem    Current {@link AuthSystem} for the system
     * @param classLoader   {@link ClassLoader} to use to load configured classes
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> promoteToLeader(TaskSettingsType configuration, Iterable<Task> tasks, AuthSystem authSystem,
            ClassLoader classLoader) {
        log.debug("MANAGER: Promoted as system leader");
        return execute(() -> {
            m_leader = true;
            processCatalogInline(configuration, tasks, authSystem, classLoader, false);
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
        return processUpdate(context.getDeployment().getTasks(), context.database.getTasks(), context.authSystem,
                context.getCatalogJar().getLoader(), classesUpdated);
    }

    /**
     * Asynchronously process an update to the scheduler configuration
     *
     * @param configuration  Global configuration for all tasks
     * @param tasks          {@link Collection} of configured {@link Task}s
     * @param authSystem     Current {@link AuthSystem} for the system
     * @param classLoader    {@link ClassLoader} to use to load configured classes
     * @param classesUpdated If {@code true} handle classes being updated in the system jar
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> processUpdate(TaskSettingsType configuration, Iterable<Task> tasks, AuthSystem authSystem,
            ClassLoader classLoader, boolean classesUpdated) {
        return execute(
                () -> processCatalogInline(configuration, tasks, authSystem, classLoader, classesUpdated));
    }

    /**
     * Notify the manager that some local partitions have been promoted to leader. Any PARTITION schedules will be
     * asynchronously started for these partitions.
     *
     * @param partitionId which was promoted
     * @return {@link ListenableFuture} which will be completed once the async task completes
     */
    ListenableFuture<?> promotedPartition(int partitionId) {
        if (log.isDebugEnabled()) {
            log.debug("MANAGER: Promoting partition: " + partitionId);
        }
        return execute(() -> {
            if (m_locallyLedPartitions.add(partitionId)) {
                updatePartitionedThreadPoolSize();
                for (TaskHandler sd : m_handlers.values()) {
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
        if (log.isDebugEnabled()) {
            log.debug("MANAGER: Demoting partition: " + partitionId);
        }
        return execute(() -> {
            if (!m_locallyLedPartitions.remove(partitionId)) {
                return false;
            }
            updatePartitionedThreadPoolSize();
            for (TaskHandler sd : m_handlers.values()) {
                sd.demotedPartition(partitionId);
            }
            return true;
        });
    }

    private void updatePartitionedThreadPoolSize() {
        if (m_handlers.values().stream().anyMatch(h -> h instanceof PartitionedTaskHandler)) {
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
                Map<String, TaskHandler> handlers = m_handlers;
                m_handlers = Collections.emptyMap();
                handlers.values().stream().forEach(TaskHandler::cancel);
                m_singleExecutor.getExecutor().shutdown();
                m_partitionedExecutor.getExecutor().shutdown();
            });
        } catch (RejectedExecutionException e) {
            return Futures.immediateFuture(null);
        }
    }

    /**
     * Validate that all tasks present in {@code database} have valid classes and parameters defined.
     *
     * @param database    {@link Database} to be validated
     * @param classLoader {@link ClassLoader} to use to load referenced classes
     * @return An error message or {@code null} if no errors were found
     */
    public static String validateTasks(Database database, ClassLoader classLoader) {
        TaskValidationErrors errors = new TaskValidationErrors();
        for (Task task : database.getTasks()) {
            errors.addErrorMessage(validateTask(task, database, classLoader).getErrorMessage());
        }
        return errors.getErrorMessage();
    }

    /**
     * Create a factory supplier for instances of {@link ActionScheduler} as defined by the provided {@link Task}. If an
     * instance of {@link SchedulerFactory} cannot be constructed using the provided configuration the returned
     * {@link TaskValidationResult} will have an appropriate error message.
     * <p>
     * If {@code database} is null this method will return a {@link TaskValidationResult} or an error message. However
     * if {@code database} is not null the returned {@link TaskValidationResult} will only ever have an error message.
     *
     * @param definition  {@link Task} defining the configuration of the schedule
     * @param database    {@link Database} instance used to validate procedures. May be {@link null}
     * @param classLoader {@link ClassLoader} to use when loading the classes in {@code definition}
     * @return {@link TaskValidationResult} describing any problems encountered or a {@link SchedulerFactory}
     */
    static TaskValidationResult validateTask(Task definition, Database database, ClassLoader classLoader) {
        String schedulerClassName = definition.getSchedulerclass();
        SchedulerFactory factory;
        if (!StringUtils.isBlank(schedulerClassName)) {
            // Construct scheduler from the provided class
            try {
                Pair<String, InitializableFactory<ActionScheduler>> result = createFactory(definition,
                        ActionScheduler.class, schedulerClassName, definition.getSchedulerparameters(), database,
                        classLoader);
                String errorMessage = result.getFirst();
                if (errorMessage != null) {
                    return new TaskValidationResult(errorMessage);
                }
                factory = new SchedulerFactoryImpl(result.getSecond());
            } catch (Exception e) {
                return new TaskValidationResult(
                        String.format("Could not load and construct class: %s", schedulerClassName), e);
            }
        } else {
            // Construct the scheduler by combining a generator with a schedule
            String actionGeneratorClass = definition.getActiongeneratorclass();
            String actionScheduleClass = definition.getScheduleclass();

            if (StringUtils.isBlank(actionGeneratorClass) || StringUtils.isBlank(actionScheduleClass)) {
                return new TaskValidationResult(
                        "If an ActionScheduler is not defined then both an ActionGenerator and ActionSchedule must be defined.");
            }

            InitializableFactory<ActionGenerator> actionGeneratorFactory;
            InitializableFactory<ActionSchedule> actionScheduleFactory;
            try {
                Pair<String, InitializableFactory<ActionGenerator>> result = createFactory(definition,
                        ActionGenerator.class, actionGeneratorClass, definition.getActiongeneratorparameters(),
                        database, classLoader);
                String errorMessage = result.getFirst();
                if (errorMessage != null) {
                    return new TaskValidationResult(errorMessage);
                }
                actionGeneratorFactory = result.getSecond();
            } catch (Exception e) {
                return new TaskValidationResult(
                        String.format("Could not load and construct class: %s", actionGeneratorClass), e);
            }

            try {
                Pair<String, InitializableFactory<ActionSchedule>> result = createFactory(definition,
                        ActionSchedule.class, actionScheduleClass, definition.getScheduleparameters(),
                        database, classLoader);
                String errorMessage = result.getFirst();
                if (errorMessage != null) {
                    return new TaskValidationResult(errorMessage);
                }
                actionScheduleFactory = result.getSecond();
            } catch (Exception e) {
                return new TaskValidationResult(
                        String.format("Could not load and construct class: %s", actionScheduleClass), e);
            }

            factory = database == null ? new CompositeSchedulerFactory(actionGeneratorFactory, actionScheduleFactory)
                    : null;
        }

        return new TaskValidationResult(factory);
    }

    /**
     * Create a factory for constructing {@code className} which implements {@code interfaceClass}. The returned
     * {@link Pair} will have one of an error message or {@link InitializableFactory} and the other will be {@code null}
     *
     * @param <T>                   Type of class the factory will create
     * @param definition            {@link Task} which this factory is associated with
     * @param interfaceClass        Class of the interface which {@code className} should implement
     * @param className             Name of class the factory should construct
     * @param initializerParameters Parameters which are to be passed to constructed instance
     * @param database              {@link Database} instance used to validate procedures. May be {@link null}
     * @param classLoader           {@link ClassLoader} to use to find the class instance of {@code className}
     * @return A {@link Pair} of an errorMessage or {@link InitializableFactory}
     * @throws NoSuchAlgorithmException
     */
    @SuppressWarnings("unchecked")
    private static <T extends Initializable> Pair<String, InitializableFactory<T>> createFactory(Task definition,
            Class<T> interfaceClass, String className, CatalogMap<TaskParameter> initializerParameters,
            Database database, ClassLoader classLoader) throws NoSuchAlgorithmException {
        Class<?> initializableClass;
        try {
            initializableClass = classLoader.loadClass(className);
        } catch (ClassNotFoundException e) {
            return Pair.of("Class does not exist: " + className, null);
        }
        if (!interfaceClass.isAssignableFrom(initializableClass)) {
            return Pair.of(String.format("Class %s is not an instance of %s", className, interfaceClass.getName()),
                    null);
        }

        Constructor<T> constructor;
        try {
            constructor = (Constructor<T>) initializableClass.getConstructor();
        } catch (NoSuchMethodException e) {
            return Pair.of(String.format("Class should have a public no argument constructor: %s", className), null);
        }
        Method initMethod = null;
        for (Method method : initializableClass.getMethods()) {
            if ("initialize".equals(method.getName())) {
                initMethod = method;
                break;
            }
        }

        Object[] parameters;
        boolean takesHelper = false;
        if (initMethod == null) {
            if (!initializerParameters.isEmpty()) {
                return Pair.of(String.format(
                        "Class does not have an initialize method and parameters were provided: %s", className), null);
            }
            parameters = ArrayUtils.EMPTY_OBJECT_ARRAY;
        } else {
            if (initMethod.getReturnType() != void.class) {
                return Pair.of(String.format("Class initialization method is not void: %s", className), null);
            }

            Class<?>[] initMethodParamTypes = initMethod.getParameterTypes();
            takesHelper = TaskHelper.class.isAssignableFrom(initMethodParamTypes[0]);

            int actualParamCount = initializerParameters.size() + (takesHelper ? 1 : 0);
            int minVarArgParamCount = isLastParamaterVarArgs(initMethod) ? initMethodParamTypes.length - 1
                    : Integer.MAX_VALUE;
            if (initMethodParamTypes.length != actualParamCount && minVarArgParamCount > actualParamCount) {
                return Pair.of(String.format(
                        "Class, %s, constructor paremeter count %d does not match provided parameter count %d",
                        className, initMethod.getParameterCount(), initializerParameters.size()), null);
            }

            if (actualParamCount == 0) {
                parameters = ArrayUtils.EMPTY_OBJECT_ARRAY;
            } else {
                parameters = new Object[initMethod.getParameterCount()];
                int indexOffset = takesHelper ? 1 : 0;
                String[] varArgParams = null;
                if (minVarArgParamCount < Integer.MAX_VALUE) {
                    varArgParams = new String[actualParamCount - minVarArgParamCount];
                    parameters[parameters.length - 1] = varArgParams;
                }
                for (TaskParameter sp : initializerParameters) {
                    int index = sp.getIndex() + indexOffset;
                    if (index < minVarArgParamCount) {
                        try {
                            parameters[index] = ParameterConverter.tryToMakeCompatible(initMethodParamTypes[index],
                                    sp.getParameter());
                        } catch (Exception e) {
                            return Pair.of(
                                    String.format("Could not convert parameter %d with the value \"%s\" to type %s: %s",
                                            sp.getIndex(), sp.getParameter(), initMethodParamTypes[index].getName(),
                                            e.getMessage()),
                                    null);
                        }
                    } else {
                        varArgParams[index - minVarArgParamCount] = sp.getParameter();
                    }
                }
            }

            String parameterErrors = validateInitializeParameters(definition, initMethod, parameters, takesHelper,
                    database);
            if (parameterErrors != null) {
                return Pair.of("Error validating parameters for task " + definition.getName() + ": " + parameterErrors,
                        null);
            }
        }

        if (database != null) {
            // Don't bother with the factory since database is only passed in for pure validation
            return Pair.of(null, null);
        }

        byte[] hash = null;
        if (classLoader instanceof InMemoryJarfile.JarLoader) {
            InMemoryJarfile jarFile = ((InMemoryJarfile.JarLoader) classLoader).getInMemoryJarfile();
            hash = jarFile.getClassHash(className, HASH_ALGO);
        }

        return Pair.of(null, new InitializableFactory<>(constructor, initMethod, parameters, takesHelper, hash));
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

    static <T> ListenableFuture<T> addExceptionListener(ListenableFuture<T> future) {
        future.addListener(() -> {
            try {
                if (!future.isCancelled()) {
                    future.get();
                }
            } catch (Exception e) {
                log.error(generateLogMessage("NONE", "Unexpected exception encountered"), e);
            }
        }, MoreExecutors.newDirectExecutorService());
        return future;
    }

    /**
     * Process any potential scheduler changes. Any modified schedules will be stopped and restarted with their new
     * configuration. If a schedule was not modified it will be left running.
     *
     * @param configuration Global configuration for all tasks
     * @param tasks         {@link Collection} of configured {@link Task}s
     * @param authSystem    Current {@link AuthSystem} for the system
     * @param classLoader   {@link ClassLoader} to use to load classes
     */
    private void processCatalogInline(TaskSettingsType configuration, Iterable<Task> tasks, AuthSystem authSystem,
            ClassLoader classLoader, boolean classesUpdated) {
        if (!m_started) {
            return;
        }

        Map<String, TaskHandler> newHandlers = new HashMap<>();
        m_authSystem = authSystem;

        if (configuration == null) {
            if (log.isDebugEnabled()) {
                log.debug("MANAGER: Using default schedules configuration");
            }
            // No configuration provided so use defaults
            configuration = new TaskSettingsType();
        } else if (log.isDebugEnabled()) {
            log.debug("MANAGER: Applying schedule configuration: "
                    + MoreObjects.toStringHelper(configuration).add("minDelayMs", configuration.getMinDelayMs())
                            .add("maxRunFrequency", configuration.getMaxRunFrequency())
                            .add("hostThreadCount", configuration.getHostThreadCount())
                            .add("partitionedThreadCount", configuration.getPartitionedThreadCount()).toString());
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

        for (Task procedureSchedule : tasks) {
            if (log.isDebugEnabled()) {
                ToStringHelper toString = MoreObjects.toStringHelper(procedureSchedule);
                for (String field : procedureSchedule.getFields()) {
                    toString.add(field, procedureSchedule.getField(field));
                }
                log.debug(generateLogMessage(procedureSchedule.getName(),
                        "Applying schedule configuration: " + toString()));
            }
            TaskHandler handler = m_handlers.remove(procedureSchedule.getName());
            TaskValidationResult result = validateTask(procedureSchedule, null, classLoader);

            if (handler != null) {
                // Do not restart a schedule if it has not changed
                if (handler.isSameSchedule(procedureSchedule, result.m_factory, classesUpdated)) {
                    if (log.isDebugEnabled()) {
                        log.debug(generateLogMessage(procedureSchedule.getName(),
                                "Schedule is running and does not need to be restarted"));
                    }
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
                if (log.isDebugEnabled()) {
                    log.debug(generateLogMessage(procedureSchedule.getName(),
                            "Schedule is running and needs to be restarted"));
                }
                handler.cancel();
            }

            String scope = procedureSchedule.getScope();
            if (procedureSchedule.getEnabled() && (m_leader || !SCOPE_DATABASE.equals(scope))) {
                if (!result.isValid()) {
                    log.warn(generateLogMessage(procedureSchedule.getName(), result.getErrorMessage()),
                            result.getException());
                    continue;
                }

                if (log.isDebugEnabled()) {
                    log.debug(generateLogMessage(procedureSchedule.getName(),
                            "Creating handler for scope: " + procedureSchedule.getScope()));
                }

                TaskHandler definition;
                switch (scope) {
                case SCOPE_HOSTS:
                case SCOPE_DATABASE:
                    definition = new SingleTaskHandler(procedureSchedule, scope, result.m_factory,
                            m_singleExecutor.getExecutor());
                    hasNonPartitionedSchedule = true;
                    break;
                case SCOPE_PARTITIONS:
                    definition = new PartitionedTaskHandler(procedureSchedule, result.m_factory,
                            m_partitionedExecutor.getExecutor());
                    m_locallyLedPartitions.forEach(definition::promotedPartition);
                    hasPartitionedSchedule = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported run location: " + procedureSchedule.getScope());
                }
                newHandlers.put(procedureSchedule.getName(), definition);
            }
        }

        // Cancel all removed schedules
        for (TaskHandler handler : m_handlers.values()) {
            handler.cancel();
        }

        // Set the dynamic thread counts based on whether or not schedules exist and partition counts
        m_singleExecutor.setDynamicThreadCount(hasNonPartitionedSchedule ? 1 : 0);
        m_partitionedExecutor.setDynamicThreadCount(hasPartitionedSchedule ? calculatePartitionedThreadPoolSize() : 0);

        // Start all current schedules. This is a no-op for already started schedules
        for (TaskHandler handler : newHandlers.values()) {
            handler.start();
        }

        m_handlers = newHandlers;
    }

    /**
     * Try to find the optional static method {@code validateParameters} and call it if it is compatible to see if the
     * parameters to be passed to the scheduler constructor are valid for the scheduler.
     *
     * @param definition  Instance of {@link Task} defining the schedule
     * @param initMethod  initialize {@link Method} instance for the {@link Initializable}
     * @param parameters  that are going to be passed to the constructor
     * @param takesHelper If {@code true} the first parameter of the init method is a {@link ScopedHandler}
     * @param database    {@link Database} instance used to validate procedures. May be {@link null}
     * @return error message if the parameters are not valid or {@code null} if they are
     */
    private static String validateInitializeParameters(Task definition, Method initMethod, Object[] parameters,
            boolean takesHelper, Database database) {
        Class<?> schedulerClass = initMethod.getDeclaringClass();

        for (Method m : schedulerClass.getMethods()) {
            // Find method declared as: public static void validateParameters
            if (!Modifier.isStatic(m.getModifiers()) || !"validateParameters".equals(m.getName())) {
                continue;
            }

            if (m.getReturnType() != String.class) {
                log.warn(generateLogMessage(definition.getName(), schedulerClass.getName()
                        + " defines a 'validateParameters' method but it does not return a String"));
            }

            if (m.getParameterCount() != initMethod.getParameterCount()) {
                log.warn(generateLogMessage(definition.getName(), schedulerClass.getName()
                        + " defines a 'validateParameters' method but parameter count is not correct. It should be the same as constructor with possibly an optional "
                        + TaskHelper.class.getSimpleName() + " first"));
                continue;
            }

            if (!Arrays.equals(initMethod.getParameterTypes(), m.getParameterTypes())) {
                log.warn(generateLogMessage(definition.getName(), schedulerClass.getName()
                        + " defines a 'validateParameters' method but parameters do not match constructor parameters"));
                continue;
            }

            // Construct the parameter array to be passed to the validate method
            Object[] validatorParameters = parameters.clone();

            if (takesHelper) {
                validatorParameters[0] = new TaskHelper(log, b -> generateLogMessage(definition.getName(), b),
                        definition.getScope(), database);
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
     * When procedure is being restricted by the scope validate that the procedure can be executed within that scope.
     *
     * @see Scheduler#restrictProcedureByScope()
     * @param scope     of the schedule being validated
     * @param procedure {@link Procedure} instance to validate
     * @return {@code null} if procedure is valid for scope otherwise a detailed error message will be returned
     */
    static String isProcedureValidForScope(String scope, Procedure procedure) {
        switch (scope) {
        case SCOPE_DATABASE:
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
            if (procedure.getPartitionparameter() != -1) {
                return String.format(
                        "Procedure %s must be a work procedure which is not partitioned on a table. "
                                + "Cannot be scheduled on a partition.",
                        procedure.getTypeName());
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown scope: " + scope);
        }
        return null;
    }

    /**
     * Result object returned by {@link TaskManager#validateTask(Task, ClassLoader)}. Used to determine if the
     * configuration in {@link Task} is valid and all referenced classes can be constructed and initialized. If any are
     * not valid then an error message and potential exception are contained within this result describing the problem.
     */
    static final class TaskValidationResult {
        final String m_errorMessage;
        final Exception m_exception;
        final SchedulerFactory m_factory;

        TaskValidationResult(String errorMessage) {
            this(errorMessage, null);
        }

        TaskValidationResult(String errorMessage, Exception exception) {
            m_errorMessage = errorMessage;
            m_exception = exception;
            m_factory = null;
        }

        TaskValidationResult(SchedulerFactory factory) {
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
    private abstract class TaskHandler {
        private final Task m_definition;
        private final SchedulerFactory m_factory;

        TaskHandler(Task definition, SchedulerFactory factory) {
            m_definition = definition;
            m_factory = factory;
        }

        /**
         * Does not include {@link Task#getEnabled()} or {@link Task#getOnerror()} in {@code definition} comparison
         *
         * @param definition {@link Task} defining the schedule configuration
         * @param factory    {@link SchedulerFactory} derived from {@code definition}
         * @return {@code true} if both {@code definition} and {@code factory} match those in this handler
         */
        boolean isSameSchedule(Task definition, SchedulerFactory factory, boolean checkHashes) {
            return isSameDefinition(definition) && (!checkHashes || m_factory.doHashesMatch(factory));
        }

        private boolean isSameDefinition(Task definition) {
            return Objects.equals(m_definition.getName(), definition.getName())
                    && Objects.equals(m_definition.getScope(), definition.getScope())
                    && Objects.equals(m_definition.getUser(), definition.getUser())
                    && Objects.equals(m_definition.getSchedulerclass(), definition.getSchedulerclass())
                    && Objects.equals(m_definition.getSchedulerparameters(), definition.getSchedulerparameters())
                    && Objects.equals(m_definition.getActiongeneratorclass(), definition.getActiongeneratorclass())
                    && Objects.equals(m_definition.getActiongeneratorparameters(),
                            definition.getActiongeneratorparameters())
                    && Objects.equals(m_definition.getScheduleclass(), definition.getScheduleclass())
                    && Objects.equals(m_definition.getScheduleparameters(), definition.getScheduleparameters());
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
         * {@link #isSameSchedule(Task, SchedulerFactory, boolean)}
         *
         * @param newDefintion Updated definition
         */
        void updateDefinition(Task newDefintion) {
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

        ActionScheduler constructScheduler(TaskHelper helper) {
            return m_factory.construct(helper);
        }

        String generateLogMessage(String body) {
            return TaskManager.generateLogMessage(m_definition.getName(), body);
        }

        abstract void setMaxRunFrequency(double frequency);
    }

    /**
     * An instance of {@link TaskHandler} which contains a single {@link SchedulerWrapper}. This is used for schedules
     * which are configured for {@link TaskManager#SCOPE_DATABASE} or {@link TaskManager#SCOPE_HOSTS}.
     */
    private class SingleTaskHandler extends TaskHandler {
        private final SchedulerWrapper<? extends SingleTaskHandler> m_wrapper;

        SingleTaskHandler(Task definition, String scope, SchedulerFactory factory,
                ListeningScheduledExecutorService executor) {
            super(definition, factory);

            switch (scope) {
            case SCOPE_HOSTS:
                m_wrapper = new HostSchedulerWrapper(this, executor);
                break;
            case SCOPE_DATABASE:
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
        void updateDefinition(Task newDefintion) {
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
     * An instance of {@link TaskHandler} which contains a {@link SchedulerWrapper} for each locally led partition. This
     * is used for schedules which are configured for {@link TaskManager#SCOPE_PARTITIONS}.
     */
    private class PartitionedTaskHandler extends TaskHandler {
        private final Map<Integer, PartitionSchedulerWrapper> m_wrappers = new HashMap<>();
        private final ListeningScheduledExecutorService m_executor;
        private boolean m_handlerStarted = false;

        PartitionedTaskHandler(Task definition, SchedulerFactory factory, ListeningScheduledExecutorService executor) {
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
        void updateDefinition(Task newDefintion) {
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
     * Base class which wraps the execution and handling of a single {@link ActionScheduler} instance.
     * <p>
     * On start {@link Scheduler#getNextAction(ScheduledAction)} is invoked with a null {@link ScheduledAction}
     * argument. If the result has a status of {@link Action.Type#PROCEDURE} then the provided procedure will be
     * scheduled and executed after the delay. Once a response is received
     * {@link Scheduler#getNextAction(ScheduledAction)} will be invoked again with the {@link ScheduledAction}
     * previously returned and {@link ScheduledAction#getResponse()} updated with response. This repeats until this
     * wrapper is cancelled or {@link Scheduler#getNextAction(ScheduledAction)} returns a non schedule result.
     * <p>
     * This class needs to be thread safe since it's execution is split between the
     * {@link TaskManager#m_managerExecutor} and the schedule executors
     *
     * @param <H> Type of {@link TaskHandler} which created this wrapper
     */
    private abstract class SchedulerWrapper<H extends TaskHandler> {
        ScheduledAction m_scheduledAction;

        final H m_handler;

        private final ListeningScheduledExecutorService m_executor;
        private ActionScheduler m_scheduler;
        private Future<?> m_scheduledFuture;
        private volatile SchedulerWrapperState m_state = SchedulerWrapperState.INITIALIZED;
        private TaskStatsSource m_stats;
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
                if (log.isTraceEnabled()) {
                    log.trace(generateLogMessage("Ignoring start on already initialized schedule"));
                }
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug(generateLogMessage("Starting schedule"));
            }
            m_scheduler = m_handler
                    .constructScheduler(new TaskHelper(log, this::generateLogMessage, getScope(), m_clientInterface));
            if (m_stats == null) {
                m_stats = TaskStatsSource.create(m_handler.getName(), getScope(), getSiteId());
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
            ActionScheduler scheduler;
            synchronized (this) {
                if (m_state != SchedulerWrapperState.RUNNING) {
                    return;
                }
                scheduler = m_scheduler;
            }
            long startTime = System.nanoTime();
            long waitTime = startTime - m_expectedExecutionTime;

            if (log.isTraceEnabled()) {
                log.trace(generateLogMessage("Calling scheduler"));
            }

            DelayedAction action;
            try {
                action = m_scheduledAction == null ? scheduler.getFirstDelayedAction()
                        : m_scheduledAction.callCallback();

            } catch (RuntimeException e) {
                errorOccurred("Scheduler encountered unexpected error", e);
                return;
            }

            if (action == null) {
                errorOccurred("Scheduler returned a null result");
                return;
            }

            m_stats.addSchedulerCall(System.nanoTime() - startTime, waitTime, action.getStatusMessage());

            if (log.isDebugEnabled()) {
                log.debug(generateLogMessage("Scheduler returned action: " + action));
            }

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
                case CALLBACK:
                    runnable = this::handleNextRun;
                    break;
                case PROCEDURE:
                    runnable = this::executeProcedure;
                    break;
                default:
                    throw new IllegalStateException("Unknown status: " + action.getType());
                }

                m_scheduledAction = new ScheduledAction(action);

                try {
                    long delay = calculateDelay();
                    if (log.isTraceEnabled()) {
                        log.trace(generateLogMessage("Scheduling action with delay " + delay));
                    }

                    m_expectedExecutionTime = System.nanoTime() + delay;
                    m_scheduledAction.setExpectedExecutionTime(m_expectedExecutionTime);
                    m_scheduledFuture = addExceptionListener(
                            m_executor.schedule(runnable, delay, TimeUnit.NANOSECONDS));
                } catch (RejectedExecutionException e) {
                    if (log.isDebugEnabled()) {
                        log.debug(generateLogMessage("Could not schedule next procedure scheduler shutdown: "
                                + m_scheduledAction.getProcedure()));
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

            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.setProcName(m_scheduledAction.getProcedure());
            invocation.setParams(m_scheduledAction.getRawProcedureParameters());

            modifyInvocation(procedure, invocation);

            String userName = m_handler.getUser();
            AuthUser user = getUser(userName);
            if (user == null) {
                errorOccurred("User %s does not exist", userName);
                return;
            }

            if (log.isTraceEnabled()) {
                log.trace(generateLogMessage("Executing procedure " + m_scheduledAction.getProcedure() + ' '
                        + invocation.getParams()));
            }

            m_scheduledAction.setStarted();
            if (!m_clientInterface.getInternalConnectionHandler().callProcedure(null, user, false, invocation,
                    procedure, this::handleResponse, false, null)) {
                errorOccurred("Could not call procedure %s", m_scheduledAction.getProcedure());
            }
        }

        private synchronized void handleResponse(ClientResponse response) {
            if (m_state != SchedulerWrapperState.RUNNING) {
                return;
            }

            boolean failed = response.getStatus() != ClientResponse.SUCCESS;
            m_scheduledAction.setResponse(response);
            m_stats.addProcedureCall(m_scheduledAction.getExecutionTime(), m_scheduledAction.getWaitTime(), failed);

            if (failed) {
                String onError = m_handler.getOnError();

                boolean isIgnore = "IGNORE".equalsIgnoreCase(onError);
                if (!isIgnore || log.isDebugEnabled()) {
                    String message = "Procedure " + m_scheduledAction.getProcedure() + " with parameters "
                            + Arrays.toString(m_scheduledAction.getProcedureParameters()) + " failed: "
                            + m_scheduledAction.getResponse().getStatusString();

                    if (isIgnore || "LOG".equalsIgnoreCase(onError)) {
                        log.log(isIgnore ? Level.DEBUG : Level.INFO, generateLogMessage(message), null);
                    } else {
                        errorOccurred(message);
                        return;
                    }
                }
            } else if (log.isTraceEnabled()) {
                log.trace(generateLogMessage("Received response: " + ((ClientResponseImpl) response).toJSONString()));
            } else if (log.isDebugEnabled()) {
                log.debug(generateLogMessage(
                        "Received response: " + ((ClientResponseImpl) response).toStatusJSONString()));
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
            if (log.isDebugEnabled()) {
                log.debug(generateLogMessage("Canceling schedule"));
            }
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
         * Method which can be overridden to modify the {@code invocation} prior to the transacation being created
         *
         * @param procedure  {@link Procedure} which is to be invoked
         * @param invocation {@link StoredProcedureInvocation} describing how to invoke the procedure
         */
        void modifyInvocation(Procedure procedure, StoredProcedureInvocation invocation) {}

        /**
         * @return The {@link Procedure} definition for the procedure in {@link #m_scheduledAction} or {@code null} if
         *         an error was encountered.
         */
        private Procedure getProcedureDefinition() {
            String procedureName = m_scheduledAction.getProcedure();
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
            log.info(generateLogMessage("Schedule is terminating because of an error. "
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
            m_scheduledAction = null;

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
            return Math.max(m_scheduledAction.getDelay(TimeUnit.NANOSECONDS), minDelayNs);
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
     * Wrapper class for schedulers with a run location of {@link TaskManager#SCOPE_DATABASE}
     */
    private class SystemSchedulerWrapper extends SchedulerWrapper<SingleTaskHandler> {
        SystemSchedulerWrapper(SingleTaskHandler definition, ListeningScheduledExecutorService executor) {
            super(definition, executor);
        }

        @Override
        String getScope() {
            return SCOPE_DATABASE;
        }

        @Override
        int getSiteId() {
            return -1;
        }
    }

    /**
     * Wrapper class for schedulers with a run location of {@link TaskManager#SCOPE_HOSTS}
     */
    private class HostSchedulerWrapper extends SchedulerWrapper<SingleTaskHandler> {
        HostSchedulerWrapper(SingleTaskHandler handler, ListeningScheduledExecutorService executor) {
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
     * Wrapper class for schedulers with a run location of {@link TaskManager#SCOPE_PARTITIONS}
     */
    private class PartitionSchedulerWrapper extends SchedulerWrapper<PartitionedTaskHandler> {
        private final int m_partition;

        PartitionSchedulerWrapper(PartitionedTaskHandler handler, int partition,
                ListeningScheduledExecutorService executor) {
            super(handler, executor);
            m_partition = partition;
        }

        @Override
        void modifyInvocation(Procedure procedure, StoredProcedureInvocation invocation) {
            if (procedure.getSinglepartition() && procedure.getPartitionparameter() == -1) {
                invocation.setPartitionDestination(m_partition);
            }
        }

        @Override
        String generateLogMessage(String body) {
            return TaskManager.generateLogMessage(m_handler.getName() + " P" + m_partition, body);
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

    private interface SchedulerFactory {
        /**
         * @param helper which can be passed to the constructed classes
         * @return New instance of an {@link ActionScheduler}
         */
        ActionScheduler construct(TaskHelper helper);

        /**
         * Compare the hashes of the classes used to construct the {@link ActionScheduler} returned by this factory and
         * {@code other}
         *
         * @param other {@link SchedulerFactory} to compare with
         * @return {@code true} if this factory and {@code other} are using the same classes
         */
        boolean doHashesMatch(SchedulerFactory other);
    }

    /**
     * Class for constructing class which implement {@link Initializable}
     *
     * @param <T> Type of class being constructed
     */
    private static class InitializableFactory<T extends Initializable> {
        private final Constructor<T> m_constructor;
        private final Method m_initMethod;
        private final Object[] m_parameters;
        private final boolean m_takesHelper;
        private final byte[] m_classHash;
        Collection<String> m_classDeps = null;

        InitializableFactory(Constructor<T> constructor, Method initMethod, Object[] parameters,
                boolean takesHelper, byte[] classHash) {
            super();
            this.m_constructor = constructor;
            this.m_initMethod = initMethod;
            this.m_parameters = parameters;
            this.m_takesHelper = takesHelper;
            this.m_classHash = classHash;
        }

        public T construct(TaskHelper helper) {
            try {
                T scheduler = m_constructor.newInstance();
                if (m_initMethod != null) {
                    if (m_takesHelper) {
                        m_parameters[0] = helper;
                    }
                    m_initMethod.invoke(scheduler, m_parameters);
                }
                if (m_classDeps == null) {
                    m_classDeps = scheduler.getDependencies();
                }
                return scheduler;
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                throw new IllegalArgumentException(e);
            }
        }

        public boolean doHashesMatch(InitializableFactory<T> other) {
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
     * Factory for directly constructing a {@link ActionScheduler}
     */
    private static final class SchedulerFactoryImpl implements SchedulerFactory {
        private final InitializableFactory<ActionScheduler> m_factory;

        SchedulerFactoryImpl(InitializableFactory<ActionScheduler> factory) {
            m_factory = factory;
        }

        @Override
        public ActionScheduler construct(TaskHelper helper) {
            return m_factory.construct(helper);
        }

        @Override
        public boolean doHashesMatch(SchedulerFactory other) {
            if (getClass() != other.getClass()) {
                return false;
            }
            return m_factory.doHashesMatch(((SchedulerFactoryImpl) other).m_factory);
        }
    }

    /**
     * Factory for constructing a {@link ActionScheduler} from an {@link ActionGenerator} and {@link ActionSchedule}
     */
    private static final class CompositeSchedulerFactory implements SchedulerFactory {
        private final InitializableFactory<ActionGenerator> m_actionGeneratorFactory;
        private final InitializableFactory<ActionSchedule> m_actionScheduleFactory;

        CompositeSchedulerFactory(InitializableFactory<ActionGenerator> actionGeneratorFactory,
                InitializableFactory<ActionSchedule> actionScheduleFactory) {
            super();
            m_actionGeneratorFactory = actionGeneratorFactory;
            m_actionScheduleFactory = actionScheduleFactory;
        }

        @Override
        public ActionScheduler construct(TaskHelper helper) {
            return new CompositeActionScheduler(m_actionGeneratorFactory.construct(helper),
                    m_actionScheduleFactory.construct(helper));
        }

        @Override
        public boolean doHashesMatch(SchedulerFactory other) {
            if (getClass() != other.getClass()) {
                ;
            }
            CompositeSchedulerFactory otherFactory = (CompositeSchedulerFactory) other;
            return m_actionGeneratorFactory.doHashesMatch(otherFactory.m_actionGeneratorFactory)
                    && m_actionScheduleFactory.doHashesMatch(otherFactory.m_actionScheduleFactory);
        }
    }

    /**
     * Utility class to wrap a {@link ListeningScheduledExecutorService} so that it can have either an explicit thread
     * count or a dynamic thread count.
     */
    static private final class ScheduledExecutorHolder {
        private final String m_name;
        private final ScheduledThreadPoolExecutor m_rawExecutor;
        private final ListeningScheduledExecutorService m_executor;
        private boolean m_dynamicThreadCount = true;

        ScheduledExecutorHolder(String name) {
            m_name = name;
            m_rawExecutor = CoreUtils.getScheduledThreadPoolExecutor("Task-" + name, 0,
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
                if (log.isTraceEnabled()) {
                    log.trace("MANAGER: Updating dynamic thread count to " + threadCount + " on " + m_name);
                }
                setCorePoolSize(threadCount);
            }
        }

        private void setCorePoolSize(int threadCount) {
            if (threadCount != m_rawExecutor.getCorePoolSize()) {
                m_rawExecutor.setCorePoolSize(threadCount);
                m_rawExecutor.setMaximumPoolSize(Math.max(threadCount, 1));
            }
        }
    }
}
