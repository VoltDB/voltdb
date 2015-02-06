/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.jni;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.PlannerStatsCollector;
import org.voltdb.PlannerStatsCollector.CacheUse;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltTableUtil;

/**
 * Wrapper for native Execution Engine library. There are two implementations,
 * one using JNI and one using IPC. ExecutionEngine provides a consistent interface
 * for these implementations to the ExecutionSite.
 */
public abstract class ExecutionEngine implements FastDeserializer.DeserializationMonitor {

    static VoltLogger log = new VoltLogger("HOST");

    public static enum TaskType {
        VALIDATE_PARTITIONING(0),
        APPLY_BINARY_LOG(1);

        private TaskType(int taskId) {
            this.taskId = taskId;
        }

        public final int taskId;
    }

    // is the execution site dirty
    protected boolean m_dirty;

    /** Error codes exported for JNI methods. */
    public static final int ERRORCODE_SUCCESS = 0;
    public static final int ERRORCODE_ERROR = 1; // just error or not so far.
    public static final int ERRORCODE_WRONG_SERIALIZED_BYTES = 101;
    public static final int ERRORCODE_NEED_PLAN = 110;
    public static final int ERRORCODE_PROGRESS_UPDATE = 111;
    public static final int ERRORCODE_DECODE_BASE64_AND_DECOMPRESS = 112;

    /** For now sync this value with the value in the EE C++ code to get good stats. */
    public static final int EE_PLAN_CACHE_SIZE = 1000;

    /** Partition ID */
    protected final int m_partitionId;

    /** Site ID */
    protected final long m_siteId;

    /** Statistics collector (provided later) */
    private PlannerStatsCollector m_plannerStats = null;

    // used for tracking statistics about the plan cache in the EE
    private int m_cacheMisses = 0;
    private int m_eeCacheSize = 0;

    /** Context information of the current running procedure,
     * for logging "long running query" messages */
    private static long INITIAL_LOG_DURATION = 1000; // in milliseconds,
                                                     // not final to allow unit testing
    String m_currentProcedureName = null;
    int m_currentBatchIndex = 0;
    private boolean m_readOnly;
    private long m_startTime;
    private long m_lastMsgTime;
    private long m_logDuration = INITIAL_LOG_DURATION;
    private String[] m_sqlTexts = null;

    /** information about EE calls back to JAVA. For test.*/
    public int m_callsFromEE = 0;
    public long m_lastTuplesAccessed = 0;
    public long m_currMemoryInBytes = 0;
    public long m_peakMemoryInBytes = 0;

    /** Make the EE clean and ready to do new transactional work. */
    public void resetDirtyStatus() {
        m_dirty = false;
    }

    /** Has the database changed any state since the last reset of dirty status? */
    public boolean getDirtyStatus() {
        return m_dirty;
    }

    /** Utility method to verify return code and throw as required */
    final protected void checkErrorCode(final int errorCode) {
        if ((errorCode != ERRORCODE_SUCCESS) && (errorCode != ERRORCODE_NEED_PLAN)) {
            throwExceptionForError(errorCode);
        }
    }

    /**
     * Utility method to generate an EEXception that can be overridden by
     * derived classes. This needs to be implemented by each interface
     * as data is required from the execution engine.
     */
    abstract protected void throwExceptionForError(final int errorCode);

    @Override
    public void deserializedBytes(final int numBytes) {
    }

    /** Create an ee and load the volt shared library */
    public ExecutionEngine(long siteId, int partitionId) {
        m_partitionId = partitionId;
        m_siteId = siteId;
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
        // In mock test environments there may be no stats agent.
        final StatsAgent statsAgent = VoltDB.instance().getStatsAgent();
        if (statsAgent != null) {
            m_plannerStats = new PlannerStatsCollector(siteId);
            statsAgent.registerStatsSource(StatsSelector.PLANNER, siteId, m_plannerStats);
        }
    }

    /** Alternate constructor without planner statistics tracking. */
    public ExecutionEngine() {
        m_partitionId = 0;  // not used
        m_siteId = 0; // not used
        m_plannerStats = null;
    }

    /*
     * State to manage dependency tables for the current work unit.
     * The EE pulls from this state as necessary across JNI (or IPC)
     */
    DependencyTracker m_dependencyTracker = new DependencyTracker();

    /**
     * Called by Java to store dependencies for the EE. Creates
     * a private list of dependencies to be manipulated by the tracker.
     * Does not copy the table data - references WorkUnit's tables.
     * @param dependencies
     */
    public void stashWorkUnitDependencies(final Map<Integer, List<VoltTable>> dependencies) {
        m_dependencyTracker.trackNewWorkUnit(dependencies);
    }

    /**
     * Stash a single dependency. Exists only for test cases.
     * @param depId
     * @param vt
     */
    public void stashDependency(final int depId, final VoltTable vt) {
        m_dependencyTracker.addDependency(depId, vt);
    }


    private class DependencyTracker {
        private final HashMap<Integer, ArrayDeque<VoltTable>> m_depsById =
            new HashMap<Integer, ArrayDeque<VoltTable>>();

        private final VoltLogger hostLog = new VoltLogger("HOST");

        /**
         * Add a single dependency. Exists only for test cases.
         * @param depId
         * @param vt
         */
        void addDependency(final int depId, final VoltTable vt) {
            ArrayDeque<VoltTable> deque = m_depsById.get(depId);
            if (deque == null) {
                deque = new ArrayDeque<VoltTable>();
                m_depsById.put(depId, deque);
            }
            deque.add(vt);
        }

        /**
         * Store dependency tables for later retrieval by the EE.
         * @param workunit
         */
        void trackNewWorkUnit(final Map<Integer, List<VoltTable>> dependencies) {
            for (final Entry<Integer, List<VoltTable>> e : dependencies.entrySet()) {
                // could do this optionally - debug only.
                verifyDependencySanity(e.getKey(), e.getValue());
                // create a new list of references to the workunit's table
                // to avoid any changes to the WorkUnit's list. But do not
                // copy the table data.
                final ArrayDeque<VoltTable> deque = new ArrayDeque<VoltTable>();
                for (VoltTable depTable : e.getValue()) {
                    // A joining node will respond with a table that has this status code
                    if (depTable.getStatusCode() != VoltTableUtil.NULL_DEPENDENCY_STATUS) {
                        deque.add(depTable);
                    }
                }
                // intentionally overwrite the previous dependency id.
                // would a lookup and a clear() be faster?
                m_depsById.put(e.getKey(), deque);
            }
        }

        public VoltTable nextDependency(final int dependencyId) {
            // this formulation retains an arraydeque in the tracker that is
            // overwritten by the next transaction using this dependency id. If
            // the EE requests all dependencies (as is expected), the deque
            // will not retain any references to VoltTables (which is the goal).
            final ArrayDeque<VoltTable> vtstack = m_depsById.get(dependencyId);
            if (vtstack != null && vtstack.size() > 0) {
                // java doc. says this amortized constant time.
                return vtstack.pop();
            }
            else if (vtstack == null) {
                assert(false) : "receive without associated tracked dependency.";
                return null;
            }
            else {
                return null;
            }
        }

        /**
         * Log and exit if a dependency list fails an invariant.
         * @param dependencyId
         * @param dependencies
         */
        void verifyDependencySanity(final Integer dependencyId, final List<VoltTable> dependencies) {
            if (dependencies == null) {
                hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyNotFound.name(),
                               new Object[] { dependencyId }, null);
                VoltDB.crashLocalVoltDB("No additional info.", false, null);
                // Prevent warnings.
                return;
            }
            for (final Object dependency : dependencies) {
                if (dependency == null) {
                    hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyContainedNull.name(),
                                   new Object[] { dependencyId },
                            null);
                    VoltDB.crashLocalVoltDB("No additional info.", false, null);
                    // Prevent warnings.
                    return;
                }
                if (hostLog.isTraceEnabled()) {
                    hostLog.l7dlog(Level.TRACE, LogKeys.org_voltdb_ExecutionSite_ImportingDependency.name(),
                               new Object[] { dependencyId, dependency.getClass().getName(), dependency.toString() },
                               null);
                }
                if (!(dependency instanceof VoltTable)) {
                    hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyNotVoltTable.name(),
                                   new Object[] { dependencyId }, null);
                    VoltDB.crashLocalVoltDB("No additional info.", false, null);
                }
            }

        }

    }



    /*
     * Interface backend invokes to communicate to Java frontend
     */


    /**
     * Call VoltDB.crashVoltDB on behalf of the EE
     * @param reason Reason the EE crashed
     */
    public static void crashVoltDB(String reason, String traces[], String filename, int lineno) {
        VoltLogger hostLog = new VoltLogger("HOST");
        String fn = (filename == null) ? "unknown" : filename;
        String re = (reason == null) ? "Fatal EE error." : reason;
        hostLog.fatal(re + " In " + fn + ":" + lineno);
        if (traces != null) {
            for ( String trace : traces) {
                hostLog.fatal(trace);
            }
        }
        VoltDB.crashLocalVoltDB(re + " In " + fn + ":" + lineno, true, null);
    }

    /**
     * Called from the ExecutionEngine to request serialized dependencies.
     */
    public byte[] nextDependencyAsBytes(final int dependencyId) {
        final VoltTable vt =  m_dependencyTracker.nextDependency(dependencyId);
        if (vt != null) {
            final ByteBuffer buf2 = PrivateVoltTableFactory.getTableDataReference(vt);
            int pos = buf2.position();
            byte[] bytes = new byte[buf2.limit() - pos];
            buf2.get(bytes);
            buf2.position(pos);
            return bytes;
        }
        else {
            return null;
        }
    }

    static final long LONG_OP_THRESHOLD = 10000;
    private static int TIME_OUT_MILLIS = 0; // No time out

    public void setTimeoutLatency(int newLatency) {
        TIME_OUT_MILLIS = newLatency;
    }

    public long fragmentProgressUpdate(int indexFromFragmentTask,
            String planNodeName,
            String lastAccessedTable,
            long lastAccessedTableSize,
            long tuplesProcessed,
            long currMemoryInBytes,
            long peakMemoryInBytes)
    {
        ++m_callsFromEE;
        m_lastTuplesAccessed = tuplesProcessed;
        m_currMemoryInBytes = currMemoryInBytes;
        m_peakMemoryInBytes = peakMemoryInBytes;

        long currentTime = System.currentTimeMillis();
        if (m_startTime == 0) {
            m_startTime = m_lastMsgTime = currentTime;
            return LONG_OP_THRESHOLD;
        }
        long latency = currentTime - m_startTime;

        if (m_readOnly && TIME_OUT_MILLIS > 0 && latency > TIME_OUT_MILLIS) {
            String msg = getLongRunningQueriesMessage(indexFromFragmentTask, latency, planNodeName, true);
            log.info(msg);

            // timing out the long running queries
            return -1 * latency;
        }

        if (currentTime <= (m_logDuration + m_lastMsgTime)) {
            // The callback was triggered earlier than we were ready to log.
            // If this keeps happening, it might makes sense to ramp up the threshold
            // to lower the callback frequency to something closer to the log duration
            // (== the desired log message fequency).
            // That probably involves keeping more stats to estimate the recent tuple processing rate.
            // Such a calibration should probably wait until the next "full batch" and NOT immediately
            // reflected in the current return value which effects the remaining processing of the
            // current batch.
            // It might make more sense to adjust the short-term threshold (possibly downward) to
            // reflect the current tuple processing rate AND the time already elapsed since the last
            // logged message. The goal would be to specifically synchronize the next callback to fall
            // just after m_logDuration + m_lastMsgTime.
            // AFTER the current progress is logged and (possibly) a new log frequency is set, it makes
            // sense to recalibrate the initial/default threshold batch size to minimize the number of
            // future callbacks per log entry, ideally so that one callback arrives just in time to log.
            return LONG_OP_THRESHOLD;
        }
        String msg = getLongRunningQueriesMessage(indexFromFragmentTask, latency, planNodeName, false);
        log.info(msg);

        m_logDuration = (m_logDuration < 30000) ? (2 * m_logDuration) : 30000;
        m_lastMsgTime = currentTime;
        // Return 0 if we want to interrupt ee. Otherwise return the number of tuple operations to let
        // pass before the next call. For now, this is a fixed number. Ideally the threshold would vary
        // to try to get one callback to arrive just after the log duration interval expires.
        return LONG_OP_THRESHOLD;
    }

    private String getLongRunningQueriesMessage(int indexFromFragmentTask,
            long latency, String planNodeName, boolean timeout) {
        String status = timeout ? "timed out at" : "taking a long time to execute -- at least";
        String msg = String.format(
                "Procedure %s is %s " +
                        "%.2f seconds spent accessing " +
                        "%d tuples. Current plan fragment " +
                        "%s in call " +
                        "%d to voltExecuteSQL on site " +
                        "%s. Current temp table uses " +
                        "%d bytes memory, and the peak usage of memory for temp table is " +
                        "%d bytes.",
                        m_currentProcedureName,
                        status,
                        latency / 1000.0,
                        m_lastTuplesAccessed,
                        planNodeName,
                        m_currentBatchIndex,
                        CoreUtils.hsIdToString(m_siteId),
                        m_currMemoryInBytes,
                        m_peakMemoryInBytes);

        if (m_sqlTexts != null
                && indexFromFragmentTask >= 0
                && indexFromFragmentTask < m_sqlTexts.length) {
            msg += "  Executing SQL statement is \"" + m_sqlTexts[indexFromFragmentTask] + "\".";
        }
        else if (m_sqlTexts == null) {
            // Can this happen?
            msg += "  SQL statement text is not available.";
        }
        else {
            // For some reason, the current index in the fragment task message isn't a valid
            // index into the m_sqlTexts array.  We don't expect this to happen,
            // but let's dump something useful if it does.  (See ENG-7610)
            StringBuffer sb = new StringBuffer();
            sb.append("  Unable to report specific SQL statement text for "
                    + "fragment task message index " + indexFromFragmentTask + ".  ");
            sb.append("It MAY be one of these " + m_sqlTexts.length + " items: ");
            for (int i = 0; i < m_sqlTexts.length; ++i) {
                if (m_sqlTexts[i] != null) {
                    sb.append("\"" + m_sqlTexts[i] + "\"");
                }
                else {
                    sb.append("[null]");
                }

                if (i != m_sqlTexts.length - 1) {
                    sb.append(", ");
                }
            }

            msg += sb.toString();
        }

        return msg;
    }

    /**
     * Called from the execution engine to fetch a plan for a given hash.
     * Also update cache stats.
     */
    public byte[] planForFragmentId(long fragmentId) {
        // track cache misses
        m_cacheMisses++;
        // estimate the cache size by the number of misses
        m_eeCacheSize = Math.max(EE_PLAN_CACHE_SIZE, m_eeCacheSize + 1);
        // get the plan for realz
        return ActivePlanRepository.planForFragmentId(fragmentId);
    }

    /*
     * Interface frontend invokes to communicate to CPP execution engine.
     */

    abstract public boolean activateTableStream(final int tableId,
                                                TableStreamType type,
                                                long undoQuantumToken,
                                                byte[] predicates);

    /**
     * Serialize more tuples from the specified table that already has a stream enabled
     *
     * @param tableId Catalog ID of the table to serialize
     * @param outputBuffers Buffers to receive serialized tuple data
     * @return The first number in the pair indicates that there is more data if it's positive,
     * 0 if it's the end of stream, or -1 if there was an error. The second value of the pair is the serialized bytes
     * for each output buffer.
     */
    public abstract Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType type,
                                                               List<DBBPool.BBContainer> outputBuffers);

    public abstract void processRecoveryMessage( ByteBuffer buffer, long pointer);

    /** Releases the Engine object. */
    abstract public void release() throws EEException, InterruptedException;

    public static byte[] getStringBytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    public void loadCatalog(long timestamp, String serializedCatalog) {
        loadCatalog(timestamp, getStringBytes(serializedCatalog));
    }

    /** Pass the catalog to the engine */
    abstract protected void loadCatalog(final long timestamp, final byte[] catalogBytes) throws EEException;

    /** Pass diffs to apply to the EE's catalog to update it */
    abstract public void updateCatalog(final long timestamp, final String diffCommands) throws EEException;

    public void setBatch(int batchIndex) {
        m_currentBatchIndex = batchIndex;
    }

    public void setProcedureName(String procedureName) {
        m_currentProcedureName = procedureName;
    }

    /** Run multiple plan fragments */
    public VoltTable[] executePlanFragments(int numFragmentIds,
                                            long[] planFragmentIds,
                                            long[] inputDepIds,
                                            Object[] parameterSets,
                                            String[] sqlTexts,
                                            long txnId,
                                            long spHandle,
                                            long lastCommittedSpHandle,
                                            long uniqueId,
                                            long undoQuantumToken) throws EEException
    {
        try {
            // For now, re-transform undoQuantumToken to readOnly. Redundancy work in site.executePlanFragments()
            m_readOnly = (undoQuantumToken == Long.MAX_VALUE) ? true : false;

            // reset context for progress updates
            m_startTime = 0;
            m_logDuration = INITIAL_LOG_DURATION;
            m_sqlTexts = sqlTexts;

            VoltTable[] results = coreExecutePlanFragments(numFragmentIds, planFragmentIds, inputDepIds,
                    parameterSets, txnId, spHandle, lastCommittedSpHandle, uniqueId, undoQuantumToken);
            m_plannerStats.updateEECacheStats(m_eeCacheSize, numFragmentIds - m_cacheMisses,
                    m_cacheMisses, m_partitionId);
            return results;
        }
        finally {
            // don't count any cache misses when there's an exception. This is a lie and they
            // will still be used to estimate the cache size, but it's hard to count cache hits
            // during an exception, so we don't count cache misses either to get the right ratio.
            m_cacheMisses = 0;

            m_sqlTexts = null;
        }
    }

    protected abstract VoltTable[] coreExecutePlanFragments(int numFragmentIds,
                                                            long[] planFragmentIds,
                                                            long[] inputDepIds,
                                                            Object[] parameterSets,
                                                            long txnId,
                                                            long spHandle,
                                                            long lastCommittedSpHandle,
                                                            long uniqueId,
                                                            long undoQuantumToken) throws EEException;

    /** Used for test code only (AFAIK jhugg) */
    abstract public VoltTable serializeTable(int tableId) throws EEException;

    abstract public long getThreadLocalPoolAllocations();

    abstract public byte[] loadTable(
        int tableId, VoltTable table, long txnId, long spHandle,
        long lastCommittedSpHandle, boolean returnUniqueViolations, boolean shouldDRStream,
        long undoToken) throws EEException;

    /**
     * Set the log levels to be used when logging in this engine
     * @param logLevels Levels to set
     * @throws EEException
     */
    abstract public boolean setLogLevels(long logLevels) throws EEException;

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    abstract public void tick(long time, long lastCommittedSpHandle);

    /**
     * Instruct EE to come to an idle state. Flush Export buffers, finish
     * any in-progress checkpoint, etc.
     */
    abstract public void quiesce(long lastCommittedSpHandle);

    /**
     * Retrieve a set of statistics using the specified selector from the StatisticsSelector enum.
     * @param selector Selector from StatisticsSelector specifying what statistics to retrieve
     * @param locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Array of results tables. An array of length 0 indicates there are no results. null indicates failure.
     */
    abstract public VoltTable[] getStats(
            StatsSelector selector,
            int locators[],
            boolean interval,
            Long now);

    /**
     * Instruct the EE to start/stop its profiler.
     */
    public abstract void toggleProfiler(int toggle);

    /**
     * Release all undo actions up to and including the specified undo token
     * @param undoToken The undo token.
     */
    public abstract boolean releaseUndoToken(long undoToken);

    /**
     * Undo all undo actions back to and including the specified undo token
     * @param undoToken The undo token.
     */
    public abstract boolean undoUndoToken(long undoToken);

    /**
     * Execute an Export action against the execution engine.
     */
    public abstract void exportAction( boolean syncAction,
            long ackOffset, long seqNo, int partitionId, String tableSignature);

    /**
     * Get the seqNo and offset for an export table.
     * @param tableSignature the signature of the table being polled or acked.
     * @return the response ExportMessage
     */
    public abstract long[] getUSOForExportTable(String tableSignature);

    /**
     * Calculate a hash code for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId table to calculate a hash code for
     */
    public abstract long tableHashCode(int tableId);

    /**
     * Compute the partition to which the parameter value maps using the
     * ExecutionEngine's hashinator.  Currently only valid for int types
     * (tiny, small, integer, big) and strings.
     *
     * THIS METHOD IS CURRENTLY ONLY USED FOR TESTING
     */
    public abstract int hashinate(
            Object value,
            HashinatorConfig config);

    /**
     * Updates the hashinator with new config
     * @param type hashinator type
     * @param config new hashinator config
     */
    public abstract void updateHashinator(HashinatorConfig config);

    /**
     * Execute an arbitrary task that is described by the task id and serialized task parameters.
     * The return value is also opaquely encoded. This means you don't have to update the IPC
     * client when adding new task types
     * @param taskId
     * @param task
     * @return
     */
    public abstract byte[] executeTask(TaskType taskType, ByteBuffer task);

    public abstract ByteBuffer getParamBufferForExecuteTask(int requiredCapacity);

    /*
     * Declare the native interface. Structurally, in Java, it would be cleaner to
     * declare this in ExecutionEngineJNI.java. However, that would necessitate multiple
     * jni_class instances in the execution engine. From the EE perspective, a single
     * JNI class is better.  So put this here with the backend->frontend api definition.
     */

    protected native byte[] nextDependencyTest(int dependencyId);

    /**
     * Just creates a new VoltDBEngine object and returns it to Java.
     * Never fail to destroy() for the VoltDBEngine* once you call this method
     * NOTE: Call initialize() separately for initialization.
     * This does strictly nothing so that this method never throws an exception.
     * @return the created VoltDBEngine pointer casted to jlong.
     */
    protected native long nativeCreate(boolean isSunJVM);
    /**
     * Releases all resources held in the execution engine.
     * @param pointer the VoltDBEngine pointer to be destroyed
     * @return error code
     */
    protected native int nativeDestroy(long pointer);

    /**
     * Initializes the execution engine with given parameter.
     * @param pointer the VoltDBEngine pointer to be initialized
     * @param cluster_id id of the cluster the execution engine belongs to
     * @param siteId this id will be set to the execution engine
     * @param partitionId id of partitioned assigned to this EE
     * @param hostId id of the host this EE is running on
     * @param hostname name of the host this EE is running on
     * @return error code
     */
    protected native int nativeInitialize(
            long pointer,
            int clusterIndex,
            long siteId,
            int partitionId,
            int hostId,
            byte hostname[],
            long tempTableMemory,
            int compactionThreshold);

    /**
     * Sets (or re-sets) all the shared direct byte buffers in the EE.
     * @param pointer
     * @param parameter_buffer
     * @param parameter_buffer_size
     * @param resultBuffer
     * @param result_buffer_size
     * @param exceptionBuffer
     * @param exception_buffer_size
     * @return error code
     */
    protected native int nativeSetBuffers(long pointer, ByteBuffer parameter_buffer, int parameter_buffer_size,
                                          ByteBuffer resultBuffer, int result_buffer_size,
                                          ByteBuffer exceptionBuffer, int exception_buffer_size);

    /**
     * Load the system catalog for this engine.
     * @param pointer the VoltDBEngine pointer
     * @param txnId the catalog is being loaded at
     * @param serialized_catalog the root catalog object serialized as text strings.
     * this parameter is jstring, not jbytearray because Catalog is serialized into
     * human-readable text strings separated by line feeds.
     * @return error code
     */
    protected native int nativeLoadCatalog(long pointer, long timestamp, byte serialized_catalog[]);

    /**
     * Update the EE's catalog.
     * @param pointer the VoltDBEngine pointer
     * @param txnId
     * @param diff_commands Commands to apply to the existing EE catalog to update it
     * @param catalogVersion
     * @return error code
     */
    protected native int nativeUpdateCatalog(long pointer, long timestamp, byte diff_commands[]);

    /**
     * This method is called to initially load table data.
     * @param pointer the VoltDBEngine pointer
     * @param table_id catalog ID of the table
     * @param serialized_table the table data to be loaded
     * @param Length of the serialized table
     * @param undoToken token for undo quantum where changes should be logged.
     * @param returnUniqueViolations If true unique violations won't cause a fatal error and will be returned instead
     * @param undoToken The undo token to release
     */
    protected native int nativeLoadTable(long pointer, int table_id, byte[] serialized_table, long txnId,
            long spHandle, long lastCommittedSpHandle, boolean returnUniqueViolations, boolean shouldDRStream,
            long undoToken);

    /**
     * Executes multiple plan fragments with the given parameter sets and gets the results.
     * @param pointer the VoltDBEngine pointer
     * @param planFragmentIds ID of the plan fragment to be executed.
     * @param inputDepIds list of input dependency ids or null if no deps expected
     * @return error code
     */
    protected native int nativeExecutePlanFragments(
            long pointer,
            int numFragments,
            long[] planFragmentIds,
            long[] inputDepIds,
            long txnId,
            long spHandle, long lastCommittedSpHandle, long uniqueId, long undoToken);

    /**
     * Serialize the result temporary table.
     * @param pointer the VoltDBEngine pointer
     * @param table_id Id of the table to be serialized
     * @param outputBuffer buffer to be filled with the table.
     * @param outputCapacity maximum number of bytes to write to buffer.
     * @return serialized temporary table
     */
    protected native int nativeSerializeTable(long pointer, int table_id,
                                              ByteBuffer outputBuffer, int outputCapacity);

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    protected native void nativeTick(long pointer, long time, long lastCommittedSpHandle);

    /**
     * Native implementation of quiesce engine interface method.
     * @param pointer
     */
    protected native void nativeQuiesce(long pointer, long lastCommittedSpHandle);

    /**
     * Retrieve a set of statistics using the specified selector ordinal from the StatisticsSelector enum.
     * @param stat_selector Ordinal value of a statistic selector from StatisticsSelector.
     * @param catalog_locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Number of result tables, 0 on no results, -1 on failure.
     */
    protected native int nativeGetStats(
            long pointer,
            int stat_selector,
            int catalog_locators[],
            boolean interval,
            long now);

    /**
     * Toggle profile gathering within the execution engine
     * @param mode 0 to disable. 1 to enable.
     * @return 0 on success.
     */
    protected native int nativeToggleProfiler(long pointer, int mode);

    /**
     * Use the EE's hashinator to compute the partition to which the
     * value provided in the input parameter buffer maps.  This is
     * currently a test-only method. Hashinator type and config are also
     * in the parameter buffer
     * @return
     */
    protected native int nativeHashinate(long pointer, long configPtr, int tokenCount);

    /**
     * Updates the EE's hashinator
     */
    protected native void nativeUpdateHashinator(long pointer, int typeId, long configPtr, int tokenCount);

    /**
     * Retrieve the thread local counter of pooled memory that has been allocated
     * @return
     */
    protected static native long nativeGetThreadLocalPoolAllocations();

    /**
     * @param nextUndoToken The undo token to associate with future work
     * @return true for success false for failure
     */
    protected native boolean nativeSetUndoToken(long pointer, long nextUndoToken);

    /**
     * @param undoToken The undo token to release
     * @return true for success false for failure
     */
    protected native boolean nativeReleaseUndoToken(long pointer, long undoToken);

    /**
     * @param undoToken The undo token to undo
     * @return true for success false for failure
     */
    protected native boolean nativeUndoUndoToken(long pointer, long undoToken);

    /**
     * @param pointer Pointer to an engine instance
     * @param logLevels Levels for the various loggers
     * @return true for success false for failure
     */
    protected native boolean nativeSetLogLevels(long pointer, long logLevels);

    /**
     * Active a table stream of the specified type for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId Catalog ID of the table
     * @param streamType type of stream to activate
     * @param undoQuantumToken Undo quantum allowing destructive index clearing to be undone
     * @param data serialized predicates
     * @return <code>true</code> on success and <code>false</code> on failure
     */
    protected native boolean nativeActivateTableStream(long pointer, int tableId, int streamType, long undoQuantumToken, byte[] data);

    /**
     * Serialize more tuples from the specified table that has an active stream of the specified type
     * @param pointer Pointer to an engine instance
     * @param tableId Catalog ID of the table to serialize
     * @param streamType type of stream to pull data from
     * @param data Serialized buffer count and array
     * @return remaining tuple count, 0 when done, or -1 for an error.
     * array of per-buffer byte counts with an extra leading int that is set to
     *         the count of unstreamed tuples, 0 when done, or -1 indicating an error
     *         (such as the table not being COW mode).
     */
    protected native long nativeTableStreamSerializeMore(long pointer, int tableId, int streamType, byte[] data);

    /**
     * Process a recovery message and load the data it contains.
     * @param pointer Pointer to an engine instance
     * @param message Recovery message to load
     */
    protected native void nativeProcessRecoveryMessage(long pointer, long message, int offset, int length);

    /**
     * Calculate a hash code for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId table to calculate a hash code for
     */
    protected native long nativeTableHashCode(long pointer, int tableId);

    /**
     * Execute an arbitrary task based on the task ID and serialized task parameters.
     * This is a generic entry point into the EE that doesn't need to be updated in the IPC
     * client every time you add a new task
     * @param pointer
     */
    protected native void nativeExecuteTask(long pointer);

    /**
     * Perform an export poll or ack action. Poll data will be returned via the usual
     * results buffer. A single action may encompass both a poll and ack.
     * @param pointer Pointer to an engine instance
     * @param mAckOffset The offset being ACKd.
     * @param mTableSignature Signature of the table being acted against
     * @return
     */
    protected native long nativeExportAction(
            long pointer,
            boolean syncAction,
            long mAckOffset,
            long seqNo,
            byte mTableSignature[]);

    /**
     * Get the USO for an export table. This is primarily used for recovery.
     *
     * @param pointer Pointer to an engine instance
     * @param tableId The table in question
     * @return The USO for the export table.
     */
    public native long[] nativeGetUSOForExportTable(long pointer, byte mTableSignature[]);

    /**
     * This code only does anything useful on MACOSX.
     * On LINUX, procfs is read to get RSS
     * @return Returns the RSS size in bytes or -1 on error (or wrong platform).
     */
    public native static long nativeGetRSS();

    /**
     * Start collecting statistics (starts timer).
     */
    protected void startStatsCollection() {
        if (m_plannerStats != null) {
            m_plannerStats.startStatsCollection();
        }
    }

    /**
     * Finalize collected statistics (stops timer and supplies cache statistics).
     *
     * @param cacheSize  size of cache
     * @param cacheUse   where the plan came from
     */
    protected void endStatsCollection(long cacheSize, CacheUse cacheUse) {
        if (m_plannerStats != null) {
            m_plannerStats.endStatsCollection(cacheSize, 0, cacheUse, m_partitionId);
        }
    }

    /**
     * Useful in unit tests.  Allows one to supply a mocked logger
     * to verify that something was logged.
     *
     * @param vl  The new logger to install
     */
    @Deprecated
    public static void setVoltLoggerForTest(VoltLogger vl) {
        log = vl;
    }

    /**
     * Useful in unit tests.  Sets the starting frequency with which
     * the long-running query info message will be logged.
     *
     * @param newDuration  The duration in milliseconds before the first message is logged
     */
    @Deprecated
    public void setInitialLogDurationForTest(long newDuration) {
        INITIAL_LOG_DURATION = newDuration;
    }
}
