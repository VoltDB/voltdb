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

package org.voltdb.jni;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.CatalogContext;
import org.voltdb.PlannerStatsCollector;
import org.voltdb.PlannerStatsCollector.CacheUse;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.UserDefinedFunctionManager;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dr2.DRProtocol;
import org.voltdb.exceptions.EEException;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.largequery.LargeBlockManager;
import org.voltdb.largequery.LargeBlockResponse;
import org.voltdb.largequery.LargeBlockTask;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.sysprocs.saverestore.HiddenColumnFilter;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTrace;

/**
 * Wrapper for native Execution Engine library. There are two implementations,
 * one using JNI and one using IPC. ExecutionEngine provides a consistent interface
 * for these implementations to the ExecutionSite.
 */

public abstract class ExecutionEngine implements FastDeserializer.DeserializationMonitor {

    protected static VoltLogger LOG = new VoltLogger("HOST");
    protected static VoltLogger EXPORT_LOG = new VoltLogger("EXPORT");

    public static enum TaskType {
        VALIDATE_PARTITIONING(0),
        GET_DR_TUPLESTREAM_STATE(1),
        SET_DR_SEQUENCE_NUMBERS(2),
        SET_DR_PROTOCOL_VERSION(3),
        JAVA_GET_DRID_TRACKER(4),
        GENERATE_DR_EVENT(5),
        RESET_DR_APPLIED_TRACKER(6),
        SET_MERGED_DRID_TRACKER(7),
        INIT_DRID_TRACKER(8),
        RESET_DR_APPLIED_TRACKER_SINGLE(9),
        ELASTIC_CHANGE(10);

        private TaskType(int taskId) {
            this.taskId = taskId;
        }

        public final int taskId;
    }

    // keep sync with DREventType in ee/src/common/types.h
    public static enum EventType {
        NOT_A_EVENT(0),
        POISON_PILL(1),
        CATALOG_UPDATE(2),
        DR_STREAM_START(3),
        SWAP_TABLE(4),
        DR_STREAM_END(5),
        DR_ELASTIC_CHANGE(6),
        DR_ELASTIC_REBALANCE(7);

        private EventType(int typeId) {
            this.typeId = typeId;
        }

        public final int typeId;
    }

    public static enum FragmentContext {
        UNKNOWN,
        RO_BATCH,
        RW_BATCH,
        CATALOG_UPDATE,
        CATALOG_LOAD
    }

    /**
     * Enum needs to align with LoadTableCaller in ee
     */
    public static enum LoadTableCaller {
        SNAPSHOT_REPORT_UNIQ_VIOLATIONS(0, false),
        SNAPSHOT_THROW_ON_UNIQ_VIOLATION(1, false),
        DR(2, false),
        BALANCE_PARTITIONS(3),
        CLIENT(4);

        private final byte m_id;
        private final boolean m_undo;

        private LoadTableCaller(int id) {
            this(id, true);
        }

        private LoadTableCaller(int id, boolean undo) {
            m_id = (byte) id;
            m_undo = undo;
        }

        public byte getId() {
            return m_id;
        }

        public boolean createUndoToken() {
            return m_undo;
        }
    }

    private FragmentContext m_fragmentContext = FragmentContext.UNKNOWN;

    // is the execution site dirty
    // TODO: deprecate the dirty bit?
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
    private static final long LONG_OP_THRESHOLD = 10000;

    public static final int NO_BATCH_TIMEOUT_VALUE = 0;
    /** Fragment or batch time out in milliseconds.
     *  By default 0 means no time out setting.
     */
    private int m_batchTimeout = NO_BATCH_TIMEOUT_VALUE;

    String m_currentProcedureName = null;
    int m_currentBatchIndex = 0;
    boolean m_usingFallbackBuffer = false;
    private long m_startTime;
    private long m_lastMsgTime;
    private long m_logDuration = INITIAL_LOG_DURATION;
    private String[] m_sqlTexts = null;

    /** information about EE calls back to JAVA. For test.*/
    public int m_callsFromEE = 0;
    public long m_lastTuplesAccessed = 0;
    public long m_currMemoryInBytes = 0;
    public long m_peakMemoryInBytes = 0;

    protected UserDefinedFunctionManager m_functionManager = new UserDefinedFunctionManager();

    public void loadFunctions(CatalogContext catalogContext) {
        m_functionManager.loadFunctions(catalogContext);
    }

    /** Make the EE clean and ready to do new transactional work. */
    public void resetDirtyStatus() {
        m_dirty = false;
    }

    /** Has the database changed any state since the last reset of dirty status? */
    public boolean getDirtyStatus() {
        return m_dirty;
    }

    public boolean usingFallbackBuffer() {
        return m_usingFallbackBuffer;
    }

    public void setBatchTimeout(int batchTimeout) {
        m_batchTimeout = batchTimeout;
    }

    public int getBatchTimeout() {
        return m_batchTimeout;
    }

    private boolean shouldTimedOut (long latency) {
        if (m_fragmentContext == FragmentContext.RO_BATCH
                && m_batchTimeout > NO_BATCH_TIMEOUT_VALUE
                && m_batchTimeout < latency) {
            return true;
        }
        return false;
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
    protected abstract void throwExceptionForError(final int errorCode);

    @Override
    public void deserializedBytes(final int numBytes) {
    }

    /** Create an ee and load the volt shared library */
    public ExecutionEngine(long siteId, int partitionId) {
        m_partitionId = partitionId;
        m_siteId = siteId;
        org.voltdb.NativeLibraryLoader.loadVoltDB();
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
            new HashMap<>();

        private final VoltLogger hostLog = new VoltLogger("HOST");

        /**
         * Add a single dependency. Exists only for test cases.
         * @param depId
         * @param vt
         */
        void addDependency(final int depId, final VoltTable vt) {
            ArrayDeque<VoltTable> deque = m_depsById.get(depId);
            if (deque == null) {
                deque = new ArrayDeque<>();
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
                final ArrayDeque<VoltTable> deque = new ArrayDeque<>();
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
                hostLog.fatalFmt("dependency(id=%s) was not found.", dependencyId);
                VoltDB.crashLocalVoltDB("No additional info.", false, null);
                // Prevent warnings.
                return;
            }
            for (final Object dependency : dependencies) {
                if (dependency == null) {
                    hostLog.fatalFmt("dependency(id=%s) contained a null.", dependencyId);
                    VoltDB.crashLocalVoltDB("No additional info.", false, null);
                    // Prevent warnings.
                    return;
                }
                if (hostLog.isTraceEnabled()) {
                    hostLog.traceFmt("importing a dependency (id=%s):%s: toString=%s ...",
                                     dependencyId, dependency.getClass().getName(), dependency.toString());
                }
                if (!(dependency instanceof VoltTable)) {
                    hostLog.fatalFmt("dependency(id=%s) was not VoltTable type", dependencyId);
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

    public void traceLog(boolean isBegin, String name, String args)
    {
        if (isBegin) {
            final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.EE);
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.beginDuration(name,
                                                           "partition", Integer.toString(m_partitionId),
                                                           "info", args));
            }
        } else {
            final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.EE);
            if (traceLog != null) {
                traceLog.add(VoltTrace::endDuration);
            }
        }
    }

    public long fragmentProgressUpdate(int indexFromFragmentTask,
            int planNodeTypeAsInt,
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

        if (shouldTimedOut(latency)) {

            String msg = getLongRunningQueriesMessage(indexFromFragmentTask, latency, planNodeTypeAsInt, true);
            LOG.info(msg);

            // timing out the long running queries
            return -1 * latency;
        }

        if (currentTime <= (m_logDuration + m_lastMsgTime)) {
            // The callback was triggered earlier than we were ready to log.
            // If this keeps happening, it might makes sense to ramp up the threshold
            // to lower the callback frequency to something closer to the log duration
            // (== the desired log message frequency).
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
        String msg = getLongRunningQueriesMessage(indexFromFragmentTask, latency, planNodeTypeAsInt, false);
        LOG.info(msg);

        m_logDuration = (m_logDuration < 30000) ? (2 * m_logDuration) : 30000;
        m_lastMsgTime = currentTime;
        // Return 0 if we want to interrupt ee. Otherwise return the number of tuple operations to let
        // pass before the next call. For now, this is a fixed number. Ideally the threshold would vary
        // to try to get one callback to arrive just after the log duration interval expires.
        return LONG_OP_THRESHOLD;
    }

    private String getLongRunningQueriesMessage(int indexFromFragmentTask,
            long latency, int planNodeTypeAsInt, boolean timeout) {
        StringBuilder sb = new StringBuilder();

        // First describe what is taking a long time.
        switch (m_fragmentContext) {
        default:
        case RO_BATCH:
        case RW_BATCH:
            sb.append("Procedure " + m_currentProcedureName);
            break;

        case CATALOG_UPDATE:
            sb.append("Catalog update");
            break;

        case CATALOG_LOAD:
            sb.append("Catalog load");
            break;
        }

        // Timing out (canceling) versus just reporting something taking a long time
        sb.append(timeout ? " is timed out at " : " is taking a long time to execute -- at least ");
        sb.append(String.format("%.2f seconds spent accessing %d tuples.",
                latency / 1000.0, m_lastTuplesAccessed));

        // Type of plan node executing, and index in batch if known.
        sb.append(" Current plan fragment " + PlanNodeType.get(planNodeTypeAsInt).name());
        if (indexFromFragmentTask >= 0) {
            sb.append(" in call " + m_currentBatchIndex + " to voltExecuteSQL");
        }
        sb.append(" on site " + CoreUtils.hsIdToString(m_siteId) + ".");

        if (m_currMemoryInBytes > 0 && m_peakMemoryInBytes > 0) {
            sb.append(" Current temp table uses " + m_currMemoryInBytes + " bytes memory,"
                    + " and the peak usage of memory for temp tables is " + m_peakMemoryInBytes + " bytes.");
        }

        if (m_sqlTexts != null
                && indexFromFragmentTask >= 0
                && indexFromFragmentTask < m_sqlTexts.length) {
            sb.append(" Executing SQL statement is \"" + m_sqlTexts[indexFromFragmentTask] + "\".");
        }
        else if (m_sqlTexts == null) {
            // Can this happen?
            sb.append(" SQL statement text is not available.");
        }
        else {
            // For some reason, the current index in the fragment task message isn't a valid
            // index into the m_sqlTexts array.  We don't expect this to happen,
            // but let's dump something useful if it does.  (See ENG-7610)
            sb.append(" Unable to report specific SQL statement text for "
                    + "fragment task message index " + indexFromFragmentTask + ".");
            sb.append(" It MAY be one of these " + m_sqlTexts.length + " items: ");
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
        }

        return sb.toString();
    }

    /**
     * Called from the execution engine to fetch a plan for a given hash.
     * Also update cache stats.
     */
    public byte[] planForFragmentId(long fragmentId) {
        // track cache misses
        m_cacheMisses++;
        // estimate the cache size by the number of misses
        if (m_eeCacheSize < EE_PLAN_CACHE_SIZE) {
            m_eeCacheSize++;
        }
        // get the plan for realz
        return ActivePlanRepository.planForFragmentId(fragmentId);
    }

    /*
     * Interface frontend invokes to communicate to CPP execution engine.
     */
    /**
     * Retrieve the schema for a table from the EE
     *
     * @param tableId            ID of the table
     * @param hiddenColumnFilter {@link HiddenColumnFilter} to indicate which hidden columns to include in the schema
     * @param forceLive          if {@code true} the current catalog schema will be used and not the one associated with
     *                           the snapshot
     * @return {@link Pair} with {@code first} being the encoded schema and {@code second} being the partition column id
     */
    public abstract Pair<byte[], Integer> getSnapshotSchema(int tableId, HiddenColumnFilter hiddenColumnFilter,
            boolean forceLive);

    public abstract boolean activateTableStream(final int tableId,
                                                TableStreamType type,
                                                HiddenColumnFilter hiddenColumnFilter,
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

    /** Releases the Engine object. */
    public abstract void release() throws EEException, InterruptedException;

    /** Reset the Engine object. */
    public abstract void decommission(boolean remove, boolean promote, int newSitePerHost) throws EEException, InterruptedException;

    public static byte[] getStringBytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    /** Pass the catalog to the engine */
    public void loadCatalog(long timestamp, String serializedCatalog) {
        try {
            setupProcedure(null);
            m_fragmentContext = FragmentContext.CATALOG_LOAD;
            coreLoadCatalog(timestamp, getStringBytes(serializedCatalog));
        }
        finally {
            m_fragmentContext = FragmentContext.UNKNOWN;
        }
    }

    protected abstract void coreLoadCatalog(final long timestamp, final byte[] catalogBytes) throws EEException;

    /** Pass diffs to apply to the EE's catalog to update it */
    public final void updateCatalog(final long timestamp, final boolean isStreamUpdate, final String diffCommands,
            Map<Byte, String[]> replicableTables) throws EEException {
        try {
            setupProcedure(null);
            m_fragmentContext = FragmentContext.CATALOG_UPDATE;
            coreUpdateCatalog(timestamp, isStreamUpdate, diffCommands);

            if (replicableTables != null) {
                replicableTables.forEach(this::setReplicableTables);
            }
        }
        finally {
            m_fragmentContext = FragmentContext.UNKNOWN;
        }
    }

    protected abstract void coreUpdateCatalog(final long timestamp, final boolean isStreamUpdate, final String diffCommands) throws EEException;

    public void setBatch(int batchIndex) {
        m_currentBatchIndex = batchIndex;
    }

    public void setupProcedure(String procedureName) {
        m_currentProcedureName = procedureName;
        m_startTime = 0;
        m_logDuration = INITIAL_LOG_DURATION;
    }

    public void completeProcedure() {
        m_currentProcedureName = null;
    }

    /** Run multiple plan fragments */
    public FastDeserializer executePlanFragments(
            int numFragmentIds,
            long[] planFragmentIds,
            long[] inputDepIds,
            Object[] parameterSets,
            DeterminismHash determinismHash,
            String[] sqlTexts,
            boolean[] isWriteFrags,
            int[] sqlCRCs,
            long txnId,
            long spHandle,
            long lastCommittedSpHandle,
            long uniqueId,
            long undoQuantumToken,
            boolean traceOn) throws EEException {
        try {
            // For now, re-transform undoQuantumToken to readOnly. Redundancy work in site.executePlanFragments()
            m_fragmentContext = (undoQuantumToken == Long.MAX_VALUE) ? FragmentContext.RO_BATCH : FragmentContext.RW_BATCH;

            // reset context for progress updates
            m_sqlTexts = sqlTexts;

            if (traceOn) {
                final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPSITE);
                if (traceLog != null) {
                    traceLog.add(() -> VoltTrace.beginDuration("execplanfragment",
                                                               "txnId", TxnEgo.txnIdToString(txnId),
                                                               "partition", Integer.toString(m_partitionId)));
                }
            }

            FastDeserializer results = coreExecutePlanFragments(m_currentBatchIndex, numFragmentIds, planFragmentIds,
                    inputDepIds, parameterSets, determinismHash, isWriteFrags, sqlCRCs, txnId, spHandle, lastCommittedSpHandle,
                    uniqueId, undoQuantumToken, traceOn);

            if (traceOn) {
                final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPSITE);
                if (traceLog != null) {
                    traceLog.add(VoltTrace::endDuration);
                }
            }

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

            m_fragmentContext = FragmentContext.UNKNOWN;
        }
    }

    public abstract FastDeserializer coreExecutePlanFragments(
            int batchIndex,
            int numFragmentIds,
            long[] planFragmentIds,
            long[] inputDepIds,
            Object[] parameterSets,
            DeterminismHash determinismHash,
            boolean[] isWriteFrags,
            int[] sqlCRCs,
            long txnId,
            long spHandle,
            long lastCommittedSpHandle,
            long uniqueId,
            long undoQuantumToken,
            boolean traceOn) throws EEException;

    public abstract void setPerFragmentTimingEnabled(boolean enabled);

    // Extract the per-fragment stats from the buffer.
    public abstract int extractPerFragmentStats(int batchSize, long[] executionTimesOut);

    /** Used for test code only (AFAIK jhugg) */
    public abstract VoltTable serializeTable(int tableId) throws EEException;

    public abstract long getThreadLocalPoolAllocations();

    public abstract byte[] loadTable(
        int tableId, VoltTable table, long txnId, long spHandle,
            long lastCommittedSpHandle, long uniqueId, long undoToken, LoadTableCaller caller) throws EEException;

    /**
     * Set the log levels to be used when logging in this engine
     * @param logLevels Levels to set
     * @throws EEException
     */
    public abstract boolean setLogLevels(long logLevels) throws EEException;

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    public abstract void tick(long time, long lastCommittedSpHandle);

    /**
     * Instruct EE to come to an idle state. Flush Export buffers, finish
     * any in-progress checkpoint, etc.
     */
    public abstract void quiesce(long lastCommittedSpHandle);

    /**
     * Retrieve a set of statistics using the specified selector from the StatisticsSelector enum.
     * @param selector Selector from StatisticsSelector specifying what statistics to retrieve
     * @param locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Array of results tables. An array of length 0 indicates there are no results. null indicates failure.
     */
    public abstract VoltTable[] getStats(
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
    public abstract boolean releaseUndoToken(long undoToken, boolean isEmptyDRTxn);

    /**
     * Undo all undo actions back to and including the specified undo token
     * @param undoToken The undo token.
     */
    public abstract boolean undoUndoToken(long undoToken);

    /**
     * Execute an Export action against the execution engine.
     */
    public abstract void setExportStreamPositions(ExportSnapshotTuple sequences, int partitionId, String streamName);

    /**
     * Execute an Delete of migrated rows in the execution engine.
     */
    public abstract boolean deleteMigratedRows(
            long txnid, long spHandle, long uniqueId,
            String tableName, long deletableTxnId, long undoToken);

    /**
     * Get the seqNo and offset for an export table.
     * @param streamName the name of the stream being polled.
     * @return the response ExportMessage
     */
    public abstract long[] getUSOForExportTable(String streamName);

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
     * Apply binary log data. To be able to advance the DR sequence number and
     * regenerate binary log for chaining correctly, proper spHandle and
     * lastCommittedSpHandle from the current transaction need to be passed in.
     * @param logs                     The binary log data
     * @param txnId                    The txnId of the current transaction
     * @param spHandle                 The spHandle of the current transaction
     * @param lastCommittedSpHandle    The spHandle of the last committed transaction
     * @param uniqueId                 The uniqueId of the current transaction
     * @param remoteClusterId          The cluster id of producer cluster
     * @param undoToken                For undo
     * @throws EEException
     */
    public abstract long applyBinaryLog(ByteBuffer logs, long txnId, long spHandle, long lastCommittedSpHandle,
            long uniqueId, int remoteClusterId, long undoToken) throws EEException;

    /**
     * Execute an arbitrary non-transactional task that is described by the task id and
     * serialized task parameters. The return value is also opaquely encoded. This means
     * you don't have to update the IPC client when adding new task types.
     *
     * This method shouldn't be used to perform transactional tasks such as mutating
     * table data because it doesn't setup transaction context in the EE.
     * @param taskId
     * @param task
     * @return
     */
    public abstract byte[] executeTask(TaskType taskType, ByteBuffer task) throws EEException;

    public abstract ByteBuffer getParamBufferForExecuteTask(int requiredCapacity);

    /**
     * Pause/resume the maintenance of materialized views specified in viewNames.
     */
    public abstract void setViewsEnabled(String viewNames, boolean enabled);

    /**
     * Use this to disable writing to all streams from EE like export and DR.
     * Currently used by elastic shrink to stop a site from writing to export and DR streams
     * once all its data has been migrated and it is ready to be shutdown.
     * All streams are enabled by default.
     */
    public abstract void disableExternalStreams();

    /**
     * Return the EE state that indicates if external streams are enabled for this Site or not.
     */
    public abstract boolean externalStreamsEnabled();

    /**
     * Store a topics group and its members in the system tables
     *
     * @param undoToken       to be used for this transaction
     * @param serializedGroup topics group serialized into a byte[]
     */
    public abstract void storeTopicsGroup(long undoToken, byte[] serializedGroup);

    /**
     * Delete a topics group, all members and all offsets
     *
     * @param undoToken to be used for this transaction
     * @param groupId   ID of group to be dleted
     */
    public abstract void deleteTopicsGroup(long undoToken, String groupId);

    /**
     * Start or continue a fetch of all group from this site.
     *
     * If the Boolean in the return is {@code true} then there are more groups to fetch and this method should be called
     * repeatedly with the last groupId returned until {@code false} is returned.
     *
     * @param maxResultSize for any result returned by this fetch
     * @param groupId       non-inclusive start point for iterating over groups. {@code null} means start at begining
     * @return A {@link Pair} indicating if there are more groups and the serialized groups
     */
    public abstract Pair<Boolean, byte[]> fetchTopicsGroups(int maxResultSize, String groupId);

    /**
     * Commit topic partition offsets for a topics group in the system tables
     *
     * @param spUniqueId     for this transaction
     * @param undoToken      to be used for this transaction
     * @param requestVersion Version of the topics message making this request
     * @param groupId        ID of the group
     * @param offsets        serialized offsets to be stored
     * @return response to requester
     */
    public abstract byte[] commitTopicsGroupOffsets(long spUniqueId, long undoToken, short requestVersion,
            String groupId, byte[] offsets);

    /**
     * Fetch topic partition offsets for a topics group from the system tables
     *
     * @param requestVersion version of the topics message making this request
     * @param groupId        IF of group
     * @param offsets        serialized offsets being requested
     * @return response to requester
     */
    public abstract byte[] fetchTopicsGroupOffsets(short requestVersion, String groupId, byte[] offsets);

    /**
     * Delete the expired offsets of standalone groups. An offset is expired if its commit timestamp is <
     * deleteOlderThan
     *
     * @param undoToken       to be used for this transaction
     * @param deleteOlderThan timestamp to use to select what offsets should be deleted
     */
    public abstract void deleteExpiredTopicsOffsets(long undoToken, TimestampType deleteOlderThan);

    /**
     * Set the list of tables which can be replicated to from {@code clusterId}
     *
     * @param clusterId of producer cluster
     * @param tables    which match in both schemas and can be the target of replication. If {@code null} then the
     *                  tables will be removed
     */
    public abstract void setReplicableTables(int clusterId, String[] tables);

    /**
     * Clear all clusters replicable tables
     */
    public abstract void clearAllReplicableTables();

    /**
     * Clear replicable tables for cluster
     */
    public abstract void clearReplicableTables(int clusterId);

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
     * Temporarily decommission the execution engine.
     * @param pointer the VoltDBEngine pointer to be destroyed
     * @param remove
     * @param promote
     * @param newSitePerHost
     * @return error code
     */
    protected native int nativeDecommission(long pointer, boolean remove, boolean promote, int newSitePerHost);

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
            int sitesPerHost,
            int hostId,
            byte hostname[],
            int drClusterId,
            int defaultDrBufferSize,
            boolean drIgnoreConflicts,
            int drCrcErrorIgnoreMax,
            boolean drCrcErrorIgnoreFatal,
            long tempTableMemory,
            boolean createDrReplicatedStream,
            int compactionThreshold);

    /**
     * Sets (or re-sets) all the shared direct byte buffers in the EE.
     * @param pointer
     * @param parameter_buffer
     * @param parameter_buffer_size
     * @param per_fragment_stats_buffer
     * @param per_fragment_stats_buffer_size
     * @param udf_buffer
     * @param udf_buffer_size
     * @param first_result_buffer
     * @param first_result_buffer_size
     * @param final_result_buffer
     * @param final_result_buffer_size
     * @param exception_buffer
     * @param exception_buffer_size
     * @return error code
     */
    protected native int nativeSetBuffers(long pointer,
                                          ByteBuffer parameter_buffer,          int parameter_buffer_size,
                                          ByteBuffer per_fragment_stats_buffer, int per_fragment_stats_buffer_size,
                                          ByteBuffer udf_buffer,                int udf_buffer_size,
                                          ByteBuffer first_result_buffer,       int first_result_buffer_size,
                                          ByteBuffer final_result_buffer,       int final_result_buffer_size,
                                          ByteBuffer exception_buffer,          int exception_buffer_size);

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
    protected native int nativeUpdateCatalog(long pointer, long timestamp, boolean isStreamUpdate, byte diff_commands[]);

    /**
     * This method is called to initially load table data.
     *
     * @param pointer               the VoltDBEngine pointer
     * @param table_id              catalog ID of the table
     * @param serialized_table      the table data to be loaded
     * @param txnId                 ID of the transaction
     * @param spHandle              SP handle for this transaction
     * @param lastCommittedSpHandle Most recently committed SP Handled
     * @param uniqueId              Unique ID for the transaction
     * @param undoToken             token for undo quantum where changes should be logged.
     * @param callerId              ID of the caller who is invoking load table
     */
    protected native int nativeLoadTable(long pointer, int table_id, byte[] serialized_table, long txnId,
            long spHandle, long lastCommittedSpHandle, long uniqueId, long undoToken, byte callerId);

    /**
     * This method is called to initially load table data from a direct byte buffer
     *
     * @param pointer               the VoltDBEngine pointer
     * @param table_id              catalog ID of the table
     * @param serializedTable       the table data to be loaded
     * @param txnId                 ID of the transaction
     * @param spHandle              SP handle for this transaction
     * @param lastCommittedSpHandle Most recently committed SP Handled
     * @param uniqueId              Unique ID for the transaction
     * @param undoToken             token for undo quantum where changes should be logged.
     * @param callerId              ID of the caller who is invoking load table
     */
    protected native int nativeLoadTable(long pointer, int table_id, ByteBuffer serializedTable, long txnId,
            long spHandle, long lastCommittedSpHandle, long uniqueId, long undoToken, byte callerId);

    /**
     * Executes multiple plan fragments with the given parameter sets and gets the results.
     *
     * @param pointer         the VoltDBEngine pointer
     * @param planFragmentIds ID of the plan fragment to be executed.
     * @param inputDepIds     list of input dependency ids or null if no deps expected
     * @return error code
     */
    protected native int nativeExecutePlanFragments(
            long pointer,
            int batchIndex,
            int numFragments,
            long[] planFragmentIds,
            long[] inputDepIds,
            long txnId,
            long spHandle,
            long lastCommittedSpHandle,
            long uniqueId,
            long undoToken,
            boolean traceOn);

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
    protected native void nativeUpdateHashinator(long pointer,long configPtr, int tokenCount);

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
    protected native boolean nativeReleaseUndoToken(long pointer, long undoToken, boolean isEmptyDRTxn);

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
     * @param pointer          Pointer to an engine instance
     * @param tableId          ID of the table whose schema is being retrieved
     * @param schemaFilterType Type of filter to apply to schema
     * @param forceLive        Force the schema to be read from current catalog and not snapshot schemas
     * @return error code indicating status of execution
     */
    protected native int nativeGetSnapshotSchema(long pointer, int tableId, byte schemaFilterType, boolean forceLive);

    /**
     * Active a table stream of the specified type for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId Catalog ID of the table
     * @param streamType type of stream to activate
     * @param schemaFilterType ID for which schema filter should be used during this table stream
     * @param undoQuantumToken Undo quantum allowing destructive index clearing to be undone
     * @param data serialized predicates
     * @return <code>true</code> on success and <code>false</code> on failure
     */
    protected native boolean nativeActivateTableStream(long pointer, int tableId, int streamType, byte schemaFilterType,
            long undoQuantumToken, byte[] data);

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
     * Calculate a hash code for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId table to calculate a hash code for
     */
    protected native long nativeTableHashCode(long pointer, int tableId);

    protected native long nativeApplyBinaryLog(long pointer, long txnId, long spHandle, long lastCommittedSpHandle,
            long uniqueId, int remoteClusterId, long undoToken);

    /**
     * Execute an arbitrary task based on the task ID and serialized task parameters.
     * This is a generic entry point into the EE that doesn't need to be updated in the IPC
     * client every time you add a new task
     * @param pointer
     * @return error code
     */
    protected native int nativeExecuteTask(long pointer);

    /**
     * Perform an export poll or ack action. Poll data will be returned via the usual
     * results buffer. A single action may encompass both a poll and ack.
     * @param pointer Pointer to an engine instance
     * @param mAckOffset The offset being ACKd.
     * @param seqNo The current export sequence number
     * @param generationId The timestamp from the most-recently restored snapshot
     * @param mStreamName Name of the stream being acted against
     * @return
     */
    protected native void nativeSetExportStreamPositions(
            long pointer,
            long mAckOffset,
            long seqNo,
            long generationId,
            byte mStreamName[]);

    /**
     * Complete the deletion of the Migrated Table rows.
     * @param pointer Pointer to an engine instance
     * @param txnId The transactionId of the currently executing stored procedure
     * @param spHandle The spHandle of the currently executing stored procedure
     * @param uniqueId The uniqueId of the currently executing stored procedure
     * @param mTableName The name of the table that the deletes should be applied to.
     * @param deletableTxnId The transactionId of the last row that can be deleted
     * @param undoToken The token marking the rollback point for this transaction
     * @return true if more rows to be deleted
     */
    protected native boolean nativeDeleteMigratedRows(long pointer,
            long txnid, long spHandle, long uniqueId,
            byte mTableName[], long deletableTxnId, long undoToken);

    protected native void nativeSetViewsEnabled(long pointer, byte[] viewNamesAsBytes, boolean enabled);

    /**
     * @see ExecutionEngine#disableExternalStreams()
     */
    protected native void nativeDisableExternalStreams(long pointer);

    /**
     * @see ExecutionEngine#externalStreamsEnabled()
     */
    protected native boolean nativeExternalStreamsEnabled(long pointer);

    /**
     * Get the USO for an export table. This is primarily used for recovery.
     *
     * @param pointer Pointer to an engine instance
     * @param stream name of the stream we need state (USO + Seqno) from
     * @return The USO for the export table.
     */
    public native long[] nativeGetUSOForExportTable(long pointer, byte streamName[]);

    /**
     * This code only does anything useful on MACOSX.
     * On LINUX, procfs is read to get RSS
     * @return Returns the RSS size in bytes or -1 on error (or wrong platform).
     */
    public native static long nativeGetRSS();

    /**
     * Request a DR buffer payload with specified content, partition key value list and flag list should have the same length
     * @param drProtocolVersion the protocol version of desired DR buffer
     * @param partitionId producer partition ID
     * @param partitionKeyValues list of partition key value that specifies the desired partition key value of each txn
     * @param flags list of DRTxnPartitionHashFlags that specifies the desired type of each txn
     * @param startSequenceNumber the starting sequence number of DR buffers
     * @return payload bytes (only txns with no InvocationBuffer header)
     */
    public native static byte[] getTestDRBuffer(int drProtocolVersion, int partitionId,
                                                int partitionKeyValues[], int flags[],
                                                long startSequenceNumber);

    // --------------------------------------------------------
    // Topics methods
    // --------------------------------------------------------
    /**
     * Store a topics group in the topics system tables. The serialized group should be put in the m_psetBuffer
     *
     * @param pointer   to execution engine
     * @param undoToken to use to be able to rollback the store
     * @return 0 on succes otherwise 1
     */
    native static protected int nativeStoreTopicsGroup(long pointer, long undoToken);

    /**
     * Delete a topics group, all members and offsets
     *
     * @param pointer   to execution engine
     * @param undoToken to use to be able to rollback the delete
     * @param groupId   of the group to delete
     * @return 0 on success otherwise 1
     */
    native static protected int nativeDeleteTopicsGroup(long pointer, long undoToken, byte[] groupId);

    /**
     * Start or continue a fetch of all groups from this site. The response will be in m_nextDeserializer
     *
     * @param pointer      to execution engine
     * @param maxResponse  size for any response. If {@code <= 0} this is a continue of a fetch else a new fetch will be
     *                     started
     * @param startGroupId Non inclusive groupId from which to start fetching groups
     * @return 0 on success otherwise 1
     */
    native static protected int nativeFetchTopicsGroups(long pointer, int maxResponse, byte[] startGroupId);

    /**
     * Commit topic partition offsets for the topics group. The serialized offsets should be put in the m_psetBuffer
     * and the response will be in m_nextDeserializer
     *
     * @param pointer        to execution engine
     * @param spUniqueId     for this transaction
     * @param undoToken      to use to be able to rollback the commit
     * @param requestVersion of the topics request
     * @param groupId        of the group these offsets should be associated with
     * @return 0 on success otherwise 1
     */
    native static protected int nativeCommitTopicsGroupOffsets(long pointer, long spUniqueId, long undoToken,
            short requestVersion, byte[] groupId);

    /**
     * Fetch topic partition offsets for a group. The serialized offsets being requested should be put in the
     * m_psetBuffer and the response will be in m_nextDeserializer
     *
     * @param pointer        to execution engine
     * @param requestVersion of the topics request
     * @param groupId        of the group to fetch offsets from
     * @return 0 on success otherwise 1
     */
    native static protected int nativeFetchTopicsGroupOffsets(long pointer, short requestVersion, byte[] groupId);

    /**
     * Delete the expired offsets of standalone groups. An offset is expired if its commit timestamp is <
     * deleteOlderThan
     *
     * @param pointer         to execution engine
     * @param undoToken       to use to be able to rollback the delete
     * @param deleteOlderThan timestamp to use to select what offsets should be deleted
     * @return 0 on success otherwise 1
     */
    native static protected int nativeDeleteExpiredTopicsOffsets(long pointer, long undoToken, long deleteOlderThan);

    /**
     * Set the list of tables which can be replicated to from {@code clusterId}
     *
     * @param pointer   to execution engine
     * @param clusterId of producer cluster
     * @param tables    which match in both schemas and can be the target of replication
     */
    native static protected int nativeSetReplicableTables(long pointer, int clusterId, byte[][] tables);

    /**
     * Clear the map of replicable tables for all clusters
     *
     * @param pointer to execution engine
     */
    native static protected int nativeClearAllReplicableTables(long pointer);

    /**
     * Clear the map of replicable tables for a clusters
     *
     * @param pointer to execution engine
     */
    native static protected int nativeClearReplicableTables(long pointer, int clusterId);

    public static byte[] getTestDRBuffer(int partitionId,
                                         int partitionKeyValues[], int flags[],
                                         long startSequenceNumber) {
        return getTestDRBuffer(DRProtocol.PROTOCOL_VERSION, partitionId, partitionKeyValues, flags, startSequenceNumber);
    }

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

    /*
     * Execute a large block task synchronously.  Log errors if they occur.
     * Return true if successful and false otherwise.
     */
    protected boolean executeLargeBlockTaskSynchronously(LargeBlockTask task) {
        LargeBlockManager lbm = LargeBlockManager.getInstance();
        assert (lbm != null);

        LargeBlockResponse response = null;
        try {
            // The call to get() will block until the task completes.
            response = lbm.submitTask(task).get();
        }
        catch (RejectedExecutionException ree) {
            LOG.error("Could not queue large block task: " + ree.getMessage());
        }
        catch (ExecutionException ee) {
            LOG.error("Could not execute large block task: " + ee.getMessage());
        }
        catch (InterruptedException ie) {
            LOG.error("Large block task was interrupted: " + ie.getMessage());
        }

        if (response != null && !response.wasSuccessful()) {
            LOG.error("Large block task failed: " + response.getException().getMessage());
        }

        return response == null ? false : response.wasSuccessful();
    }

    /**
     * Useful in unit tests.  Allows one to supply a mocked logger
     * to verify that something was logged.
     *
     * @param vl  The new logger to install
     */
    @Deprecated
    public static void setVoltLoggerForTest(VoltLogger vl) {
        LOG = vl;
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

    /**
     * Useful in unit tests.  Gets the initial frequency with which
     * the long-running query info message will be logged.
     */
    @Deprecated
    public long getInitialLogDurationForTest() {
        return INITIAL_LOG_DURATION;
    }
}
