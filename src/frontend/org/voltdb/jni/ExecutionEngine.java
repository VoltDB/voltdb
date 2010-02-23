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

package org.voltdb.jni;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.ParameterSet;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.elt.ELTManager;
import org.voltdb.exceptions.EEException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * Wrapper for native Execution Engine library. There are two implementations,
 * one using JNI and one using IPC. ExecutionEngine provides a consistent interface
 * for these implementations to the ExecutionSite.
 */
public abstract class ExecutionEngine implements FastDeserializer.DeserializationMonitor {
    protected ExecutionSite site;

    // is the execution site dirty
    protected boolean m_dirty;

    /** Error codes exported for JNI methods. */
    public static final int ERRORCODE_SUCCESS = 0;
    public static final int ERRORCODE_ERROR = 1; // just error or not so far.
    public static final int ERRORCODE_WRONG_SERIALIZED_BYTES = 101;

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
        if (errorCode != ERRORCODE_SUCCESS) {
            throwExceptionForError(errorCode);
        }
    }

    /**
     * Utility method to generate an EEXception that can be overridden by
     * derived classes
     **/
    protected void throwExceptionForError(final int errorCode) {
        throw new EEException(errorCode);
    }

    /* Fast serialize interface implementation */
    @Override
    protected void finalize() throws Throwable {
        release();
    }

    @Override
    public void deserializedBytes(final int numBytes) {
    }

    /** Create an ee and load the volt shared library */
    public ExecutionEngine(final ExecutionSite site) {
        this.site = site;
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
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
    public void stashWorkUnitDependencies(final HashMap<Integer, List<VoltTable>> dependencies) {
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

        private final Logger hostLog =
            Logger.getLogger("HOST", VoltLoggerFactory.instance());

        private final Logger log =
            Logger.getLogger(ExecutionSite.class.getName(), VoltLoggerFactory.instance());


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
        void trackNewWorkUnit(final HashMap<Integer, List<VoltTable>> dependencies) {
            for (final Entry<Integer, List<VoltTable>> e : dependencies.entrySet()) {
                // could do this optionally - debug only.
                verifyDependencySanity(e.getKey(), e.getValue());
                // create a new list of references to the workunit's table
                // to avoid any changes to the WorkUnit's list. But do not
                // copy the table data.
                final ArrayDeque<VoltTable> deque = new ArrayDeque<VoltTable>();
                deque.addAll(e.getValue());
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
                VoltDB.crashVoltDB();
            }
            for (final Object dependency : dependencies) {
                if (dependency == null) {
                    hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyContainedNull.name(),
                                   new Object[] { dependencyId },
                            null);
                    VoltDB.crashVoltDB();
                }
                if (log.isTraceEnabled()) {
                    log.l7dlog(Level.TRACE, LogKeys.org_voltdb_ExecutionSite_ImportingDependency.name(),
                               new Object[] { dependencyId, dependency.getClass().getName(), dependency.toString() },
                               null);
                }
                if (!(dependency instanceof VoltTable)) {
                    hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyNotVoltTable.name(),
                                   new Object[] { dependencyId }, null);
                    VoltDB.crashVoltDB();
                }
            }

        }

    }



    /*
     * Interface backend invokes to communicate to Java frontend
     */


    /**
     * Called from ExecutionEngine to notify the Java Runtime that an EL buffer
     * is full of tuples and ready to be sent to the companion OLAP system. It is
     * assumed the buffer was originally requested through a call to
     * {@link #claimManagedBuffer(int)}. Do not call {@link #releaseManagedBuffer(long)}
     * for this buffer after calling this method; it will be cleaned up by
     * the Java Runtime.
     *
     * @param bufferPtr The pointer to the buffer memory, cast as a long.
     * @param bytesUsed The number of bytes in the buffer to be sent.
     * @param tableId The catalog id of the table which sources these tuples.
     */
    public void handoffReadyELBuffer(final long bufferPtr, final int bytesUsed, final int tableId) {
        ELTManager.instance().handoffToConnection(bufferPtr, bytesUsed, tableId);
    }

    /**
     * This is called from VoltDBEngine to request a new buffer from the Java Runtime.
     *
     * @param desiredSizeInBytes The minimum size for the buffer requested in bytes.
     * @return The pointer to the buffer memory cast as a long value or zero on failure.
     */
    public long claimManagedBuffer(final int desiredSizeInBytes) {
        final long claimedBuffer = ELTManager.instance().claimBufferForEE(desiredSizeInBytes);
        assert claimedBuffer != 0;
        return claimedBuffer;
    }

    /**
     * This is called from VoltDBEngine to release a buffer that was originally requested
     * through a call to {@link #claimManagedBuffer(int)}.
     *
     * @param bufferPtr Pointer to the buffer to be released cast as a long.
     */
    public void releaseManagedBuffer(final long bufferPtr) {
        ELTManager.instance().releaseManagedBuffer(bufferPtr);
    }

    /**
     * Called from the ExecutionEngine to request serialized dependencies.
     */
    public byte[] nextDependencyAsBytes(final int dependencyId) {
        final VoltTable vt =  m_dependencyTracker.nextDependency(dependencyId);
        if (vt != null) {
            final byte[]  bytes = vt.getTableDataReference().array();
            return bytes;
        }
        else {
            return null;
        }
    }

    /*
     * Interface frontend invokes to communicate to CPP execution engine.
     */

    /**
     *
     */
    abstract public boolean activateCopyOnWrite(final int tableId);

    /**
     * Serialize more tuples from the specified table that is already in COW mode
     * @param c Buffer to serialize tuple data too
     * @param tableId Catalog ID of the table to serialize
     * @return A positive number indicating the number of bytes serialized or 0 if there is no more data.
     *        -1 is returned if there is an error (such as the table not being COW mode).
     */
    public abstract int cowSerializeMore(BBContainer c, int tableId);

    /** Releases the Engine object. */
    abstract public void release() throws EEException, InterruptedException;

    /** Pass the catalog to the engine */
    abstract public void loadCatalog(final String serializedCatalog) throws EEException;

    /** Run a plan fragment */
    abstract public DependencyPair executePlanFragment(
        long planFragmentId, int outputDepId,
        int inputDepId, ParameterSet parameterSet,
        long txnId, long lastCommittedTxnId, long undoQuantumToken)
      throws EEException;

    /** Run a plan fragment */
    abstract public VoltTable executeCustomPlanFragment(
            String plan, int outputDepId,
            int inputDepId, long txnId,
            long lastCommittedTxnId, long undoQuantumToken) throws EEException;

    /** Run multiple query plan fragments */
    abstract public VoltTable[] executeQueryPlanFragmentsAndGetResults(long[] planFragmentIds,
                                                                       int numFragmentIds,
                                                                       ParameterSet[] parameterSets,
                                                                       int numParameterSets,
                                                                       long txnId, long lastCommittedTxnId,
                                                                       long undoQuantumToken) throws EEException;

    /** Used for test code only (AFAIK jhugg) */
    abstract public VoltTable serializeTable(int tableId) throws EEException;

    abstract public void loadTable(
        int tableId, VoltTable table, long txnId,
        long lastCommittedTxnId, long undoToken, boolean allowELT) throws EEException;

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
    abstract public void tick(long time, long lastCommittedTxnId);

    /**
     * Instruct EE to come to an idle state. Flush ELT buffers, finish
     * any in-progress checkpoint, etc.
     */
    abstract public void quiesce(long lastCommittedTxnId);

    /**
     * Retrieve a set of statistics using the specified selector from the StatisticsSelector enum.
     * @param selector Selector from StatisticsSelector specifying what statistics to retrieve
     * @param locators CatalogIds specifying what set of items the stats should come from.
     *  @return Array of results tables. An array of length 0 indicates there are no results. null indicates failure.
     */
    abstract public VoltTable[] getStats(SysProcSelector selector, int locators[]);

    /**
     * Wrapper for {@link #nativeToggleProfiler(long, int)}.
     */
    public abstract int toggleProfiler(int toggle);

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
    protected native long nativeCreate();
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
     * @param node_id this id will be set to the execution engine
     * @return error code
     */
    protected native int nativeInitialize(long pointer, int clusterIndex, int siteId);

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
     * @param serialized_catalog the root catalog object serialized as text strings.
     * this parameter is jstring, not jbytearray because Catalog is serialized into
     * human-readable text strings separated by line feeds.
     * @return error code
     */
    protected native int nativeLoadCatalog(long pointer, String serialized_catalog);

    /**
     * This method is called to initially load table data.
     * @param pointer the VoltDBEngine pointer
     * @param table_id catalog ID of the table
     * @param serialized_table the table data to be loaded
     * @param Length of the serialized table
     * @param undoToken token for undo quantum where changes should be logged.
     */
    protected native int nativeLoadTable(long pointer, int table_id, byte[] serialized_table,
            long txnId, long lastCommittedTxnId, long undoToken, boolean allowELT);

    //Execution

    /**
     * Executes a plan fragment with the given parameter set.
     * @param pointer the VoltDBEngine pointer
     * @param plan_fragment_id ID of the plan fragment to be executed.
     * @return error code
     */
    protected native int nativeExecutePlanFragment(long pointer, long planFragmentId,
            int outputDepId, int inputDepId, long txnId, long lastCommittedTxnId, long undoToken);

    protected native int nativeExecuteCustomPlanFragment(long pointer, String plan,
            int outputDepId, int inputDepId, long txnId, long lastCommittedTxnId, long undoToken);

    /**
     * Executes multiple plan fragments with the given parameter sets and gets the results.
     * @param pointer the VoltDBEngine pointer
     * @param planFragmentIds ID of the plan fragment to be executed.
     * @return error code
     */
    protected native int nativeExecuteQueryPlanFragmentsAndGetResults(long pointer,
            long[] planFragmentIds, int numFragments, long txnId, long lastCommittedTxnId, long undoToken);

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
    protected native void nativeTick(long pointer, long time, long lastCommittedTxnId);

    /**
     * Native implementation of quiesce engine interface method.
     * @param pointer
     */
    protected native void nativeQuiesce(long pointer, long lastCommittedTxnId);

    /**
     * Retrieve a set of statistics using the specified selector ordinal from the StatisticsSelector enum.
     * @param stat_selector Ordinal value of a statistic selector from StatisticsSelector.
     * @param catalog_locators CatalogIds specifying what set of items the stats should come from.
     * @return Number of result tables, 0 on no results, -1 on failure.
     */
    protected native int nativeGetStats(long pointer, int stat_selector, int catalog_locators[]);

    /**
     * Toggle profile gathering within the execution engine
     * @param mode 0 to disable. 1 to enable.
     * @return 0 on success.
     */
    protected native int nativeToggleProfiler(long pointer, int mode);

    /**
     * Given a long value, pick a partition to store the data.
     * This is for test code only. Identical functionality exists in Java in
     * TheHashinator.hashinate(..).
     *
     * @param value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    public native int hashinate(long value, int partitionCount);

    /**
     * Given a String value, pick a partition to store the data.
     * This is for test code only. Identical functionality exists in Java in
     * TheHashinator.hashinate(..).
     *
     * @param string value The value to hash.
     * @param partitionCount The number of partitions to choose from.
     * @return A value between 0 and partitionCount-1, hopefully pretty evenly
     * distributed.
     */
    public native int hashinate(String string, int partitionCount);

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
     * Active copy on write mode for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId Catalog ID of the table
     * @return <code>true</code> on success and <code>false</code> on failure
     */
    protected native boolean nativeActivateCopyOnWrite(long pointer, int tableId);

    /**
     * Serialize more tuples from the specified table that is already in COW mode
     * @param pointer Pointer to an engine instance
     * @param bufferPointer Buffer to serialize data to
     * @param offset Offset into the buffer to start serializing to
     * @param length length of the buffer
     * @param tableId Catalog ID of the table to serialize
     * @return A positive number indicating the number of bytes serialized or 0 if there is no more data.
     *         -1 is returned if there is an error (such as the table not being COW mode).
     */
    protected native int nativeCOWSerializeMore(long pointer, long bufferPointer, int offset, int length, int tableId);
}
