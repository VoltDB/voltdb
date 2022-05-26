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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.List;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.UserDefinedAggregateFunctionRunner;
import org.voltdb.UserDefinedScalarFunctionRunner;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.largequery.BlockId;
import org.voltdb.largequery.LargeBlockTask;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.sysprocs.saverestore.HiddenColumnFilter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.SerializationHelper;


/**
 * Wrapper for native Execution Engine library.
 * All native methods are private to make it simple
 * and keep Java/C++ separated as much as possible.
 * These native methods basically just receive IDs in primitive types
 * to simplify JNI calls. This is why these native methods are private and
 * we have public methods that receive objects for type-safety.
 * For each methods, see comments in hstorejni.cpp.
 * Read <a href="package-summary.html">com.horinzontica.jni</a> for
 * guidelines to add/modify JNI methods.
 */
public class ExecutionEngineJNI extends ExecutionEngine {

    /*
     * Threshold of fullness where the EE will start compacting a table's blocks together
     * to free memory and return to the OS. Block will always be freed if they are emptied
     * and since rows are fixed size for a table they are always available for reuse.
     *
     * Valid values are 0-99, where 0 disables compaction completely and 99 compacts the table
     * if it is even 1% empty.
     */
    public static final int EE_COMPACTION_THRESHOLD;

    private static final boolean HOST_TRACE_ENABLED;

    // Size of the parameter set buffer and the per-fragment stats buffer.
    // 256K is a reasonable size for those relatively small buffers.
    private static final int smallBufferSize = 256 * 1024;

    static {
        EE_COMPACTION_THRESHOLD = Integer.getInteger("EE_COMPACTION_THRESHOLD", 95);
        if (EE_COMPACTION_THRESHOLD < 0 || EE_COMPACTION_THRESHOLD > 99) {
            VoltDB.crashLocalVoltDB("EE_COMPACTION_THRESHOLD " +
                    EE_COMPACTION_THRESHOLD + " is not valid, must be between 0 and 99", false, null);
        }
        HOST_TRACE_ENABLED = LOG.isTraceEnabled();
    }


    /** The HStoreEngine pointer. */
    private long pointer;

    /** Create a ByteBuffer (in a container) for serializing arguments to C++. Use a direct
    ByteBuffer as it will be passed directly to the C++ code. */
    // This matches MAX_UDF_BUFFER_SIZE in VoltDBEngine.h
    // It does not limit the maximum size of the UDF buffer / parameter set buffer we can allocate,
    // this is the maximum size of the buffer that we can persist without shrinking it at appropriate time.
    private static final int MAX_BUFFER_SIZE = 50 * 1024 * 1024; // 50MB
    private BBContainer m_psetBufferC = null;
    private ByteBuffer m_psetBuffer = null;

    /** Create a ByteBuffer (in a container) for the C++ side to share time measurements and
        the success / fail status for fragments in a batch. */
    private BBContainer m_perFragmentStatsBufferC = null;
    private ByteBuffer m_perFragmentStatsBuffer = null;

    // This a shared buffer for UDFs. The top end and the EE use this buffer to exchange the
    // function parameters and the return value.
    private BBContainer m_udfBufferC = null;
    private ByteBuffer m_udfBuffer = null;

    /**
     * A deserializer backed by a direct byte buffer, for fast access from C++.
     * Since this is generally the largest shared buffer between Java and C++
     * it is also generally speaking (since there are no larger shared buffers)
     * the largest possible message size that can be sent between the IPC backend
     * and Java. Every time the size of this buffer is changed the MAX_MSG_SZ define
     * in voltdbipc.cpp must be changed to match so that tests and apps
     * that rely on being able to serialize large results sets will get the same amount of storage
     * when using the IPC backend.
     **/
    private final BBContainer m_firstDeserializerBufferOrigin = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private FastDeserializer m_firstDeserializer =
            new FastDeserializer(m_firstDeserializerBufferOrigin.b());
    private final BBContainer m_nextDeserializerBufferOrigin = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private FastDeserializer m_nextDeserializer =
            new FastDeserializer(m_nextDeserializerBufferOrigin.b());

    private final BBContainer m_emptyDeserializerBuffer = org.voltcore.utils.DBBPool.allocateDirect(0);
    private FastDeserializer m_emptyDeserializer = new FastDeserializer(m_emptyDeserializerBuffer.b());

    /*
     * For large result sets the EE will allocate new memory for the results
     * and invoke a callback to set the allocated memory here. This buffer will
     * be deallocated by EE as well.
     */
    private ByteBuffer m_fallbackBuffer = null;

    private final BBContainer m_exceptionBufferOrigin = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 5);
    private ByteBuffer m_exceptionBuffer = m_exceptionBufferOrigin.b();

    /**
     * initialize the native Engine object.
     */
    public ExecutionEngineJNI(
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int sitesPerHost,
            final int hostId,
            final String hostname,
            final int drClusterId,
            final int defaultDrBufferSize,
            final boolean drIgnoreConflicts,
            final int drCrcErrorIgnoreMax,
            final boolean drCrcErrorIgnoreFatal,
            final int tempTableMemory,
            final HashinatorConfig hashinatorConfig,
            final boolean isLowestSiteId)
    {
        // base class loads the volt shared library.
        super(siteId, partitionId);

        //exceptionBuffer.order(ByteOrder.nativeOrder());
        LOG.trace("Creating Execution Engine on clusterIndex=" + clusterIndex
                + ", site_id = " + siteId + "...");
        /*
         * (Ning): The reason I'm testing if we're running in Sun's JVM is that
         * EE needs this info in order to decide whether it's safe to install
         * the signal handler or not.
         */
        pointer = nativeCreate(System.getProperty("java.vm.vendor")
                               .toLowerCase().contains("sun microsystems"));

        nativeSetLogLevels(pointer, EELoggers.getLogLevels());
        int errorCode =
            nativeInitialize(
                    pointer,
                    clusterIndex,
                    siteId,
                    partitionId,
                    sitesPerHost,
                    hostId,
                    getStringBytes(hostname),
                    drClusterId,
                    defaultDrBufferSize,
                    drIgnoreConflicts,
                    drCrcErrorIgnoreMax,
                    drCrcErrorIgnoreFatal,
                    tempTableMemory * 1024 * 1024,
                    isLowestSiteId,
                    EE_COMPACTION_THRESHOLD);
        checkErrorCode(errorCode);

        setupPsetBuffer(smallBufferSize);
        setupPerFragmentStatsBuffer(smallBufferSize);
        setupUDFBuffer(smallBufferSize);
        updateEEBufferPointers();

        updateHashinator(hashinatorConfig);
        //LOG.info("Initialized Execution Engine");
    }

    final void updateEEBufferPointers() {
        int errorCode = nativeSetBuffers(pointer,
                m_psetBuffer,                 m_psetBuffer.capacity(),
                m_perFragmentStatsBuffer,     m_perFragmentStatsBuffer.capacity(),
                m_udfBuffer,                  m_udfBuffer.capacity(),
                m_firstDeserializer.buffer(), m_firstDeserializer.buffer().capacity(),
                m_nextDeserializer.buffer(),  m_nextDeserializer.buffer().capacity(),
                m_exceptionBuffer,            m_exceptionBuffer.capacity());
        checkErrorCode(errorCode);
    }

    final void setupPsetBuffer(int size) {
        if (m_psetBuffer != null) {
            m_psetBufferC.discard();
            m_psetBuffer = null;
        }

        m_psetBufferC = DBBPool.allocateDirect(size);
        m_psetBuffer = m_psetBufferC.b();
    }

    final void setupPerFragmentStatsBuffer(int size) {
        if (m_perFragmentStatsBuffer != null) {
            m_perFragmentStatsBufferC.discard();
            m_perFragmentStatsBuffer = null;
        }

        m_perFragmentStatsBufferC = DBBPool.allocateDirect(size);
        m_perFragmentStatsBuffer = m_perFragmentStatsBufferC.b();
    }

    final void setupUDFBuffer(int size) {
        if (m_udfBuffer != null) {
            m_udfBufferC.discard();
            m_udfBuffer = null;
        }

        try {
            m_udfBufferC = DBBPool.allocateDirect(size);
            m_udfBuffer = m_udfBufferC.b();
        }
        catch (OutOfMemoryError e) {
            // If the allocation failed, we will just fail the current SQL statement,
            // the server will not crash and can continue to execute the following requests.
            // In this case, we cannot leave the buffer as NULL, reset it to the default size.
            setupUDFBuffer(smallBufferSize);
            updateEEBufferPointers();
            // But the exception still needs to be thrown out so that the current SQL statement can fail.
            throw e;
        }
    }

    final void clearPsetAndEnsureCapacity(int size) {
        assert(m_psetBuffer != null);
        if (size > m_psetBuffer.capacity()) {
            setupPsetBuffer(size);
            updateEEBufferPointers();
        }
        else if (m_psetBuffer.capacity() > MAX_BUFFER_SIZE && size < MAX_BUFFER_SIZE) {
            // The last request was a batch that was greater than max network buffer size,
            // so let's not hang on to all that memory
            setupPsetBuffer(MAX_BUFFER_SIZE);
            updateEEBufferPointers();
        }
        else {
            m_psetBuffer.clear();
        }
    }

    final void clearPerFragmentStatsAndEnsureCapacity(int batchSize) {
        assert(m_perFragmentStatsBuffer != null);
        // Determine the required size of the per-fragment stats buffer:
        // int8_t perFragmentTimingEnabled
        // int32_t succeededFragmentsCount
        // succeededFragmentsCount * sizeof(int64_t) for duration time numbers.
        int size = 1 + 4 + batchSize * 8;
        if (size > m_perFragmentStatsBuffer.capacity()) {
            setupPerFragmentStatsBuffer(size);
            updateEEBufferPointers();
        }
        else {
            m_perFragmentStatsBuffer.clear();
        }
    }

    /** Utility method to throw a Runtime exception based on the error code and serialized exception **/
    @Override
    final protected void throwExceptionForError(final int errorCode) throws RuntimeException {
        throw getExceptionFromError(errorCode);
    }

    private SerializableException getExceptionFromError(final int errorCode) {
        m_exceptionBuffer.clear();
        final int exceptionLength = m_exceptionBuffer.getInt();

        if (exceptionLength == 0) {
            return new EEException(errorCode);
        } else {
            m_exceptionBuffer.position(0);
            m_exceptionBuffer.limit(4 + exceptionLength);
            return SerializableException.deserializeFromBuffer(m_exceptionBuffer);
        }
    }

    /**
     * Releases the Engine object.
     * This method is automatically called from #finalize(), but
     * it's recommended to call this method just after you finish
     * using the object.
     */
    @Override
    public void release() throws EEException {
        LOG.trace("Releasing Execution Engine... " + pointer);
        if (pointer != 0L) {
            final int errorCode = nativeDestroy(pointer);
            pointer = 0L;
            checkErrorCode(errorCode);
        }
        m_firstDeserializer = null;
        m_firstDeserializerBufferOrigin.discard();
        m_nextDeserializer = null;
        m_nextDeserializerBufferOrigin.discard();
        m_exceptionBuffer = null;
        m_exceptionBufferOrigin.discard();
        m_emptyDeserializer = null;
        m_emptyDeserializerBuffer.discard();
        m_psetBufferC.discard();
        m_psetBuffer = null;
        m_perFragmentStatsBufferC.discard();
        m_perFragmentStatsBuffer = null;
        m_udfBufferC.discard();
        m_udfBuffer = null;
        LOG.trace("Released Execution Engine.");
    }

    /**
     * Reset the Engine object.
     */
    @Override
    public void decommission(boolean remove, boolean promote, int newSitePerHost) throws EEException {
        LOG.trace("Decommissioning Execution Engine... " + pointer);
        if (pointer != 0L) {
            final int errorCode = nativeDecommission(pointer,remove,promote,newSitePerHost);
            checkErrorCode(errorCode);
        }
        // Don't need reset the buffers, they can be reused after recommission
        LOG.trace("Decommissioned Execution Engine.");
    }

    /**
     *  Provide a serialized catalog and initialize version 0 of the engine's
     *  catalog.
     */
    @Override
    protected void coreLoadCatalog(long timestamp, final byte[] catalogBytes) throws EEException {
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        errorCode = nativeLoadCatalog(pointer, timestamp, catalogBytes);
        checkErrorCode(errorCode);
        //LOG.info("Loaded Catalog.");
    }

    /**
     * Provide a catalog diff and a new catalog version and update the
     * engine's catalog.
     */
    @Override
    public void coreUpdateCatalog(long timestamp, boolean isStreamUpdate, final String catalogDiffs) throws EEException {
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        errorCode = nativeUpdateCatalog(pointer, timestamp, isStreamUpdate, getStringBytes(catalogDiffs));
        checkErrorCode(errorCode);
    }

    // Tell EE that we need the time measurements for the next fragment.
    // The timing is off by default.
    @Override
    public void setPerFragmentTimingEnabled(boolean enabled) {
        m_perFragmentStatsBuffer.clear();
        m_perFragmentStatsBuffer.put((byte)(enabled ? 1 : 0));
    }

    // Extract the per-fragment stats from the buffer.
    @Override
    public int extractPerFragmentStats(int batchSize, long[] executionTimesOut) {
        m_perFragmentStatsBuffer.clear();
        // Discard the first byte since it is the timing on/off switch.
        m_perFragmentStatsBuffer.get();
        int succeededFragmentsCount = m_perFragmentStatsBuffer.getInt();
        if (executionTimesOut != null) {
            assert(executionTimesOut.length >= succeededFragmentsCount);
            for (int i = 0; i < succeededFragmentsCount; i++) {
                executionTimesOut[i] = m_perFragmentStatsBuffer.getLong();
            }
            // This is the time for the failed fragment.
            if (succeededFragmentsCount < executionTimesOut.length) {
                executionTimesOut[succeededFragmentsCount] = m_perFragmentStatsBuffer.getLong();
            }
        }
        return succeededFragmentsCount;
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     * @param traceOn
     */
    @Override
    public FastDeserializer coreExecutePlanFragments(
            final int batchIndex, final int numFragmentIds, final long[] planFragmentIds, final long[] inputDepIds,
            final Object[] parameterSets, DeterminismHash determinismHash, boolean[] isWriteFrags, int[] sqlCRCs,
            final long txnId, final long spHandle, final long lastCommittedSpHandle, long uniqueId,
            final long undoToken, final boolean traceOn) throws EEException {
        // plan frag zero is invalid
        assert((numFragmentIds == 0) || (planFragmentIds[0] != 0));

        if (numFragmentIds == 0) {
            return m_emptyDeserializer;
        }
        final int batchSize = numFragmentIds;
        if (HOST_TRACE_ENABLED) {
            for (int i = 0; i < batchSize; ++i) {
                LOG.trace("Batch Executing planfragment:" + planFragmentIds[i] + ", params=" + parameterSets[i].toString());
            }
        }

        // serialize the param sets
        int allPsetSize = 0;
        for (int i = 0; i < batchSize; ++i) {
            if (parameterSets[i] instanceof ByteBuffer) {
                allPsetSize += ((ByteBuffer) parameterSets[i]).limit();
            }
            else {
                allPsetSize += ((ParameterSet) parameterSets[i]).getSerializedSize();
            }
        }

        clearPsetAndEnsureCapacity(allPsetSize);
        for (int i = 0; i < batchSize; ++i) {
            int paramStart = m_psetBuffer.position();
            Object param = parameterSets[i];
            if (param instanceof ByteBuffer) {
                ByteBuffer buf = (ByteBuffer) param;
                m_psetBuffer.put(buf);
            } else {
                ParameterSet pset = (ParameterSet) param;
                try {
                    pset.flattenToBuffer(m_psetBuffer);
                } catch (final Exception exception) { //Not Just IO but bad params can throw RuntimeExceptions
                    throw new RuntimeException("Error serializing parameters for SQL batch element: " +
                                               i + " with plan fragment ID: " + planFragmentIds[i] +
                                               " and with params: " +
                                               pset.toJSONString(), exception);
                }
            }
            // determinismHash can be null in FragmentTask.processFragmentTask() and many tests
            if (determinismHash != null && isWriteFrags[i]){
                determinismHash.offerStatement(sqlCRCs[i], paramStart, m_psetBuffer);
            }
        }
        // checkMaxFsSize();
        clearPerFragmentStatsAndEnsureCapacity(batchSize);

        // Execute the plan, passing a raw pointer to the byte buffers for input and output
        //Clear is destructive, do it before the native call
        FastDeserializer targetDeserializer = (batchIndex == 0) ? m_firstDeserializer : m_nextDeserializer;
        targetDeserializer.clear();

        final int errorCode = nativeExecutePlanFragments(
                pointer, batchIndex, numFragmentIds, planFragmentIds, inputDepIds, txnId, spHandle,
                lastCommittedSpHandle, uniqueId, undoToken, traceOn);
        try {
            checkErrorCode(errorCode);
            m_usingFallbackBuffer = m_fallbackBuffer != null;
            FastDeserializer fds = m_usingFallbackBuffer ? new FastDeserializer(m_fallbackBuffer) : targetDeserializer;
            assert(fds != null);
            try {
                // check if anything was changed
                m_dirty |= fds.readBoolean();
            } catch (final IOException ex) {
                LOG.error("Failed to deserialize result table" + ex);
                throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
            }

            return fds;
        } finally {
            m_fallbackBuffer = null;
        }
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        if (HOST_TRACE_ENABLED) {
            LOG.trace("Retrieving VoltTable:" + tableId);
        }
        // Clear is destructive, do it before the native call
        m_nextDeserializer.clear();
        final int errorCode = nativeSerializeTable(pointer, tableId, m_nextDeserializer.buffer(),
                m_nextDeserializer.buffer().capacity());
        checkErrorCode(errorCode);

        return PrivateVoltTableFactory.createVoltTableFromSharedBuffer(m_nextDeserializer.buffer());
    }

    @Override
    public byte[] loadTable(final int tableId, final VoltTable table, final long txnId,
        final long spHandle,
        final long lastCommittedSpHandle,
        final long uniqueId,
        long undoToken,
        LoadTableCaller caller) throws EEException
    {
        if (HOST_TRACE_ENABLED) {
            LOG.trace("loading table id=" + tableId + "...");
        }

        ByteBuffer serializedTable = PrivateVoltTableFactory.getTableDataReference(table);
        if (HOST_TRACE_ENABLED) {
            LOG.trace("passing " + serializedTable.capacity() + " bytes to EE...");
        }

        //Clear is destructive, do it before the native call
        m_nextDeserializer.clear();
        final int errorCode;
        if (serializedTable.hasArray()) {
            errorCode = nativeLoadTable(pointer, tableId, serializedTable.array(), txnId, spHandle,
                    lastCommittedSpHandle, uniqueId, undoToken, caller.getId());
        } else {
            assert serializedTable.isDirect();
            errorCode = nativeLoadTable(pointer, tableId, serializedTable, txnId, spHandle, lastCommittedSpHandle,
                    uniqueId, undoToken, caller.getId());
        }
        checkErrorCode(errorCode);

        try {
            int length = m_nextDeserializer.readInt();
            if (length == 0) {
                return null;
            }
            if (length < 0) {
                VoltDB.crashLocalVoltDB("Length shouldn't be < 0", true, null);
            }

            byte uniqueViolations[] = new byte[length];
            m_nextDeserializer.readFully(uniqueViolations);

            return uniqueViolations;
        } catch (final IOException ex) {
            LOG.error("Failed to retrieve unique violations: " + tableId, ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    @Override
    public void tick(final long time, final long lastCommittedTxnId) {
        nativeTick(pointer, time, lastCommittedTxnId);
    }

    @Override
    public void quiesce(long lastCommittedTxnId) {
        nativeQuiesce(pointer, lastCommittedTxnId);
    }

    /**
     * Retrieve a set of statistics using the specified selector from the StatisticsSelector enum.
     * @param selector Selector from StatisticsSelector specifying what statistics to retrieve
     * @param locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Array of results tables. An array of length 0 indicates there are no results. On error, an EEException will be thrown.
     */
    @Override
    public VoltTable[] getStats(
            final StatsSelector selector,
            final int locators[],
            final boolean interval,
            final Long now)
    {
        //Clear is destructive, do it before the native call
        m_nextDeserializer.clear();
        final int numResults = nativeGetStats(pointer, selector.ordinal(), locators, interval, now);
        if (numResults == -1) {
            throwExceptionForError(ERRORCODE_ERROR);
        }

        try {
            m_nextDeserializer.readInt();//Ignore the length of the result tables
            final VoltTable results[] = new VoltTable[numResults];
            for (int ii = 0; ii < numResults; ii++) {
                int len = m_nextDeserializer.readInt();
                byte[] bufCopy = new byte[len];
                m_nextDeserializer.readFully(bufCopy, 0, len);
                // This Table should be readonly (true), but table stats need to be updated
                // Stream stats until Stream stats are deprecated from Table stats
                results[ii] = PrivateVoltTableFactory.createVoltTableFromBuffer(ByteBuffer.wrap(bufCopy), false);
            }
            return results;
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for getStats" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public void toggleProfiler(final int toggle) {
        nativeToggleProfiler(pointer, toggle);
    }

    @Override
    public boolean releaseUndoToken(final long undoToken, boolean isEmptyDRTxn) {
        return nativeReleaseUndoToken(pointer, undoToken, isEmptyDRTxn);
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        return nativeUndoUndoToken(pointer, undoToken);
    }

    /**
     * Set the log levels to be used when logging in this engine
     * @param logLevels Levels to set
     * @throws EEException
     * @returns true on success false on failure
     */
    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        return nativeSetLogLevels(pointer, logLevels);
    }

    @Override
    public Pair<byte[], Integer> getSnapshotSchema(int tableId, HiddenColumnFilter hiddenColumnFilter,
            boolean forceLive)
            throws EEException {
        m_nextDeserializer.clear();
        checkErrorCode(nativeGetSnapshotSchema(pointer, tableId, hiddenColumnFilter.getId(), forceLive));
        try {
            return Pair.of(m_nextDeserializer.readVarbinary(), m_nextDeserializer.readInt());
        } catch (IOException e) {
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType streamType,
                                       HiddenColumnFilter hiddenColumnFilter,
                                       long undoQuantumToken,
                                       byte[] predicates) {
        return nativeActivateTableStream(pointer, tableId, streamType.ordinal(), hiddenColumnFilter.getId(),
                                         undoQuantumToken, predicates);
    }

    @Override
    public Pair<Long, int[]> tableStreamSerializeMore(int tableId,
                                                      TableStreamType streamType,
                                                      List<BBContainer> outputBuffers) {
        //Clear is destructive, do it before the native call
        m_nextDeserializer.clear();
        byte[] bytes = outputBuffers != null
                            ? SnapshotUtil.OutputBuffersToBytes(outputBuffers)
                            : null;
        long remaining = nativeTableStreamSerializeMore(pointer,
                                                        tableId,
                                                        streamType.ordinal(),
                                                        bytes);
        int[] positions = null;
        assert(m_nextDeserializer != null);
        int count;
        try {
            count = m_nextDeserializer.readInt();
            if (count > 0) {
                positions = new int[count];
                for (int i = 0; i < count; i++) {
                    positions[i] = m_nextDeserializer.readInt();
                }
                return Pair.of(remaining, positions);
            }
        } catch (final IOException ex) {
            LOG.error("Failed to deserialize position array" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }

        return Pair.of(remaining, new int[] {0});
    }

    /**
     * Instruct the EE to execute an Export poll and/or ack action. Poll response
     * data is returned in the usual results buffer, length preceded as usual.
     */
    @Override
    public void setExportStreamPositions(ExportSnapshotTuple sequences, int partitionId, String streamName)
    {
        if (EXPORT_LOG.isDebugEnabled()) {
            EXPORT_LOG.debug("exportAction on partition " + partitionId + ", uso: "
                    +
                    sequences.getAckOffset() + ", seqNo: " + sequences.getSequenceNumber() +
                    ", generationId:" + sequences.getGenerationId() + ", streamName: " + streamName);
        }
        //Clear is destructive, do it before the native call
        m_nextDeserializer.clear();
        nativeSetExportStreamPositions(pointer,
                                       sequences.getAckOffset(),
                                       sequences.getSequenceNumber(),
                                       sequences.getGenerationId(),
                                       getStringBytes(streamName));
    }

    @Override
    public boolean deleteMigratedRows(long txnid, long spHandle, long uniqueId,
            String tableName, long deletableTxnId, long undoToken) {
        m_nextDeserializer.clear();
        boolean txnFullyDeleted = nativeDeleteMigratedRows(pointer, txnid, spHandle, uniqueId,
                getStringBytes(tableName), deletableTxnId, undoToken);
        return txnFullyDeleted;
    }

    @Override
    public long[] getUSOForExportTable(String streamName) {
        return nativeGetUSOForExportTable(pointer, getStringBytes(streamName));
    }

    @Override
    public long tableHashCode(int tableId) {
        return nativeTableHashCode(pointer, tableId);
    }

    @Override
    public int hashinate(
            Object value,
            HashinatorConfig config)
    {
        ParameterSet parameterSet = ParameterSet.fromArrayNoCopy(value, config.configBytes);

        // serialize the param set
        clearPsetAndEnsureCapacity(parameterSet.getSerializedSize());
        try {
            parameterSet.flattenToBuffer(m_psetBuffer);
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }

        return nativeHashinate(pointer, config.configPtr, config.numTokens);
    }

    @Override
    public void updateHashinator(HashinatorConfig config)
    {
        if (config.configPtr == 0) {
            ParameterSet parameterSet = ParameterSet.fromArrayNoCopy(config.configBytes);

            // serialize the param set
            clearPsetAndEnsureCapacity(parameterSet.getSerializedSize());
            try {
                parameterSet.flattenToBuffer(m_psetBuffer);
            } catch (final IOException exception) {
                throw new RuntimeException(exception); // can't happen
            }
        }

        nativeUpdateHashinator(pointer, config.configPtr, config.numTokens);
    }

    @Override
    public long applyBinaryLog(ByteBuffer logs, long txnId, long spHandle, long lastCommittedSpHandle,
            long uniqueId, int remoteClusterId, long undoToken) throws EEException {
        long rowCount = nativeApplyBinaryLog(pointer, txnId, spHandle, lastCommittedSpHandle, uniqueId, remoteClusterId,
                undoToken);
        if (rowCount < 0) {
            throw getExceptionFromError((int) rowCount);
        }
        return rowCount;
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        return nativeGetThreadLocalPoolAllocations();
    }

    /*
     * Instead of using the reusable output buffer to get results for the next batch,
     * use this buffer allocated by the EE. This is for one time use and is deallocated by EE.
     */
    public void fallbackToEEAllocatedBuffer(ByteBuffer buffer) {
        assert(buffer != null);
        assert(m_fallbackBuffer == null);
        m_fallbackBuffer = buffer;
    }

    public void resizeUDFBuffer(int size) {
        // Read the size which we want to change to.
        setupUDFBuffer(size);
        updateEEBufferPointers();
    }

    public int callJavaUserDefinedFunction() {
        m_udfBuffer.clear();
        m_udfBuffer.getInt(); // skip the buffer size integer, it is only used by VoltDB IPC.
        int functionId = m_udfBuffer.getInt();
        UserDefinedScalarFunctionRunner udfRunner = m_functionManager.getFunctionRunnerById(functionId);
        Object returnValue;
        Throwable throwable;
        try {
            assert(udfRunner != null);
            // Call the user-defined function.
            returnValue = udfRunner.call(m_udfBuffer);

            VoltType returnType = udfRunner.getReturnType();
            // If the function we are running returns variable-length return value,
            // it may be possible that the buffer is not large enough to hold it.
            // Check the required buffer size and enlarge the existing buffer when necessary.
            // The default buffer size is 256K, which is more than enough for any
            // fixed-length data and NULL variable-length data (the buffer size will not go less than 256K).
            if (returnType.isVariableLength() && ! VoltType.isVoltNullValue(returnValue)) {
                // The minimum required size is 5 bytes:
                // 1 byte for the type indicator, 4 bytes for the prefixed length.
                int sizeRequired = 1 + 4;
                switch(returnType) {
                case VARBINARY:
                    if (returnValue instanceof byte[]) {
                        sizeRequired += ((byte[])returnValue).length;
                    }
                    else if (returnValue instanceof Byte[]) {
                        sizeRequired += ((Byte[])returnValue).length;
                    }
                    break;
                case STRING:
                    sizeRequired += ((String)returnValue).getBytes(Constants.UTF8ENCODING).length;
                    break;
                case GEOGRAPHY:
                    sizeRequired += ((GeographyValue)returnValue).getLengthInBytes();
                    break;
                default:
                }
                if (sizeRequired > m_udfBuffer.capacity()) {
                    resizeUDFBuffer(sizeRequired);
                }
            }
            // Write the result to the shared buffer.
            m_udfBuffer.clear();
            UserDefinedScalarFunctionRunner.writeValueToBuffer(m_udfBuffer, returnType, returnValue);
            // Return zero status code for a successful execution.
            return 0;
        } catch (InvocationTargetException ex1) {
            // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
            // We need to get its cause and throw that to the user.
            throwable = ex1.getCause();
        } catch (Throwable ex2) {
            throwable = ex2;
        }
        // Getting here means the execution was not successful.
        assert(throwable != null);
        byte[] errorMsg = throwable.toString().getBytes(Constants.UTF8ENCODING);
        // It is very unlikely that the size of a user's error message will exceed the UDF buffer size.
        // But you never know.
        if (errorMsg.length + 4 > m_udfBuffer.capacity()) {
            resizeUDFBuffer(errorMsg.length + 4);
        }
        m_udfBuffer.clear();
        SerializationHelper.writeVarbinary(errorMsg, m_udfBuffer);
        return -1;
    }

    private UserDefinedAggregateFunctionRunner getUdafRunner() {
        m_udfBuffer.clear();
        m_udfBuffer.getInt(); // skip the buffer size integer, it is only used by VoltDB IPC.
        int functionId = m_udfBuffer.getInt();
        return m_functionManager.getAggregateFunctionRunnerById(functionId);
    }

    private void handleUDAFError(Throwable throwable) {
        // Getting here means the execution was not successful.
        assert(throwable != null);
        byte[] errorMsg = throwable.getLocalizedMessage().getBytes(Constants.UTF8ENCODING);
        // It is very unlikely that the size of a user's error message will exceed the UDF buffer size.
        // But you never know.
        if (errorMsg.length + 4 > m_udfBuffer.capacity()) {
            resizeUDFBuffer(errorMsg.length + 4);
        }
        m_udfBuffer.clear();
        SerializationHelper.writeVarbinary(errorMsg, m_udfBuffer);
    }

    private void resizeUDAFBuffer(Object returnValue, VoltType returnType) {
        // If the function we are running returns variable-length return value,
        // it may be possible that the buffer is not large enough to hold it.
        // Check the required buffer size and enlarge the existing buffer when necessary.
        // The default buffer size is 256K, which is more than enough for any
        // fixed-length data and NULL variable-length data (the buffer size will not go less than 256K).
        if (returnType.isVariableLength() && ! VoltType.isVoltNullValue(returnValue)) {
            // The minimum required size is 5 bytes:
            // 1 byte for the type indicator, 4 bytes for the prefixed length.
            int sizeRequired = 1 + 4;
            if (returnValue instanceof byte[] || returnValue instanceof Byte[]) {
                sizeRequired += Array.getLength(returnValue);
            }
            if (sizeRequired > m_udfBuffer.capacity()) {
                resizeUDFBuffer(sizeRequired);
            }
        }
    }

    public int callJavaUserDefinedAggregateStart(int functionId) {
        UserDefinedAggregateFunctionRunner udafRunner = m_functionManager.getAggregateFunctionRunnerById(functionId);
        try {
            assert(udafRunner != null);
            // Call the user-defined function start method
            udafRunner.start();
            m_udfBuffer.clear();
            // Return zero status code for a successful execution.
            return 0;
        } catch (InvocationTargetException ex1) {
            // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
            // We need to get its cause and throw that to the user.
            handleUDAFError(ex1.getCause());
        } catch (Throwable ex2) {
            handleUDAFError(ex2);
        }
        return -1;
    }

    public int callJavaUserDefinedAggregateAssemble() {
        UserDefinedAggregateFunctionRunner udafRunner = getUdafRunner();
        int udafIndex = m_udfBuffer.getInt();
        try {
            assert(udafRunner != null);
            // Call the user-defined function assemble method.
            // For vectorization, pass an array of arguments of same type, stored in m_udfBuffer
            udafRunner.assemble(m_udfBuffer, udafIndex);
            m_udfBuffer.clear();
            return 0;
        } catch (InvocationTargetException ex1) {
            // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
            // We need to get its cause and throw that to the user.
            handleUDAFError(ex1.getCause());
        } catch (Throwable ex2) {
            handleUDAFError(ex2);
        }
        return -1;
    }

    public int callJavaUserDefinedAggregateCombine() {
        UserDefinedAggregateFunctionRunner udafRunner = getUdafRunner();
        int udafIndex = m_udfBuffer.getInt();
        try {
            assert(udafRunner != null);

            Object workerObject = UserDefinedAggregateFunctionRunner.readObject(m_udfBuffer);
            // call the combine method with the deserialized worker object
            udafRunner.combine(workerObject, udafIndex);
            // Write the result to the shared buffer.
            m_udfBuffer.clear();
            return 0;
        } catch (InvocationTargetException ex1) {
            // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
            // We need to get its cause and throw that to the user.
            handleUDAFError(ex1.getCause());
        } catch (Throwable ex2) {
            handleUDAFError(ex2);
        }
        return -1;
    }

    public int callJavaUserDefinedAggregateWorkerEnd() {
        UserDefinedAggregateFunctionRunner udafRunner = getUdafRunner();
        int udafIndex = m_udfBuffer.getInt();
        // get the boolean value from the buffer that indicates whether this is for a partition table or a repicated table
        Object returnValue = null;
        VoltType returnType = null;
        try {
            assert(udafRunner != null);
            // we serialized the object to a byte array
            // and the return type for a worker is a varbinary
            Object workerInstance = udafRunner.getFunctionInstance(udafIndex);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(workerInstance);
                out.flush();
                returnValue = bos.toByteArray();
            } finally {
                try {
                    bos.close();
                } catch (IOException ex) {
                // ignore close exception
                }
            }
            returnType = VoltType.VARBINARY;
            udafRunner.clearFunctionInstance(udafIndex);
            resizeUDAFBuffer(returnValue, returnType);
            m_udfBuffer.clear();
            UserDefinedAggregateFunctionRunner.writeValueToBuffer(m_udfBuffer, returnType, returnValue);
            // Return zero status code for a successful execution.
            return 0;
        }
        catch (Throwable ex2) {
            handleUDAFError(ex2);
        }
        return -1;
    }

    public int callJavaUserDefinedAggregateCoordinatorEnd() {
        UserDefinedAggregateFunctionRunner udafRunner = getUdafRunner();
        int udafIndex = m_udfBuffer.getInt();
        Object returnValue = null;
        try {
            assert(udafRunner != null);
            // call the end method to terminate the entire aggregate function process
            returnValue = udafRunner.end(udafIndex);
            VoltType returnType = udafRunner.getReturnType();
            resizeUDAFBuffer(returnValue, returnType);
            m_udfBuffer.clear();
            UserDefinedAggregateFunctionRunner.writeValueToBuffer(m_udfBuffer, returnType, returnValue);
            // Return zero status code for a successful execution.
            return 0;
        }
        catch (InvocationTargetException ex1) {
            // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
            // We need to get its cause and throw that to the user.
            handleUDAFError(ex1.getCause());
        }
        catch (Throwable ex2) {
            handleUDAFError(ex2);
        }
        return -1;
    }

    /**
     * Store a large temp table block to disk.
     *
     * @param siteId       The site id of the block to store to disk
     * @param blockCounter The serial number of the block to store to disk
     * @param block        A directly-allocated ByteBuffer of the block
     * @return true if operation succeeded, false otherwise
     */
    public boolean storeLargeTempTableBlock(long siteId, long blockCounter, ByteBuffer block) {
        LargeBlockTask task = LargeBlockTask.getStoreTask(new BlockId(siteId, blockCounter), block);
        return executeLargeBlockTaskSynchronously(task);
    }

    /**
     * Read a large table block from disk and write it to a ByteBuffer.
     * Block will still be stored on disk when this operation completes.
     *
     * @param siteId         The originating site id of the block to load
     * @param blockCounter   The id of the block to load
     * @param block          The buffer to write the block to
     * @return The original address of the block (so that its internal pointers may get updated)
     */
    public boolean loadLargeTempTableBlock(long siteId, long blockCounter, ByteBuffer block) {
        LargeBlockTask task = LargeBlockTask.getLoadTask(new BlockId(siteId, blockCounter), block);
        return executeLargeBlockTaskSynchronously(task);
    }

    /**
     * Delete the block with the given id from disk.
     *
     * @param siteId        The originating site id of the block to release
     * @param blockCounter  The serial number of the block to release
     * @return True if the operation succeeded, and false otherwise
     */
    public boolean releaseLargeTempTableBlock(long siteId, long blockCounter) {
        LargeBlockTask task = LargeBlockTask.getReleaseTask(new BlockId(siteId, blockCounter));
        return executeLargeBlockTaskSynchronously(task);
    }

    @Override
    public byte[] executeTask(TaskType taskType, ByteBuffer task) throws EEException {
        try {
            assert(m_psetBuffer.limit() >= 8);
            m_psetBuffer.putLong(0, taskType.taskId);

            //Clear is destructive, do it before the native call
            m_nextDeserializer.clear();
            final int errorCode = nativeExecuteTask(pointer);
            checkErrorCode(errorCode);
            return (byte[])m_nextDeserializer.readArray(byte.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer getParamBufferForExecuteTask(int requiredCapacity) {
        clearPsetAndEnsureCapacity(8 + requiredCapacity);
        m_psetBuffer.position(8);
        return m_psetBuffer;
    }

    @Override
    public void setViewsEnabled(String viewNames, boolean enabled) {
        if (viewNames.equals("")) {
            return;
        }
        if (enabled) {
            LOG.info("The maintenance of the following views is restarting: " + viewNames);
        }
        else {
            LOG.info("The maintenance of the following views will be paused to accelerate the restoration: " + viewNames);
        }
        nativeSetViewsEnabled(pointer, getStringBytes(viewNames), enabled);
    }

    @Override
    public void disableExternalStreams() {
        nativeDisableExternalStreams(pointer);
    }

    @Override
    public boolean externalStreamsEnabled() {
        return nativeExternalStreamsEnabled(pointer);
    }

    @Override
    public void storeTopicsGroup(long undoToken, byte[] serializedGroup) {
        clearPsetAndEnsureCapacity(serializedGroup.length);
        m_psetBuffer.put(serializedGroup);
        checkErrorCode(nativeStoreTopicsGroup(pointer, undoToken));
    }

    @Override
    public void deleteTopicsGroup(long undoToken, String groupId) {
        checkErrorCode(nativeDeleteTopicsGroup(pointer, undoToken, groupId.getBytes(Constants.UTF8ENCODING)));
    }

    @Override
    public Pair<Boolean, byte[]> fetchTopicsGroups(int maxResultSize, String startGroupId) {
        byte[] groupIdBytes = startGroupId == null ? null : startGroupId.getBytes(Constants.UTF8ENCODING);
        m_nextDeserializer.clear();
        int result = nativeFetchTopicsGroups(pointer, maxResultSize, groupIdBytes);
        if (result < 0) {
            checkErrorCode(ERRORCODE_ERROR);
        }
        try {
            return Pair.of(result != 0, readVarbinary(m_nextDeserializer));
        } catch (IOException e) {
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public byte[] commitTopicsGroupOffsets(long spUniqueId, long undoToken, short requestVersion, String groupId,
            byte[] offsets) {
        clearPsetAndEnsureCapacity(offsets.length);
        m_psetBuffer.putInt(offsets.length);
        m_psetBuffer.put(offsets);
        m_nextDeserializer.clear();
        checkErrorCode(nativeCommitTopicsGroupOffsets(pointer, spUniqueId, undoToken, requestVersion,
                groupId.getBytes(Constants.UTF8ENCODING)));
        try {
            return readVarbinary(m_nextDeserializer);
        } catch (IOException e) {
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public byte[] fetchTopicsGroupOffsets(short requestVersion, String groupId, byte[] offsets) {
        clearPsetAndEnsureCapacity(offsets.length);
        m_psetBuffer.putInt(offsets.length);
        m_psetBuffer.put(offsets);
        m_nextDeserializer.clear();
        checkErrorCode(
                nativeFetchTopicsGroupOffsets(pointer, requestVersion, groupId.getBytes(Constants.UTF8ENCODING)));
        try {
            return readVarbinary(m_nextDeserializer);
        } catch (IOException e) {
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public void deleteExpiredTopicsOffsets(long undoToken, TimestampType deleteOlderThan) {
        checkErrorCode(nativeDeleteExpiredTopicsOffsets(pointer, undoToken, deleteOlderThan.getTime()));
    }

    @Override
    public void setReplicableTables(int clusterId, String[] tables) {
        byte[][] tableNames = null;
        if (tables != null) {
            tableNames = new byte[tables.length][];

            for (int i = 0; i < tables.length; ++i) {
                tableNames[i] = tables[i].getBytes(Constants.UTF8ENCODING);
            }
        }

        checkErrorCode(nativeSetReplicableTables(pointer, clusterId, tableNames));
    }

    @Override
    public void clearAllReplicableTables() {
        checkErrorCode(nativeClearAllReplicableTables(pointer));
    }

    @Override
    public void clearReplicableTables(int clusterId) {
        checkErrorCode(nativeClearReplicableTables(pointer, clusterId));
    }

    private byte[] readVarbinary(FastDeserializer defaultDeserializer) throws IOException {
        try {
            return (m_fallbackBuffer == null ? defaultDeserializer : new FastDeserializer(m_fallbackBuffer))
                    .readVarbinary();
        } finally {
            m_fallbackBuffer = null;
        }
    }
}
