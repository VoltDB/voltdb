/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.ParameterSet;
import org.voltdb.PlannerStatsCollector.CacheUse;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FastSerializer.BufferGrowCallback;

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

    /** java.util.logging logger. */
    private static final VoltLogger LOG = new VoltLogger("HOST");

    /** The HStoreEngine pointer. */
    private long pointer;

    /** Create a FastSerializer for serializing arguments to C++. Use a direct
    ByteBuffer as it will be passed directly to the C++ code. */
    private final FastSerializer fsForParameterSet;

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
    private final BBContainer deserializerBufferOrigin = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private FastDeserializer deserializer =
        new FastDeserializer(deserializerBufferOrigin.b);

    /*
     * For large result sets the EE will allocate new memory for the results
     * and invoke a callback to set the allocated memory here.
     */
    private ByteBuffer fallbackBuffer = null;

    private final BBContainer exceptionBufferOrigin = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 5);
    private ByteBuffer exceptionBuffer = exceptionBufferOrigin.b;

    /**
     * initialize the native Engine object.
     */
    public ExecutionEngineJNI(
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int hostId,
            final String hostname,
            final int tempTableMemory,
            final int totalPartitions)
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
                    hostId,
                    getStringBytes(hostname),
                    tempTableMemory * 1024 * 1024,
                    totalPartitions);
        checkErrorCode(errorCode);
        fsForParameterSet = new FastSerializer(true, new BufferGrowCallback() {
            @Override
            public void onBufferGrow(final FastSerializer obj) {
                LOG.trace("Parameter buffer has grown. re-setting to EE..");
                final int code = nativeSetBuffers(pointer,
                        fsForParameterSet.getContainerNoFlip().b,
                        fsForParameterSet.getContainerNoFlip().b.capacity(),
                        deserializer.buffer(), deserializer.buffer().capacity(),
                        exceptionBuffer, exceptionBuffer.capacity());
                checkErrorCode(code);
            }
        });

        errorCode = nativeSetBuffers(pointer, fsForParameterSet.getContainerNoFlip().b,
                fsForParameterSet.getContainerNoFlip().b.capacity(),
                deserializer.buffer(), deserializer.buffer().capacity(),
                exceptionBuffer, exceptionBuffer.capacity());
        checkErrorCode(errorCode);
        //LOG.info("Initialized Execution Engine");
    }

    /** Utility method to throw a Runtime exception based on the error code and serialized exception **/
    @Override
    final protected void throwExceptionForError(final int errorCode) throws RuntimeException {
        exceptionBuffer.clear();
        final int exceptionLength = exceptionBuffer.getInt();

        if (exceptionLength == 0) {
            throw new EEException(errorCode);
        } else {
            exceptionBuffer.position(0);
            exceptionBuffer.limit(4 + exceptionLength);
            throw SerializableException.deserializeFromBuffer(exceptionBuffer);
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
        deserializer = null;
        deserializerBufferOrigin.discard();
        exceptionBuffer = null;
        exceptionBufferOrigin.discard();
        LOG.trace("Released Execution Engine.");
    }

    /**
     *  Provide a serialized catalog and initialize version 0 of the engine's
     *  catalog.
     */
    @Override
    public void loadCatalog(long timestamp, final String serializedCatalog) throws EEException {
        //C++ JSON deserializer is not thread safe, must synchronize
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeLoadCatalog(pointer, timestamp, getStringBytes(serializedCatalog));
        }
        checkErrorCode(errorCode);
        //LOG.info("Loaded Catalog.");
    }

    /**
     * Provide a catalog diff and a new catalog version and update the
     * engine's catalog.
     */
    @Override
    public void updateCatalog(long timestamp, final String catalogDiffs) throws EEException {
        //C++ JSON deserializer is not thread safe, must synchronize
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeUpdateCatalog(pointer, timestamp, getStringBytes(catalogDiffs));
        }
        checkErrorCode(errorCode);
    }

    @Override
    public long loadPlanFragment(byte[] plan) throws EEException
    {
        deserializer.clear();

        long cacheSize = 0;
        CacheUse cacheUse = CacheUse.FAIL;

        // Start collecting statistics
        startStatsCollection();

        //C++ JSON deserializer is not thread safe, must synchronize
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeLoadPlanFragment(pointer, plan);
        }
        try {
            checkErrorCode(errorCode);
            FastDeserializer fds = fallbackBuffer == null ? deserializer : new FastDeserializer(fallbackBuffer);
            try {
                final long fragId = fds.readLong();
                final boolean wasHit = fds.readBoolean();
                cacheSize = fds.readLong();
                if (fragId == 0) {
                    throw new EEException(ERRORCODE_ERROR);
                }
                if (wasHit) {
                    cacheUse = CacheUse.HIT1;
                }
                else {
                    cacheUse = CacheUse.MISS;
                }
                return fragId;
            } catch (final IOException ex) {
                LOG.error("Failed to deserialze loadPlanFragment results" + ex);
                throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
            }
        } finally {
            // Stop collecting statistics.
            endStatsCollection(cacheSize, cacheUse);

            fallbackBuffer = null;
        }
    }

    private static byte[] getStringBytes(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
           throw new AssertionError(e);
        }
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     */
    @Override
    public VoltTable[] executePlanFragments(
            final int numFragmentIds,
            final long[] planFragmentIds,
            final long[] inputDepIds,
            final ParameterSet[] parameterSets,
            final long txnId, final long lastCommittedTxnId, final long undoToken) throws EEException
    {
        // plan frag zero is invalid
        assert((numFragmentIds == 0) || (planFragmentIds[0] != 0));

        if (numFragmentIds == 0) return new VoltTable[0];
        final int batchSize = numFragmentIds;
        if (LOG.isTraceEnabled()) {
            for (int i = 0; i < batchSize; ++i) {
                LOG.trace("Batch Executing planfragment:" + planFragmentIds[i] + ", params=" + parameterSets[i].toString());
            }
        }

        // serialize the param sets
        fsForParameterSet.clear();
        for (int i = 0; i < batchSize; ++i) {
            try {
                parameterSets[i].writeExternal(fsForParameterSet);
            }
            catch (final IOException exception) {
                throw new RuntimeException("Error serializing parameters for SQL batch element: " +
                                           i + " with plan fragment ID: " + planFragmentIds[i] +
                                           " and with params: " +
                                           parameterSets[i].toJSONString(), exception);
            }
        }
        // checkMaxFsSize();

        // Execute the plan, passing a raw pointer to the byte buffers for input and output
        deserializer.clear();
        final int errorCode =
            nativeExecutePlanFragments(
                    pointer,
                    numFragmentIds,
                    planFragmentIds,
                    inputDepIds,
                    txnId,
                    lastCommittedTxnId,
                    undoToken);

        try {
            checkErrorCode(errorCode);
            FastDeserializer fds = fallbackBuffer == null ? deserializer : new FastDeserializer(fallbackBuffer);
            // get a copy of the result buffers and make the tables
            // use the copy
            try {
                // read the complete size of the buffer used
                final int totalSize = fds.readInt();
                // check if anything was changed
                final boolean dirty = fds.readBoolean();
                if (dirty)
                    m_dirty = true;
                // get a copy of the buffer
                final ByteBuffer fullBacking = fds.readBuffer(totalSize);
                final VoltTable[] results = new VoltTable[batchSize];
                for (int i = 0; i < batchSize; ++i) {
                    final int numdeps = fullBacking.getInt(); // number of dependencies for this frag
                    assert(numdeps == 1);
                    @SuppressWarnings("unused")
                    final
                    int depid = fullBacking.getInt(); // ignore the dependency id
                    final int tableSize = fullBacking.getInt();
                    // reasonableness check
                    assert(tableSize < 50000000);
                    final ByteBuffer tableBacking = fullBacking.slice();
                    fullBacking.position(fullBacking.position() + tableSize);
                    tableBacking.limit(tableSize);

                    results[i] = PrivateVoltTableFactory.createVoltTableFromBuffer(tableBacking, true);
                }
                return results;
            } catch (final IOException ex) {
                LOG.error("Failed to deserialze result table" + ex);
                throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
            }
        } finally {
            fallbackBuffer = null;
        }
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Retrieving VoltTable:" + tableId);
        }
        deserializer.clear();
        final int errorCode = nativeSerializeTable(pointer, tableId, deserializer.buffer(),
                deserializer.buffer().capacity());
        checkErrorCode(errorCode);

        try {
            return deserializer.readObject(VoltTable.class);
        } catch (final IOException ex) {
            LOG.error("Failed to retrieve table:" + tableId + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public void loadTable(final int tableId, final VoltTable table,
        final long txnId, final long lastCommittedTxnId) throws EEException
    {
        if (LOG.isTraceEnabled()) {
            LOG.trace("loading table id=" + tableId + "...");
        }
        byte[] serialized_table = table.getTableDataReference().array();
        if (LOG.isTraceEnabled()) {
            LOG.trace("passing " + serialized_table.length + " bytes to EE...");
        }

        final int errorCode = nativeLoadTable(pointer, tableId, serialized_table,
                                              txnId, lastCommittedTxnId);
        checkErrorCode(errorCode);
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
            final SysProcSelector selector,
            final int locators[],
            final boolean interval,
            final Long now)
    {
        deserializer.clear();
        final int numResults = nativeGetStats(pointer, selector.ordinal(), locators, interval, now);
        if (numResults == -1) {
            throwExceptionForError(ERRORCODE_ERROR);
        }


        try {
            deserializer.readInt();//Ignore the length of the result tables
            FastDeserializer fds = fallbackBuffer == null ? deserializer : new FastDeserializer(fallbackBuffer);
            final VoltTable results[] = new VoltTable[numResults];
            for (int ii = 0; ii < numResults; ii++) {
                final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
                results[ii] = (VoltTable)fds.readObject(resultTable, this);
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
        return;
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        return nativeReleaseUndoToken(pointer, undoToken);
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
        return nativeSetLogLevels( pointer, logLevels);
    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType streamType) {
        return nativeActivateTableStream( pointer, tableId, streamType.ordinal());
    }

    @Override
    public int tableStreamSerializeMore(BBContainer c, int tableId, TableStreamType streamType) {
        return nativeTableStreamSerializeMore(pointer, c.address, c.b.position(), c.b.remaining(), tableId, streamType.ordinal());
    }

    /**
     * Instruct the EE to execute an Export poll and/or ack action. Poll response
     * data is returned in the usual results buffer, length preceded as usual.
     */
    @Override
    public ExportProtoMessage exportAction(boolean syncAction,
            long ackTxnId, long seqNo, int partitionId, String tableSignature)
    {
        deserializer.clear();
        ExportProtoMessage result = null;
        long retval = nativeExportAction(pointer,
                                         syncAction, ackTxnId, seqNo, getStringBytes(tableSignature));
        if (retval < 0) {
            result = new ExportProtoMessage( 0, partitionId, tableSignature);
            result.error();
        }
        return result;
    }

    @Override
    public long[] getUSOForExportTable(String tableSignature) {
        return nativeGetUSOForExportTable(pointer, getStringBytes(tableSignature));
    }

    @Override
    public void processRecoveryMessage( ByteBuffer buffer, long bufferPointer) {
        nativeProcessRecoveryMessage( pointer, bufferPointer, buffer.position(), buffer.remaining());
    }

    @Override
    public long tableHashCode(int tableId) {
        return nativeTableHashCode( pointer, tableId);
    }

    @Override
    public int hashinate(Object value, int partitionCount)
    {
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setParameters(value);

        // serialize the param set
        fsForParameterSet.clear();
        try {
            parameterSet.writeExternal(fsForParameterSet);
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }

        return nativeHashinate(pointer, partitionCount);
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        return nativeGetThreadLocalPoolAllocations();
    }

    /*
     * Instead of using the reusable output buffer to get results for the next batch,
     * use this buffer allocated by the EE. This is for one time use.
     */
    public void fallbackToEEAllocatedBuffer(ByteBuffer buffer) {
        assert(buffer != null);
        assert(fallbackBuffer == null);
        fallbackBuffer = buffer;
    }
}
