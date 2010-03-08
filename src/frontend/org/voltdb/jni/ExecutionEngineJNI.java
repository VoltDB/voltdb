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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.ParameterSet;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FastSerializer.BufferGrowCallback;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.DBBPool.BBContainer;

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
    private static final Logger LOG = Logger.getLogger(ExecutionEngine.class.getName(), VoltLoggerFactory.instance());

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
    private final BBContainer deserializerBufferOrigin = org.voltdb.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private FastDeserializer deserializer =
        new FastDeserializer(deserializerBufferOrigin.b);

    private final BBContainer exceptionBufferOrigin = org.voltdb.utils.DBBPool.allocateDirect(4096);
    private ByteBuffer exceptionBuffer = exceptionBufferOrigin.b;

    /**
     * initialize the native Engine object.
     * @see #nativeCreate()
     */
    public ExecutionEngineJNI(final ExecutionSite site, final int clusterIndex, final int siteId) {
        // base class loads the volt shared library
        super(site);
        //exceptionBuffer.order(ByteOrder.nativeOrder());
        LOG.trace("Creating Execution Engine on clusterIndex=" + clusterIndex
                + ", site_id = " + siteId + "...");
        pointer = nativeCreate();
        nativeSetLogLevels(pointer, EELoggers.getLogLevels());
        int errorCode = nativeInitialize(pointer, clusterIndex, siteId);
        checkErrorCode(errorCode);
        fsForParameterSet = new FastSerializer(false, new BufferGrowCallback() {
            public void onBufferGrow(final FastSerializer obj) {
                LOG.trace("Parameter buffer has grown. re-setting to EE..");
                final int code = nativeSetBuffers(pointer,
                        fsForParameterSet.getContainerNoFlip().b,
                        fsForParameterSet.getContainerNoFlip().b.capacity(),
                        deserializer.buffer(), deserializer.buffer().capacity(),
                        exceptionBuffer, exceptionBuffer.capacity());
                checkErrorCode(code);
            }
        }, null);

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
        final short exceptionLength = exceptionBuffer.getShort();

        if (exceptionLength == 0) {
            throw new EEException(errorCode);
        } else {
            exceptionBuffer.position(0);
            exceptionBuffer.limit(2 + exceptionLength);
            throw SerializableException.deserializeFromBuffer(exceptionBuffer);
        }
    }

    /**
     * Releases the Engine object.
     * This method is automatically called from #finalize(), but
     * it's recommended to call this method just after you finish
     * using the object.
     * @see #nativeDestroy(long)
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
     * Wrapper for {@link #nativeLoadCatalog(long, String)}.
     */
    @Override
    public void loadCatalog(final String serializedCatalog) throws EEException {
        //C++ JSON deserializer is not thread safe, must synchronize
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeLoadCatalog(pointer, serializedCatalog);
        }
        checkErrorCode(errorCode);
        //LOG.info("Loaded Catalog.");
    }

    /**
     * Wrapper for {@link #nativeUpdateCatalog(long, String)}.
     */
    @Override
    public void updateCatalog(final String catalogDiffs) throws EEException {
        //C++ JSON deserializer is not thread safe, must synchronize
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeUpdateCatalog(pointer, catalogDiffs);
        }
        checkErrorCode(errorCode);
        //LOG.info("Loaded Catalog.");
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     * Wrapper for {@link #nativeExecutePlanFragment(long, long, int, int, long, long, long)}.
     */
    @Override
    public DependencyPair executePlanFragment(final long planFragmentId,
                                              final int outputDepId,
                                              final int inputDepId,
                                              final ParameterSet parameterSet,
                                              final long txnId,
                                              final long lastCommittedTxnId,
                                              final long undoToken)
      throws EEException
    {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Executing planfragment:" + planFragmentId + ", params=" + parameterSet.toString());
        }

        // serialize the param set
        fsForParameterSet.clear();
        try {
            parameterSet.writeExternal(fsForParameterSet);
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }
        // checkMaxFsSize();
        // Execute the plan, passing a raw pointer to the byte buffer.
        deserializer.clear();
        final int errorCode = nativeExecutePlanFragment(pointer, planFragmentId, outputDepId, inputDepId,
                                                        txnId, lastCommittedTxnId, undoToken);
        checkErrorCode(errorCode);

        try {
            // read the complete size of the buffer used (ignored here)
            deserializer.readInt();
            // check if anything was changed
            final boolean dirty = deserializer.readBoolean();
            if (dirty)
                m_dirty = true;
            // read the number of tables returned by this fragment
            final int numDependencies = deserializer.readInt();
            final VoltTable dependencies[] = new VoltTable[numDependencies];
            final int depIds[] = new int[numDependencies];
            for (int i = 0; i < numDependencies; ++i) {
                depIds[i] = deserializer.readInt();
                dependencies[i] = deserializer.readObject(VoltTable.class);
            }
            assert(depIds.length == 1);
            return new DependencyPair(depIds[0], dependencies[0]);
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result dependencies" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }

    }

    @Override
    public VoltTable executeCustomPlanFragment(final String plan, final int outputDepId,
            final int inputDepId, final long txnId, final long lastCommittedTxnId,
            final long undoQuantumToken) throws EEException
    {
        fsForParameterSet.clear();
        deserializer.clear();
        //C++ JSON deserializer is not thread safe, must synchronize
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeExecuteCustomPlanFragment(pointer, plan, outputDepId, inputDepId,
                                                        txnId, lastCommittedTxnId, undoQuantumToken);
        }
        checkErrorCode(errorCode);

        try {
            deserializer.readInt(); // total size of the data
            // check if anything was changed
            final boolean dirty = deserializer.readBoolean();
            if (dirty)
                m_dirty = true;
            final int numDependencies = deserializer.readInt();
            assert(numDependencies == 1);
            final VoltTable dependencies[] = new VoltTable[numDependencies];
            for (int i = 0; i < numDependencies; ++i) {
                /*int depId =*/ deserializer.readInt();
                dependencies[i] = deserializer.readObject(VoltTable.class);
            }
            return dependencies[0];
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result dependencies" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     * Wrapper for {@link #nativeExecuteQueryPlanFragmentsAndGetResults(long, long[], int, long, long, long)}.
     */
    @Override
    public VoltTable[] executeQueryPlanFragmentsAndGetResults(
            final long[] planFragmentIds,
            final int numFragmentIds,
            final ParameterSet[] parameterSets,
            final int numParameterSets,
            final long txnId, final long lastCommittedTxnId, final long undoToken) throws EEException {

        assert (planFragmentIds.length == parameterSets.length);
        if (numFragmentIds == 0) return new VoltTable[0];
        final int batchSize = numFragmentIds;
        if (LOG.isTraceEnabled()) {
            for (int i = 0; i < batchSize; ++i) {
                LOG.trace("Batch Executing planfragment:" + planFragmentIds[i] + ", params=" + parameterSets[i].toString());
            }
        }

        // serialize the param sets
        fsForParameterSet.clear();
        try {
            for (int i = 0; i < batchSize; ++i) {
                parameterSets[i].writeExternal(fsForParameterSet);
            }
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }
        // checkMaxFsSize();

        // Execute the plan, passing a raw pointer to the byte buffers for input and output
        deserializer.clear();
        final int errorCode = nativeExecuteQueryPlanFragmentsAndGetResults(pointer, planFragmentIds, numFragmentIds, txnId, lastCommittedTxnId, undoToken);
        checkErrorCode(errorCode);

        // get a copy of the result buffers and make the tables
        // use the copy
        try {
            // read the complete size of the buffer used
            final int totalSize = deserializer.readInt();
            // check if anything was changed
            final boolean dirty = deserializer.readBoolean();
            if (dirty)
                m_dirty = true;
            // get a copy of the buffer
            final ByteBuffer fullBacking = deserializer.readBuffer(totalSize);
            final VoltTable[] results = new VoltTable[batchSize];
            for (int i = 0; i < batchSize; ++i) {
                final int numdeps = fullBacking.getInt(); // number of dependencies for this frag
                assert(numdeps == 1);
                @SuppressWarnings("unused")
                final
                int depid = fullBacking.getInt(); // ignore the dependency id
                final int tableSize = fullBacking.getInt();
                // reasonableness check
                assert(tableSize < 10000000);
                final ByteBuffer tableBacking = fullBacking.slice();
                fullBacking.position(fullBacking.position() + tableSize);
                tableBacking.limit(tableSize);

                results[i] = new VoltTable(tableBacking, true);
            }
            return results;
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    /**
     * Wrapper for {@link #nativeSerializeTable(long, int, ByteBuffer, int)}.
     */
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

    /**
     * Wrapper for {@link #nativeLoadTable(long, int, byte[], long, long, long)}.
     */
    @Override
    public void loadTable(final int tableId, final VoltTable table,
        final long txnId, final long lastCommittedTxnId,
        final long undoToken, boolean allowELT) throws EEException
    {
        if (LOG.isTraceEnabled()) {
            LOG.trace("loading table id=" + tableId + "...");
        }
        byte[] serialized_table;
        try {
            final FastSerializer fs = new FastSerializer();
            fs.writeObject(table);
            serialized_table = fs.getBytes();
        } catch (final IOException exception) {
            throw new RuntimeException(exception);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("passing " + serialized_table.length + " bytes to EE...");
        }

        final int errorCode = nativeLoadTable(pointer, tableId, serialized_table,
                                              txnId, lastCommittedTxnId,
                                              undoToken, allowELT);
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
     *  @return Array of results tables. An array of length 0 indicates there are no results. null indicates failure.
     */
    @Override
    public VoltTable[] getStats(final SysProcSelector selector, final int locators[]) {
        deserializer.clear();
        final int numResults = nativeGetStats(pointer, selector.ordinal(), locators);
        if (numResults == -1) {
            return null;
        }


        try {
            deserializer.readInt();//Ignore the length of the result tables
            final VoltTable results[] = new VoltTable[numResults];
            for (int ii = 0; ii < numResults; ii++) {
                final VoltTable resultTable = new VoltTable();
                results[ii] = (VoltTable)deserializer.readObject(resultTable, this);
            }
            return results;
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for getStats" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    /**
     * Wrapper for {@link #nativeToggleProfiler(long, int)}.
     */
    @Override
    public int toggleProfiler(final int toggle) {
        return nativeToggleProfiler(pointer, toggle);
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
    public boolean activateCopyOnWrite(int tableId) {
        return nativeActivateCopyOnWrite( pointer, tableId);
    }

    @Override
    public int cowSerializeMore(BBContainer c, int tableId) {
        return nativeCOWSerializeMore(pointer, c.address, c.b.position(), c.b.remaining(), tableId);
    }
}
