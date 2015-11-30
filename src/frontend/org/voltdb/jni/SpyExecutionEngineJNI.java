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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;

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
public class SpyExecutionEngineJNI extends ExecutionEngineJNI {
    private final Map<Long, Boolean> m_callingThreadsToBlock = new ConcurrentHashMap<Long, Boolean>();

    /**
     * initialize the native Engine object.
     */
    public SpyExecutionEngineJNI(
            final long siteThreadId,
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int hostId,
            final String hostname,
            final int drClusterId,
            final int tempTableMemory,
            final HashinatorConfig hashinatorConfig,
            final boolean createDrReplicatedStream)
    {
        super(clusterIndex, siteId, partitionId, hostId, hostname,
                drClusterId, tempTableMemory, hashinatorConfig, createDrReplicatedStream);
        m_callingThreadsToBlock.put(siteThreadId, false);
    }

    final public void debugTrackExternalThreads(long threadId, boolean ignoreThread) {
        m_callingThreadsToBlock.put(threadId, ignoreThread);
    }

    /**
     * Releases the Engine object.
     * This method is automatically called from #finalize(), but
     * it's recommended to call this method just after you finish
     * using the object.
     */
    @Override
    public Void release() throws EEException {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.release();
    }

    /**
     *  Provide a serialized catalog and initialize version 0 of the engine's
     *  catalog.
     */
    @Override
    protected Void loadCatalog(long timestamp, final byte[] catalogBytes) throws EEException {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.loadCatalog(timestamp, catalogBytes);
    }

    /**
     * Provide a catalog diff and a new catalog version and update the
     * engine's catalog.
     */
    @Override
    public Void updateCatalog(long timestamp, final String catalogDiffs) throws EEException {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.updateCatalog(timestamp, catalogDiffs);
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     */
    @Override
    protected VoltTable[] coreExecutePlanFragments(
            final int numFragmentIds,
            final long[] planFragmentIds,
            final long[] inputDepIds,
            final Object[] parameterSets,
            final long txnId,
            final long spHandle,
            final long lastCommittedSpHandle,
            long uniqueId,
            final long undoToken) throws EEException
    {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.coreExecutePlanFragments(numFragmentIds, planFragmentIds, inputDepIds,
                parameterSets, txnId, spHandle, lastCommittedSpHandle, uniqueId, undoToken);
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.serializeTable(tableId);
    }

    @Override
    public byte[] loadTable(final int tableId, final VoltTable table, final long txnId,
        final long spHandle,
        final long lastCommittedSpHandle,
        final long uniqueId,
        boolean returnUniqueViolations,
        boolean shouldDRStream,
        long undoToken) throws EEException
    {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.loadTable(tableId, table, txnId, spHandle, lastCommittedSpHandle,
                uniqueId, returnUniqueViolations, shouldDRStream, undoToken);
    }

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    @Override
    public Void tick(final long time, final long lastCommittedTxnId) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.tick(time, lastCommittedTxnId);
    }

    @Override
    public Void quiesce(long lastCommittedTxnId) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.quiesce(lastCommittedTxnId);
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
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.getStats(selector, locators, interval, now);
    }

    @Override
    public Void toggleProfiler(final int toggle) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.toggleProfiler(toggle);
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return false;
        }
        return super.releaseUndoToken(undoToken);
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return false;
        }
        return super.undoUndoToken(undoToken);
    }

    /**
     * Set the log levels to be used when logging in this engine
     * @param logLevels Levels to set
     * @throws EEException
     * @returns true on success false on failure
     */
    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return false;
        }
        return super.setLogLevels(logLevels);
    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType streamType,
                                       long undoQuantumToken,
                                       byte[] predicates) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return false;
        }
        return super.activateTableStream(tableId, streamType, undoQuantumToken, predicates);
    }

    @Override
    public Pair<Long, int[]> tableStreamSerializeMore(int tableId,
                                                      TableStreamType streamType,
                                                      List<BBContainer> outputBuffers) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return new Pair<Long, int[]>(-1l, null);
        }
        return super.tableStreamSerializeMore(tableId, streamType, outputBuffers);
    }

    /**
     * Instruct the EE to execute an Export poll and/or ack action. Poll response
     * data is returned in the usual results buffer, length preceded as usual.
     */
    @Override
    public Void exportAction(boolean syncAction,
            long ackTxnId, long seqNo, int partitionId, String tableSignature)
    {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.exportAction(syncAction, ackTxnId, seqNo, partitionId, tableSignature);
    }

    @Override
    public long[] getUSOForExportTable(String tableSignature) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.getUSOForExportTable(tableSignature);
    }

    @Override
    public Void processRecoveryMessage( ByteBuffer buffer, long bufferPointer) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.processRecoveryMessage(buffer, bufferPointer);
    }

    @Override
    public long tableHashCode(int tableId) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return Long.MIN_VALUE;
        }
        return super.tableHashCode(tableId);
    }

    @Override
    public int hashinate(
            Object value,
            HashinatorConfig config)
    {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return Integer.MIN_VALUE;
        }
        return super.hashinate(value, config);
    }

    @Override
    public Void updateHashinator(HashinatorConfig config)
    {
        if (m_callingThreadsToBlock != null && m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.updateHashinator(config);
    }

    @Override
    public long applyBinaryLog(ByteBuffer log, long txnId, long spHandle, long lastCommittedSpHandle, long uniqueId,
                               int remoteClusterId, long undoToken) throws EEException
    {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return Long.MIN_VALUE;
        }
        return super.applyBinaryLog(log, txnId, spHandle, lastCommittedSpHandle, uniqueId, remoteClusterId, undoToken);
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return Long.MIN_VALUE;
        }
        return super.getThreadLocalPoolAllocations();
    }

    /*
     * Instead of using the reusable output buffer to get results for the next batch,
     * use this buffer allocated by the EE. This is for one time use.
     */
    public void fallbackToEEAllocatedBuffer(ByteBuffer buffer) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return;
        }
        super.fallbackToEEAllocatedBuffer(buffer);
    }

    @Override
    public byte[] executeTask(TaskType taskType, ByteBuffer task) throws EEException {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.executeTask(taskType, task);
    }

    @Override
    public ByteBuffer getParamBufferForExecuteTask(int requiredCapacity) {
        if (m_callingThreadsToBlock.get(Thread.currentThread().getId())) {
            return null;
        }
        return super.getParamBufferForExecuteTask(requiredCapacity);
    }
}
