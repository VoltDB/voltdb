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

package org.voltdb;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.UnmodifiableIterator;
import com.google_voltpatches.common.util.concurrent.Callables;
import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

/**
 * A class that manages streaming data from a single table. It knows how to interpret the return codes and check for
 * errors related to table data streaming using the table stream API.
 */
public class TableStreamer {
    private static final VoltLogger log = new VoltLogger("HOST");

    // Error code returned by EE.tableStreamSerializeMore().
    private static final byte SERIALIZATION_ERROR = -1;

    private final int m_tableId;
    private final TableStreamType m_type;
    private final ImmutableList<SnapshotTableTask> m_tableTasks;

    public TableStreamer(int tableId, TableStreamType type, List<SnapshotTableTask> tableTasks)
    {
        m_tableId = tableId;
        m_type = type;
        m_tableTasks = ImmutableList.copyOf(tableTasks);
    }

    /**
     * Activate the stream with the given predicates on the given table.
     * @param context       Context
     * @param predicates    Predicates associated with the stream
     * @return true if activation succeeded.
     */
    public boolean activate(SystemProcedureExecutionContext context, byte[] predicates)
    {
        return activate(context, false, predicates);
    }

    /**
     * Activate the stream with the given predicates on the given table.
     * @param context       Context
     * @param undoToken     The undo token
     * @param predicates    Predicates associated with the stream
     * @return true if activation succeeded.
     */
    public boolean activate(SystemProcedureExecutionContext context, boolean undo, byte[] predicates)
    {
        if (!context.activateTableStream(m_tableId, m_type, undo, predicates)) {
            String tableName = CatalogUtil.getTableNameFromId(context.getDatabase(), m_tableId);
            log.debug("Attempted to activate a table stream of type " + m_type +
                      "for table " + tableName + " and failed");
            return false;
        }
        return true;
    }

    /**
     * Streams more tuples from the table.
     * @param context          Context
     * @param outputBuffers    Allocated buffers to hold output tuples
     * @param rowCountAccumulator an array of a single int use to accumulate streamed rows count
     * @return A future for all writes to data targets, and a boolean indicating if there's more left in the table.
     * The future could be null if nothing is serialized. If row count is specified it sets the number of rows that
     * is to stream
     */
    @SuppressWarnings("rawtypes")
    public Pair<ListenableFuture, Boolean> streamMore(SystemProcedureExecutionContext context,
                                                      List<DBBPool.BBContainer> outputBuffers,
                                                      int[] rowCountAccumulator)
    {
        ListenableFuture writeFuture = null;

        prepareBuffers(outputBuffers);

        Pair<Long, int[]> serializeResult = context.tableStreamSerializeMore(m_tableId, m_type, outputBuffers);
        if (serializeResult.getFirst() == SERIALIZATION_ERROR) {
            VoltDB.crashLocalVoltDB("Failure while serializing data from table " + m_tableId, false, null);
        }

        if (serializeResult.getSecond()[0] > 0) {
            if (rowCountAccumulator != null && rowCountAccumulator.length == 1) {
                rowCountAccumulator[0] += getTupleDataRowCount(outputBuffers);
            }
            writeFuture = writeBlocksToTargets(outputBuffers, serializeResult.getSecond());
        } else {
            // Return all allocated snapshot output buffers
            for (DBBPool.BBContainer container : outputBuffers) {
                container.discard();
            }
        }

        return Pair.of(writeFuture, serializeResult.getFirst() > 0);
    }

    /**
     * Get the number of rows contained of rows contained within the given list of {@link BBContainer}
     * @param outputBuffers a list of tuple data BBContainers
     * @return the number of rows contained within the given list of {@link BBContainer}
     */
    private int getTupleDataRowCount(List<DBBPool.BBContainer> outputBuffers) {
        if (outputBuffers == null || outputBuffers.size() != m_tableTasks.size()) return 0;
        int accumulator = 0;
        for (int i = 0; i < outputBuffers.size(); ++i) {
            SnapshotDataTarget target = m_tableTasks.get(i).m_target;
            int rowCount = target.getInContainerRowCount(outputBuffers.get(i));
            if (rowCount != SnapshotDataTarget.ROW_COUNT_UNSUPPORTED) {
                accumulator += target.getInContainerRowCount(outputBuffers.get(i));
            }
        }
        return accumulator;
    }

    /**
     * Set the positions of the buffers to the start of the content, leaving some room for the headers.
     */
    private void prepareBuffers(List<DBBPool.BBContainer> buffers)
    {
        Preconditions.checkArgument(buffers.size() == m_tableTasks.size());

        UnmodifiableIterator<SnapshotTableTask> iterator = m_tableTasks.iterator();
        for (DBBPool.BBContainer container : buffers) {
            int headerSize = iterator.next().m_target.getHeaderSize();
            final ByteBuffer buf = container.b();
            buf.clear();
            buf.position(headerSize);
        }
    }

    /**
     * Finalize the output buffers and write them to the corresponding data targets
     *
     * @return A future that can used to wait for all targets to finish writing the buffers
     */
    private ListenableFuture<?> writeBlocksToTargets(Collection<DBBPool.BBContainer> outputBuffers,
                                                     int[] serialized)
    {
        Preconditions.checkArgument(m_tableTasks.size() == serialized.length);
        Preconditions.checkArgument(outputBuffers.size() == serialized.length);

        final List<ListenableFuture<?>> writeFutures =
            new ArrayList<ListenableFuture<?>>(outputBuffers.size());

        // The containers, the data targets, and the serialized byte counts should all line up
        Iterator<DBBPool.BBContainer> containerIter = outputBuffers.iterator();
        int serializedIndex = 0;

        for (SnapshotTableTask task : m_tableTasks) {
            final DBBPool.BBContainer container = containerIter.next();

            /*
             * Finalize the buffer by setting position to 0 and limit to the last used byte
             */
            final ByteBuffer buf = container.b();
            buf.limit(serialized[serializedIndex++] + task.m_target.getHeaderSize());
            buf.position(0);

            Callable<DBBPool.BBContainer> valueForTarget = Callables.returning(container);
            if (task.m_filters != null) {
                for (SnapshotDataFilter filter : task.m_filters) {
                    valueForTarget = filter.filter(valueForTarget);
                }
            }

            ListenableFuture<?> writeFuture = task.m_target.write(valueForTarget, m_tableId);
            if (writeFuture != null) {
                writeFutures.add(writeFuture);
            }
        }

        // Wraps all write futures in one future
        return Futures.allAsList(writeFutures);
    }
}
