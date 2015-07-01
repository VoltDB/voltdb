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

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.voltcore.utils.DBBPool.BBContainer;

public interface SnapshotDataTarget {
    public final static int ROW_COUNT_UNSUPPORTED = -1;
    /**
     * Get the number of bytes that should be left available at the beginning of each tuple block
     * provided to this target. The default implementation requires 4 bytes in order to length
     * precede each block.
     * @return Required space for a header in bytes
     */
    public int getHeaderSize();

    /**
     * Write a block of tuple data to this target
     * @param tupleData Tuple data in a <code>ByteBuffer</code> with the required number of bytes available
     * for a header
     * @param tableId   The catalog tableId
     */
    public ListenableFuture<?> write(Callable<BBContainer> tupleData, int tableId);

    /**
     * Does this target need to be closed by the last site to finish snapshotting?
     */
    public boolean needsFinalClose();

    /**
     * Close this target releasing any held resources and writing any footer info
     * @throws IOException
     */
    public void close() throws IOException, InterruptedException;

    public long getBytesWritten();

    public void setOnCloseHandler(Runnable onClose);

    /**
     * Get last cached exception that occurred during writes
     */
    public Throwable getLastWriteException();

    /**
     * Get the snapshot format this target uses
     */
    public SnapshotFormat getFormat();

    /**
     * Get the row count if any, of the content wrapped in the given {@link BBContainer}
     * @param tupleData
     * @return the numbers of tuple data rows contained within a container or
     *   ROW_COUNT_UNSUPPORTED if the implementor does not support it
     */
    public int getInContainerRowCount(BBContainer tupleData);
}
