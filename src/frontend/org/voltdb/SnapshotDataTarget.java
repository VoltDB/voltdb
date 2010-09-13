/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.utils.DBBPool.BBContainer;
import java.io.IOException;
import java.util.concurrent.Future;

public interface SnapshotDataTarget {
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
     */
    public Future<?> write(BBContainer tupleData);

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
    public IOException getLastWriteException();
}
