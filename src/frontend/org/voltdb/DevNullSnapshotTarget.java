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

import java.io.IOException;
import java.util.concurrent.Callable;

import org.voltcore.utils.DBBPool.BBContainer;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

/**
 * SnapshotDataTarget implementation that drops snapshot data on the floor
 */
public class DevNullSnapshotTarget implements SnapshotDataTarget {

    Runnable m_onClose = null;

    @Override
    public int getHeaderSize() {
        return 0;
    }

    @Override
    public ListenableFuture<?> write(Callable<BBContainer> tupleData,
            int tableId) {
        try {
            BBContainer container = tupleData.call();
            if (container != null) {
                container.discard();
            }
        } catch (Exception e) {}
        return null;
    }

    @Override
    public boolean needsFinalClose()
    {
        return true;
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (m_onClose != null) {
            m_onClose.run();
        }
    }

    @Override
    public long getBytesWritten() {
        return 0;
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
        m_onClose = onClose;
    }

    @Override
    public Throwable getLastWriteException() {
        return null;
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.STREAM;
    }

    /**
     * Get the row count if any, of the content wrapped in the given {@link BBContainer}
     * @param tupleData
     * @return the numbers of tuple data rows contained within a container
     */
    @Override
    public int getInContainerRowCount(BBContainer tupleData) {
        return SnapshotDataTarget.ROW_COUNT_UNSUPPORTED;
    }
}
