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

package org.voltdb;

import java.io.IOException;
import java.util.concurrent.Callable;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * SnapshotDataTarget implementation that drops snapshot data on the floor
 */
public class DevNullSnapshotTarget implements SnapshotDataTarget {

    VoltLogger m_log = new VoltLogger("HOST");

    @Override
    public int getHeaderSize() {
        return 0;
    }

    @Override
    public ListenableFuture<?> write(Callable<BBContainer> tupleData,
            SnapshotTableTask context) {
        m_log.info("DEBUG: writing block to dev null");

        return null;
    }

    @Override
    public void close() throws IOException, InterruptedException {
    }

    @Override
    public long getBytesWritten() {
        return 0;
    }

    @Override
    public void setOnCloseHandler(Runnable onClose) {
    }

    @Override
    public Throwable getLastWriteException() {
        return null;
    }

    @Override
    public SnapshotFormat getFormat() {
        return SnapshotFormat.STREAM;
    }

}
