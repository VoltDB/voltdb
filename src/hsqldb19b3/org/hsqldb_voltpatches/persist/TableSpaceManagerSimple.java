/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.persist;

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.DoubleIntIndex;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.3.0
 */
public class TableSpaceManagerSimple implements TableSpaceManager {

    DataFileCache cache;
    final int     scale;

    public TableSpaceManagerSimple(DataFileCache cache) {
        this.cache = cache;
        this.scale = cache.getDataFileScale();
    }

    public int getSpaceID() {
        return DataSpaceManager.tableIdDefault;
    }

    public void release(long pos, int rowSize) {}

    /**
     * Returns the position of a free block or 0.
     */
    public long getFilePosition(long rowSize, boolean asBlocks) {

        cache.writeLock.lock();

        try {
            long position;
            long newFreePosition;

            position        = cache.getFileFreePos() / scale;
            newFreePosition = cache.getFileFreePos() + rowSize;

            if (newFreePosition > cache.maxDataFileSize) {
                cache.logSevereEvent("data file reached maximum size "
                                     + cache.dataFileName, null);

                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }

            cache.fileFreePosition = newFreePosition;

            return position;
        } finally {
            cache.writeLock.unlock();
        }
    }

    public boolean hasFileRoom(long blockSize) {
        return true;
    }

    public void addFileBlock(long blockFreePos, long blockLimit) {}

    public void initialiseFileBlock(DoubleIntIndex lookup, long blockFreePos,
                                    long blockLimit) {}

    public void reset() {}

    public long getLostBlocksSize() {
        return 0;
    }

    public boolean isDefaultSpace() {
        return true;
    }
}
