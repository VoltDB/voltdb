/* Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.lib.FileUtil;

/**
 * A file-based row store for temporary CACHED table persistence.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class DataFileCacheSession extends DataFileCache {

    public int storeCount;

    public DataFileCacheSession(Database db, String baseFileName) {
        super(db, baseFileName);
    }

    /**
     * Initial external parameters are set here. The size if fixed.
     */
    protected void initParams(Database database, String baseFileName) {

        fileName      = baseFileName + ".data.tmp";
        this.database = database;
        fa            = FileUtil.getDefaultInstance();

        int cacheSizeScale = 10;

        cacheFileScale = 8;

        Error.printSystemOut("cache_size_scale: " + cacheSizeScale);

        maxCacheSize = 2048;

        int avgRowBytes = 1 << cacheSizeScale;

        maxCacheBytes   = maxCacheSize * avgRowBytes;
        maxDataFileSize = (long) Integer.MAX_VALUE * 4;
        dataFile        = null;
    }

    /**
     * Opens the *.data file for this cache.
     */
    public void open(boolean readonly) {

        try {
            dataFile = ScaledRAFile.newScaledRAFile(database, fileName, false,
                    ScaledRAFile.DATA_FILE_RAF, null, null);
            fileFreePosition = INITIAL_FREE_POS;

            initBuffers();

            freeBlocks = new DataFileBlockManager(0, cacheFileScale, 0);
        } catch (Throwable e) {
            database.logger.appLog.logContext(e, null);
            close(false);

            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new Object[] {
                e, fileName
            });
        }
    }

    public synchronized void add(CachedObject object) {
        super.add(object);
    }

    /**
     *  Parameter write is always false. The backing file is simply closed and
     *  deleted.
     */
    public synchronized void close(boolean write) {

        try {
            if (dataFile != null) {
                dataFile.close();

                dataFile = null;

                fa.removeElement(fileName);
            }
        } catch (Throwable e) {
            database.logger.appLog.logContext(e, null);

            throw Error.error(ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                e, fileName
            });
        }
    }

    void postClose(boolean keep) {}

    public void clear() {

        cache.clear();

        fileFreePosition = INITIAL_FREE_POS;
    }

    public void deleteAll() {

        cache.clear();

        fileFreePosition = INITIAL_FREE_POS;

        initBuffers();
    }
}
