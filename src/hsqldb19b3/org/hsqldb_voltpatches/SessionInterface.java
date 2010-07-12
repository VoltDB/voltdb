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


package org.hsqldb_voltpatches;

import java.io.InputStream;

import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultLob;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.TimestampData;

/**
 * Interface to Session and its remote proxy objects. Used by the
 * implementations of JDBC interfaces to communicate with the database at
 * the session level.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.7.2
 */
public interface SessionInterface {

    int INFO_ID      = 0;                // used
    int INFO_INTEGER = 1;                // used
    int INFO_BOOLEAN = 2;                // used
    int INFO_VARCHAR = 3;                // used
    int INFO_LIMIT   = 4;

    //
    int INFO_ISOLATION           = 0;    // used
    int INFO_AUTOCOMMIT          = 1;    // used
    int INFO_CONNECTION_READONLY = 2;    // used
    int INFO_CATALOG             = 3;    // used

    //
    int TX_READ_UNCOMMITTED = 1;
    int TX_READ_COMMITTED   = 2;
    int TX_REPEATABLE_READ  = 4;
    int TX_SERIALIZABLE     = 8;

    Result execute(Result r);

    RowSetNavigatorClient getRows(long navigatorId, int offset, int size);

    void closeNavigator(long id);

    void close();

    boolean isClosed();

    boolean isReadOnlyDefault();

    void setReadOnlyDefault(boolean readonly);

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    int getIsolation();

    void setIsolationDefault(int level);

    void startPhasedTransaction();

    void prepareCommit();

    void commit(boolean chain);

    void rollback(boolean chain);

    void rollbackToSavepoint(String name);

    void savepoint(String name);

    void releaseSavepoint(String name);

    void addWarning(HsqlException warning);

    Object getAttribute(int id);

    void setAttribute(int id, Object value);

    long getId();

    void resetSession();

    String getInternalConnectionURL();

    public BlobDataID createBlob(long length);

    public ClobDataID createClob(long length);

    void allocateResultLob(ResultLob result, InputStream dataInput);

    Scanner getScanner();

    TimestampData getCurrentDate();

    int getZoneSeconds();

    int getStreamBlockSize();
}
