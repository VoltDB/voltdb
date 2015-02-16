/* Copyright (c) 2001-2014, The HSQL Development Group
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.DatabaseURL;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Statement;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlByteArrayInputStream;
import org.hsqldb_voltpatches.lib.LineGroupReader;
import org.hsqldb_voltpatches.map.ValuePool;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultLob;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobData;
import org.hsqldb_voltpatches.types.ClobDataID;
import org.hsqldb_voltpatches.types.Collation;
import org.hsqldb_voltpatches.types.Types;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class LobManager {

    static final String resourceFileName =
        "/org/hsqldb_voltpatches/resources/lob-schema.sql";
    static final String[] starters = new String[]{ "/*" };

    //
    Database         database;
    LobStore         lobStore;
    Session          sysLobSession;
    volatile boolean storeModified;
    byte[]           byteBuffer;

    //
    Inflater inflater;
    Deflater deflater;
    byte[]   dataBuffer;

    //
    boolean cryptLobs;
    boolean compressLobs;
    int     lobBlockSize;
    int     largeLobBlockSize    = SessionInterface.lobStreamBlockSize;
    int     totalBlockLimitCount = Integer.MAX_VALUE;

    //
    Statement getLob;
    Statement getSpanningBlocks;
    Statement deleteLobCall;
    Statement deleteLobPartCall;
    Statement divideLobPartCall;
    Statement createLob;
    Statement createLobPartCall;
    Statement createSingleLobPartCall;
    Statement updateLobLength;
    Statement updateLobUsage;
    Statement getNextLobId;
    Statement deleteUnusedLobs;
    Statement mergeUnusedSpace;
    Statement getLobUseLimit;
    Statement getLobCount;
    Statement getSpanningParts;
    Statement getLastPart;
    Statement createPart;

    //
    boolean usageChanged;

    //
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          writeLock = lock.writeLock();

    // LOBS columns
    private interface LOBS {

        int BLOCK_ADDR   = 0;
        int BLOCK_COUNT  = 1;
        int BLOCK_OFFSET = 2;
        int LOB_ID       = 3;
    }

    private interface LOB_IDS {

        int LOB_ID          = 0;
        int LOB_LENGTH      = 1;
        int LOB_USAGE_COUNT = 2;
        int LOB_TYPE        = 3;
    }

    private interface GET_LOB_PART {

        int LOB_ID       = 0;
        int BLOCK_OFFSET = 1;
        int BLOCK_LIMIT  = 2;
    }

    private interface DIVIDE_BLOCK {
        int BLOCK_OFFSET = 0;
        int LOB_ID       = 1;
    }

    private interface DELETE_BLOCKS {

        int LOB_ID       = 0;
        int BLOCK_OFFSET = 1;
        int BLOCK_LIMIT  = 2;
        int TX_ID        = 3;
    }

    private interface ALLOC_BLOCKS {

        int BLOCK_COUNT  = 0;
        int BLOCK_OFFSET = 1;
        int LOB_ID       = 2;
    }

    private interface UPDATE_USAGE {
        int BLOCK_COUNT = 0;
        int LOB_ID      = 1;
    }

    private interface UPDATE_LENGTH {
        int LOB_LENGTH = 0;
        int LOB_ID     = 1;
    }

    private interface ALLOC_PART {

        int BLOCK_COUNT  = 0;
        int BLOCK_OFFSET = 1;
        int PART_OFFSET  = 2;
        int PART_LENGTH  = 3;
        int PART_BYTES   = 4;
        int LOB_ID       = 5;
    }

    private static final String existsBlocksSQL =
        "SELECT * FROM SYSTEM_LOBS.BLOCKS LIMIT 1";
    private static final String initialiseBlocksSQL =
        "INSERT INTO SYSTEM_LOBS.BLOCKS VALUES(?,?,?)";
    private static final String getLobSQL =
        "SELECT * FROM SYSTEM_LOBS.LOB_IDS WHERE LOB_IDS.LOB_ID = ?";
    private static final String getLobPartSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOBS.LOB_ID = ? AND BLOCK_OFFSET + BLOCK_COUNT > ? AND BLOCK_OFFSET < ? ORDER BY BLOCK_OFFSET";
    private static final String deleteLobPartCallSQL =
        "CALL SYSTEM_LOBS.DELETE_BLOCKS(?,?,?,?)";
    private static final String createLobSQL =
        "INSERT INTO SYSTEM_LOBS.LOB_IDS VALUES(?, ?, ?, ?)";
    private static final String updateLobLengthSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_LENGTH = ? WHERE LOB_IDS.LOB_ID = ?";
    private static final String createLobPartCallSQL =
        "CALL SYSTEM_LOBS.ALLOC_BLOCKS(?, ?, ?)";
    private static final String createSingleLobPartCallSQL =
        "CALL SYSTEM_LOBS.ALLOC_SINGLE_BLOCK(?, ?, ?)";
    private static final String divideLobPartCallSQL =
        "CALL SYSTEM_LOBS.DIVIDE_BLOCK(?, ?)";
    private static final String updateLobUsageSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_USAGE_COUNT = LOB_USAGE_COUNT + ? WHERE LOB_IDS.LOB_ID = ?";
    private static final String getNextLobIdSQL =
        "VALUES NEXT VALUE FOR SYSTEM_LOBS.LOB_ID";
    private static final String deleteLobCallSQL =
        "CALL SYSTEM_LOBS.DELETE_LOB(?, ?)";
    private static final String deleteUnusedCallSQL =
        "CALL SYSTEM_LOBS.DELETE_UNUSED_LOBS(?)";
    private static final String mergeUnusedSpaceSQL =
        "CALL SYSTEM_LOBS.MERGE_EMPTY_BLOCKS()";
    private static final String getLobUseLimitSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE BLOCK_ADDR = (SELECT MAX(BLOCK_ADDR) FROM SYSTEM_LOBS.LOBS)";
    private static final String getLobCountSQL =
        "SELECT COUNT(*) FROM SYSTEM_LOBS.LOB_IDS";

    //
    //BLOCK_COUNT INT NOT NULL, BLOCK_OFFSET INT, PART_OFFSET BIGINT NOT NULL, PART_LENGTH BIGINT NOT NULL, PART_BYTES BIGINT NOT NULL, LOB_ID BIGINT,
    private static final String getPartsSQL =
        "SELECT BLOCK_COUNT, BLOCK_OFFSET, PART_OFFSET, PART_LENGTH, PART_BYTES, LOB_ID "
        + "FROM SYSTEM_LOBS.PARTS "
        + "WHERE LOB_ID = ?  AND PART_OFFSET + PART_LENGTH > ? AND PART_OFFSET < ? ORDER BY BLOCK_OFFSET";
    private static final String getLastPartSQL =
        "SELECT BLOCK_COUNT, BLOCK_OFFSET, PART_OFFSET, PART_LENGTH, PART_BYTES, LOB_ID "
        + "FROM SYSTEM_LOBS.PARTS "
        + "WHERE LOB_ID = ? ORDER BY LOB_ID DESC, BLOCK_OFFSET DESC LIMIT 1";
    private static final String createPartSQL =
        "INSERT INTO SYSTEM_LOBS.PARTS VALUES ?,?,?,?,?,?";

    public LobManager(Database database) {
        this.database = database;
    }

    public void lock() {
        writeLock.lock();
    }

    public void unlock() {
        writeLock.unlock();
    }

    public void createSchema() {

        sysLobSession = database.sessionManager.getSysLobSession();

        InputStream fis = (InputStream) AccessController.doPrivileged(
            new PrivilegedAction() {

            public InputStream run() {
                return getClass().getResourceAsStream(resourceFileName);
            }
        });
        InputStreamReader reader = null;

        try {
            reader = new InputStreamReader(fis, "ISO-8859-1");
        } catch (Exception e) {}

        LineNumberReader lineReader = new LineNumberReader(reader);
        LineGroupReader  lg = new LineGroupReader(lineReader, starters);
        HashMappedList   map        = lg.getAsMap();

        lg.close();

        String    sql       = (String) map.get("/*lob_schema_definition*/");
        Statement statement = sysLobSession.compileStatement(sql);
        Result    result    = statement.execute(sysLobSession);

        if (result.isError()) {
            throw result.getException();
        }

        HsqlName name =
            database.schemaManager.getSchemaHsqlName("SYSTEM_LOBS");
        Table table = database.schemaManager.getTable(sysLobSession, "BLOCKS",
            "SYSTEM_LOBS");

        compileStatements();
    }

    public void compileStatements() {

        writeLock.lock();

        try {
            getLob            = sysLobSession.compileStatement(getLobSQL);
            getSpanningBlocks = sysLobSession.compileStatement(getLobPartSQL);
            createLob         = sysLobSession.compileStatement(createLobSQL);
            createLobPartCall =
                sysLobSession.compileStatement(createLobPartCallSQL);
            createSingleLobPartCall =
                sysLobSession.compileStatement(createSingleLobPartCallSQL);
            divideLobPartCall =
                sysLobSession.compileStatement(divideLobPartCallSQL);
            deleteLobCall = sysLobSession.compileStatement(deleteLobCallSQL);
            deleteLobPartCall =
                sysLobSession.compileStatement(deleteLobPartCallSQL);
            updateLobLength =
                sysLobSession.compileStatement(updateLobLengthSQL);
            updateLobUsage = sysLobSession.compileStatement(updateLobUsageSQL);
            getNextLobId   = sysLobSession.compileStatement(getNextLobIdSQL);
            deleteUnusedLobs =
                sysLobSession.compileStatement(deleteUnusedCallSQL);
            mergeUnusedSpace =
                sysLobSession.compileStatement(mergeUnusedSpaceSQL);
            getLobUseLimit = sysLobSession.compileStatement(getLobUseLimitSQL);
            getLobCount    = sysLobSession.compileStatement(getLobCountSQL);

            //
            getSpanningParts = sysLobSession.compileStatement(getPartsSQL);
            getLastPart      = sysLobSession.compileStatement(getLastPartSQL);
            createPart       = sysLobSession.compileStatement(createPartSQL);
        } finally {
            writeLock.unlock();
        }
    }

    private void initialiseLobSpace() {

        Statement statement = sysLobSession.compileStatement(existsBlocksSQL);
        Result    result    = statement.execute(sysLobSession);

        if (result.isError()) {
            throw result.getException();
        }

        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();

        if (size > 0) {
            return;
        }

        statement = sysLobSession.compileStatement(initialiseBlocksSQL);

        Object[] params = new Object[3];

        params[0] = ValuePool.INTEGER_0;
        params[1] = ValuePool.getInt(totalBlockLimitCount);
        params[2] = ValuePool.getLong(0);

        sysLobSession.executeCompiledStatement(statement, params, 0);
    }

    public void open() {

        lobBlockSize = database.logger.getLobBlockSize();
        cryptLobs    = database.logger.cryptLobs;
        compressLobs = database.logger.propCompressLobs;

        if (compressLobs || cryptLobs) {
            int largeBufferBlockSize = largeLobBlockSize + 4 * 1024;

            inflater   = new Inflater();
            deflater   = new Deflater(Deflater.BEST_SPEED);
            dataBuffer = new byte[largeBufferBlockSize];
        }

        if (database.getType() == DatabaseURL.S_RES) {
            lobStore = new LobStoreInJar(database, lobBlockSize);
        } else if (database.getType() == DatabaseURL.S_FILE) {
            lobStore = new LobStoreRAFile(database, lobBlockSize);

            if (!database.isFilesReadOnly()) {
                byteBuffer = new byte[lobBlockSize];

                initialiseLobSpace();
            }
        } else {
            lobStore   = new LobStoreMem(lobBlockSize);
            byteBuffer = new byte[lobBlockSize];

            initialiseLobSpace();
        }
    }

    public void close() {

        if (lobStore != null) {
            lobStore.close();
        }

        lobStore = null;
    }

    public LobStore getLobStore() {

        if (lobStore == null) {
            open();
        }

        return lobStore;
    }

    //
    private Long getNewLobID() {

        Result result = getNextLobId.execute(sysLobSession);

        if (result.isError()) {
            return Long.valueOf(0);
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();

        if (!next) {
            navigator.release();

            return Long.valueOf(0);
        }

        Object[] data = navigator.getCurrent();

        navigator.release();

        return (Long) data[0];
    }

    private Object[] getLobHeader(long lobID) {

        ResultMetaData meta     = getLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = ValuePool.getLong(lobID);

        sysLobSession.sessionContext.pushDynamicArguments(params);

        Result result = getLob.execute(sysLobSession);

        sysLobSession.sessionContext.pop();

        if (result.isError()) {
            throw result.getException();
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();
        Object[]        data      = null;

        if (next) {
            data = navigator.getCurrent();
        }

        navigator.release();

        return data;
    }

    public BlobData getBlob(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return null;
            }

            BlobData blob = new BlobDataID(lobID);

            return blob;
        } finally {
            writeLock.unlock();
        }
    }

    public ClobData getClob(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return null;
            }

            ClobData clob = new ClobDataID(lobID);

            return clob;
        } finally {
            writeLock.unlock();
        }
    }

    public long createBlob(Session session, long length) {

        writeLock.lock();

        try {
            Long           lobID    = getNewLobID();
            ResultMetaData meta     = createLob.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[LOB_IDS.LOB_ID]          = lobID;
            params[LOB_IDS.LOB_LENGTH]      = ValuePool.getLong(length);
            params[LOB_IDS.LOB_USAGE_COUNT] = ValuePool.INTEGER_0;
            params[LOB_IDS.LOB_TYPE]        = ValuePool.getInt(Types.SQL_BLOB);

            Result result = sysLobSession.executeCompiledStatement(createLob,
                params, 0);

            usageChanged = true;

            return lobID.longValue();
        } finally {
            writeLock.unlock();
        }
    }

    public long createClob(Session session, long length) {

        writeLock.lock();

        try {
            Long           lobID    = getNewLobID();
            ResultMetaData meta     = createLob.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[LOB_IDS.LOB_ID]          = lobID;
            params[LOB_IDS.LOB_LENGTH]      = ValuePool.getLong(length);
            params[LOB_IDS.LOB_USAGE_COUNT] = ValuePool.INTEGER_0;
            params[LOB_IDS.LOB_TYPE]        = ValuePool.getInt(Types.SQL_CLOB);

            Result result = sysLobSession.executeCompiledStatement(createLob,
                params, 0);

            usageChanged = true;

            return lobID.longValue();
        } finally {
            writeLock.unlock();
        }
    }

    public Result deleteLob(long lobID) {

        writeLock.lock();

        try {
            ResultMetaData meta     = deleteLobCall.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[0] = ValuePool.getLong(lobID);
            params[1] = ValuePool.getLong(0);

            Result result =
                sysLobSession.executeCompiledStatement(deleteLobCall, params,
                    0);

            usageChanged = true;

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result deleteUnusedLobs() {

        writeLock.lock();

        try {
            if (lobStore == null || byteBuffer == null || !usageChanged) {
                return Result.updateZeroResult;
            }

            Session[] sessions   = database.sessionManager.getAllSessions();
            long      firstLobID = Long.MAX_VALUE;

            for (int i = 0; i < sessions.length; i++) {
                if (sessions[i].isClosed()) {
                    continue;
                }

                long sessionLobID = sessions[i].sessionData.getFirstLobID();

                if (sessionLobID != 0 && sessionLobID < firstLobID) {
                    firstLobID = sessionLobID;
                }
            }

            Object params[] = new Object[1];

            params[0] = new Long(firstLobID);

            Result result =
                sysLobSession.executeCompiledStatement(deleteUnusedLobs,
                    params, 0);

            if (result.isError()) {
                return result;
            }

            result = sysLobSession.executeCompiledStatement(mergeUnusedSpace,
                    ValuePool.emptyObjectArray, 0);

            if (result.isError()) {
                return result;
            }

            result = sysLobSession.executeCompiledStatement(getLobUseLimit,
                    ValuePool.emptyObjectArray, 0);

            if (result.isError()) {
                return result;
            }

            usageChanged = false;

            RowSetNavigator navigator = result.getNavigator();
            boolean         next      = navigator.next();

            if (!next) {
                navigator.release();

                return Result.updateOneResult;
            }

            Object[] data = navigator.getCurrent();

            if (data[LOBS.BLOCK_ADDR] == null
                    || data[LOBS.BLOCK_COUNT] == null) {
                return Result.updateOneResult;
            }

            long sizeLimit = ((Integer) data[LOBS.BLOCK_ADDR]).intValue()
                             + ((Integer) data[LOBS.BLOCK_COUNT]).intValue();

            sizeLimit *= lobBlockSize;

            long currentLength = lobStore.getLength();

            if (currentLength > sizeLimit) {
                database.logger.logInfoEvent("lob file truncated to usage");
                lobStore.setLength(sizeLimit);

                try {
                    lobStore.synch();
                } catch (Throwable t) {}
            } else if (currentLength < sizeLimit) {
                database.logger.logInfoEvent(
                    "lob file reported smaller than usage");
            }

            return Result.updateOneResult;
        } finally {
            writeLock.unlock();
        }
    }

    public Result getLength(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                throw Error.error(ErrorCode.X_0F502);
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int  type   = ((Integer) data[LOB_IDS.LOB_TYPE]).intValue();

            return ResultLob.newLobSetResponse(lobID, length);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        } finally {
            writeLock.unlock();
        }
    }

    public int compare(BlobData a, byte[] b) {

        writeLock.lock();

        try {
            Object[] data    = getLobHeader(a.getId());
            long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int[][] aAddresses = getBlockAddresses(a.getId(), 0,
                                                   Integer.MAX_VALUE);
            int aIndex  = 0;
            int bOffset = 0;
            int aOffset = 0;

            if (aLength == 0) {
                return b.length == 0 ? 0
                                     : -1;
            }

            if (b.length == 0) {
                return 1;
            }

            while (true) {
                int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR]
                                   + aOffset;
                byte[] aBytes = getLobStore().getBlockBytes(aBlockOffset, 1);

                for (int i = 0; i < aBytes.length; i++) {
                    if (bOffset + i >= b.length) {
                        if (aLength == b.length) {
                            return 0;
                        }

                        return 1;
                    }

                    if (aBytes[i] == b[bOffset + i]) {
                        continue;
                    }

                    return (((int) aBytes[i]) & 0xff)
                           > (((int) b[bOffset + i]) & 0xff) ? 1
                                                             : -1;
                }

                aOffset++;

                bOffset += lobBlockSize;

                if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                    aOffset = 0;

                    aIndex++;
                }

                if (aIndex == aAddresses.length) {
                    break;
                }

                if (bOffset >= b.length) {
                    break;
                }
            }

            if (aLength == b.length) {
                return 0;
            }

            return aLength > b.length ? 1
                                      : -1;
        } finally {
            writeLock.unlock();
        }
    }

    public int compare(BlobData a, BlobData b) {

        if (a.getId() == b.getId()) {
            return 0;
        }

        writeLock.lock();

        try {
            if (compressLobs || cryptLobs) {
                return compareBytesCompressed(a.getId(), b.getId());
            } else {
                return compareBytesNormal(a.getId(), b.getId());
            }
        } finally {
            writeLock.unlock();
        }
    }

    /** @todo - implement as compareText() */
    public int compare(Collation collation, ClobData a, String b) {

        writeLock.lock();

        try {
            Object[] data    = getLobHeader(a.getId());
            long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int[][] aAddresses = getBlockAddresses(a.getId(), 0,
                                                   Integer.MAX_VALUE);
            int aIndex  = 0;
            int bOffset = 0;
            int aOffset = 0;

            if (aLength == 0) {
                return b.length() == 0 ? 0
                                       : -1;
            }

            if (b.length() == 0) {
                return 1;
            }

            while (true) {
                int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR]
                                   + aOffset;
                byte[] aBytes = getLobStore().getBlockBytes(aBlockOffset, 1);
                long aLimit =
                    aLength
                    - ((long) aAddresses[aIndex][LOBS.BLOCK_OFFSET] + aOffset)
                      * lobBlockSize / 2;

                if (aLimit > lobBlockSize / 2) {
                    aLimit = lobBlockSize / 2;
                }

                String aString = new String(ArrayUtil.byteArrayToChars(aBytes),
                                            0, (int) aLimit);
                int bLimit = b.length() - bOffset;

                if (bLimit > lobBlockSize / 2) {
                    bLimit = lobBlockSize / 2;
                }

                String bString = b.substring(bOffset, bOffset + bLimit);
                int    diff    = collation.compare(aString, bString);

                if (diff != 0) {
                    return diff;
                }

                aOffset++;

                bOffset += lobBlockSize / 2;

                if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                    aOffset = 0;

                    aIndex++;
                }

                if (aIndex == aAddresses.length) {
                    break;
                }

                if (bOffset >= b.length()) {
                    break;
                }
            }

            if (aLength == b.length()) {
                return 0;
            }

            return aLength > b.length() ? 1
                                        : -1;
        } finally {
            writeLock.unlock();
        }
    }

    public int compare(Collation collation, ClobData a, ClobData b) {

        if (a.getId() == b.getId()) {
            return 0;
        }

        writeLock.lock();

        try {
            if (compressLobs || cryptLobs) {
                return compareTextCompressed(collation, a.getId(), b.getId());
            } else {
                return compareTextNormal(collation, a.getId(), b.getId());
            }
        } finally {
            writeLock.unlock();
        }
    }

    private int compareBytesNormal(long aID, long bID) {

        Object[] data    = getLobHeader(aID);
        long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

        data = getLobHeader(bID);

        long    bLength    = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
        int[][] aAddresses = getBlockAddresses(aID, 0, Integer.MAX_VALUE);
        int[][] bAddresses = getBlockAddresses(bID, 0, Integer.MAX_VALUE);
        int     aIndex     = 0;
        int     bIndex     = 0;
        int     aOffset    = 0;
        int     bOffset    = 0;

        if (aLength == 0) {
            return bLength == 0 ? 0
                                : -1;
        }

        if (bLength == 0) {
            return 1;
        }

        while (true) {
            int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR] + aOffset;
            int bBlockOffset = bAddresses[bIndex][LOBS.BLOCK_ADDR] + bOffset;
            byte[] aBytes    = getLobStore().getBlockBytes(aBlockOffset, 1);
            byte[] bBytes    = getLobStore().getBlockBytes(bBlockOffset, 1);
            int    result    = ArrayUtil.compare(aBytes, bBytes);

            if (result != 0) {
                return result;
            }

            aOffset++;
            bOffset++;

            if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                aOffset = 0;

                aIndex++;
            }

            if (bOffset == bAddresses[bIndex][LOBS.BLOCK_COUNT]) {
                bOffset = 0;

                bIndex++;
            }

            if (aIndex == aAddresses.length) {
                break;
            }

            if (bIndex == bAddresses.length) {
                break;
            }
        }

        if (aLength == bLength) {
            return 0;
        }

        return aLength > bLength ? 1
                                 : -1;
    }

    /** @todo - word-separator and end block zero issues */
    private int compareTextNormal(Collation collation, long aID, long bID) {

        Object[] data    = getLobHeader(aID);
        long     aLength = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

        data = getLobHeader(bID);

        long    bLength    = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
        int[][] aAddresses = getBlockAddresses(aID, 0, Integer.MAX_VALUE);
        int[][] bAddresses = getBlockAddresses(bID, 0, Integer.MAX_VALUE);
        int     aIndex     = 0;
        int     bIndex     = 0;
        int     aOffset    = 0;
        int     bOffset    = 0;

        if (aLength == 0) {
            return bLength == 0 ? 0
                                : -1;
        }

        if (bLength == 0) {
            return 1;
        }

        while (true) {
            int aBlockOffset = aAddresses[aIndex][LOBS.BLOCK_ADDR] + aOffset;
            int bBlockOffset = bAddresses[bIndex][LOBS.BLOCK_ADDR] + bOffset;
            byte[] aBytes    = getLobStore().getBlockBytes(aBlockOffset, 1);
            byte[] bBytes    = getLobStore().getBlockBytes(bBlockOffset, 1);
            long aLimit =
                aLength
                - ((long) aAddresses[aIndex][LOBS.BLOCK_OFFSET] + aOffset)
                  * lobBlockSize / 2;

            if (aLimit > lobBlockSize / 2) {
                aLimit = lobBlockSize / 2;
            }

            long bLimit =
                bLength
                - ((long) bAddresses[bIndex][LOBS.BLOCK_OFFSET] + bOffset)
                  * lobBlockSize / 2;

            if (bLimit > lobBlockSize / 2) {
                bLimit = lobBlockSize / 2;
            }

            String aString = new String(ArrayUtil.byteArrayToChars(aBytes), 0,
                                        (int) aLimit);
            String bString = new String(ArrayUtil.byteArrayToChars(bBytes), 0,
                                        (int) bLimit);
            int diff = collation.compare(aString, bString);

            if (diff != 0) {
                return diff;
            }

            aOffset++;
            bOffset++;

            if (aOffset == aAddresses[aIndex][LOBS.BLOCK_COUNT]) {
                aOffset = 0;

                aIndex++;
            }

            if (bOffset == bAddresses[bIndex][LOBS.BLOCK_COUNT]) {
                bOffset = 0;

                bIndex++;
            }

            if (aIndex == aAddresses.length) {
                break;
            }

            if (bIndex == bAddresses.length) {
                break;
            }
        }

        if (aLength == bLength) {
            return 0;
        }

        return aLength > bLength ? 1
                                 : -1;
    }

    /**
     * Used for SUBSTRING
     */
    public Result getLob(long lobID, long offset, long length) {

        if (offset == 0) {
            return createDuplicateLob(lobID, length);
        }

        throw Error.runtimeError(ErrorCode.U_S0500, "LobManager");
    }

    public Result createDuplicateLob(long lobID) {

        Result result = getLength(lobID);

        if (result.isError()) {
            return result;
        }

        return createDuplicateLob(lobID,
                                  ((ResultLob) result).getBlockLength());
    }

    public Result createDuplicateLob(long lobID, long newLength) {

        if (byteBuffer == null) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            Long   newLobID = getNewLobID();
            Object params[] = new Object[data.length];

            params[LOB_IDS.LOB_ID]          = newLobID;
            params[LOB_IDS.LOB_LENGTH]      = Long.valueOf(newLength);
            params[LOB_IDS.LOB_USAGE_COUNT] = ValuePool.INTEGER_0;
            params[LOB_IDS.LOB_TYPE]        = data[LOB_IDS.LOB_TYPE];

            Result result = sysLobSession.executeCompiledStatement(createLob,
                params, 0);

            if (result.isError()) {
                return result;
            }

            usageChanged = true;

            if (newLength == 0) {
                return ResultLob.newLobSetResponse(newLobID.longValue(),
                                                   newLength);
            }

            long byteLength = newLength;
            int  lobType    = ((Integer) data[LOB_IDS.LOB_TYPE]).intValue();

            if (lobType == Types.SQL_CLOB) {
                byteLength *= 2;
            }

            int newBlockCount = (int) (byteLength / lobBlockSize);

            if (byteLength % lobBlockSize != 0) {
                newBlockCount++;
            }

            createBlockAddresses(newLobID.longValue(), 0, newBlockCount);

            // copy the contents
            int[][] sourceBlocks = getBlockAddresses(lobID, 0,
                Integer.MAX_VALUE);
            int[][] targetBlocks = getBlockAddresses(newLobID.longValue(), 0,
                Integer.MAX_VALUE);

            try {
                copyBlockSet(sourceBlocks, targetBlocks);
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            // clear the end block unused space
            int endOffset = (int) (byteLength % lobBlockSize);

            if (endOffset != 0) {
                int[] block = targetBlocks[targetBlocks.length - 1];
                int blockOffset = block[LOBS.BLOCK_ADDR]
                                  + block[LOBS.BLOCK_COUNT] - 1;
                byte[] bytes = getLobStore().getBlockBytes(blockOffset, 1);

                ArrayUtil.fillArray(bytes, endOffset, (byte) 0);
                getLobStore().setBlockBytes(bytes, blockOffset, 1);
            }

            return ResultLob.newLobSetResponse(newLobID.longValue(),
                                               newLength);
        } finally {
            writeLock.unlock();
        }
    }

    /** @todo - currently unused and returns whole length */
    public Result getTruncateLength(long lobID) {

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                throw Error.error(ErrorCode.X_0F502);
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            int  type   = ((Integer) data[LOB_IDS.LOB_TYPE]).intValue();

            return ResultLob.newLobSetResponse(lobID, length);
        } finally {
            writeLock.unlock();
        }
    }

    private void copyBlockSet(int[][] source, int[][] target) {

        int sourceIndex  = 0;
        int targetIndex  = 0;
        int sourceOffset = 0;
        int targetOffset = 0;

        while (true) {
            byte[] bytes = getLobStore().getBlockBytes(
                source[sourceIndex][LOBS.BLOCK_ADDR] + sourceOffset, 1);

            getLobStore().setBlockBytes(bytes,
                                        target[targetIndex][LOBS.BLOCK_ADDR]
                                        + targetOffset, 1);

            sourceOffset++;
            targetOffset++;

            if (sourceOffset == source[sourceIndex][LOBS.BLOCK_COUNT]) {
                sourceOffset = 0;

                sourceIndex++;
            }

            if (targetOffset == target[targetIndex][LOBS.BLOCK_COUNT]) {
                targetOffset = 0;

                targetIndex++;
            }

            if (sourceIndex == source.length) {
                break;
            }

            if (targetIndex == target.length) {
                break;
            }
        }

        storeModified = true;
    }

    public Result getChars(long lobID, long offset, int length) {

        Result result;

        writeLock.lock();

        try {
            if (compressLobs || cryptLobs) {
                result = getBytesCompressed(lobID, offset * 2, length * 2,
                                            true);
            } else {
                result = getBytesNormal(lobID, offset * 2, length * 2);
            }
        } finally {
            writeLock.unlock();
        }

        if (result.isError()) {
            return result;
        }

        byte[] bytes = ((ResultLob) result).getByteArray();
        char[] chars = ArrayUtil.byteArrayToChars(bytes);

        return ResultLob.newLobGetCharsResponse(lobID, offset, chars);
    }

    public Result getBytes(long lobID, long offset, int length) {

        writeLock.lock();

        try {
            if (compressLobs || cryptLobs) {
                return getBytesCompressed(lobID, offset, length, false);
            } else {
                return getBytesNormal(lobID, offset, length);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private Result getBytesNormal(long lobID, long offset, int length) {

        int blockOffset     = (int) (offset / lobBlockSize);
        int byteBlockOffset = (int) (offset % lobBlockSize);
        int blockLimit      = (int) ((offset + length) / lobBlockSize);
        int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        if (length == 0) {
            return ResultLob.newLobGetBytesResponse(lobID, offset,
                    BinaryData.zeroLengthBytes);
        }

        int    dataBytesPosition = 0;
        byte[] dataBytes         = new byte[length];
        int[][] blockAddresses = getBlockAddresses(lobID, blockOffset,
            blockLimit);

        if (blockAddresses.length == 0) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        //
        int i = 0;
        int blockCount = blockAddresses[i][LOBS.BLOCK_COUNT]
                         + blockAddresses[i][LOBS.BLOCK_OFFSET] - blockOffset;

        if (blockAddresses[i][LOBS.BLOCK_COUNT]
                + blockAddresses[i][LOBS.BLOCK_OFFSET] > blockLimit) {
            blockCount -= (blockAddresses[i][LOBS.BLOCK_COUNT]
                           + blockAddresses[i][LOBS.BLOCK_OFFSET]
                           - blockLimit);
        }

        byte[] bytes;

        try {
            bytes = getLobStore().getBlockBytes(
                blockAddresses[i][LOBS.BLOCK_ADDR]
                - blockAddresses[i][LOBS.BLOCK_OFFSET]
                + blockOffset, blockCount);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        int subLength = lobBlockSize * blockCount - byteBlockOffset;

        if (subLength > length) {
            subLength = length;
        }

        System.arraycopy(bytes, byteBlockOffset, dataBytes, dataBytesPosition,
                         subLength);

        dataBytesPosition += subLength;

        i++;

        for (; i < blockAddresses.length && dataBytesPosition < length; i++) {
            blockCount = blockAddresses[i][LOBS.BLOCK_COUNT];

            if (blockAddresses[i][LOBS.BLOCK_COUNT]
                    + blockAddresses[i][LOBS.BLOCK_OFFSET] > blockLimit) {
                blockCount -= (blockAddresses[i][LOBS.BLOCK_COUNT]
                               + blockAddresses[i][LOBS.BLOCK_OFFSET]
                               - blockLimit);
            }

            try {
                bytes = getLobStore().getBlockBytes(
                    blockAddresses[i][LOBS.BLOCK_ADDR], blockCount);
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            subLength = lobBlockSize * blockCount;

            if (subLength > length - dataBytesPosition) {
                subLength = length - dataBytesPosition;
            }

            System.arraycopy(bytes, 0, dataBytes, dataBytesPosition,
                             subLength);

            dataBytesPosition += subLength;
        }

        return ResultLob.newLobGetBytesResponse(lobID, offset, dataBytes);
    }

    private Result setBytesBA(long lobID, long offset, byte[] dataBytes,
                              int dataLength, boolean isClob) {

        if (dataLength == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        writeLock.lock();

        try {
            if (compressLobs || cryptLobs) {
                return setBytesBACompressed(lobID, offset, dataBytes,
                                            dataLength, isClob);
            } else {
                return setBytesBANormal(lobID, offset, dataBytes, dataLength);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private Result setBytesBANormal(long lobID, long offset, byte[] dataBytes,
                                    int dataLength) {

        boolean newBlocks       = false;
        int     blockOffset     = (int) (offset / lobBlockSize);
        int     byteBlockOffset = (int) (offset % lobBlockSize);
        int     blockLimit      = (int) ((offset + dataLength) / lobBlockSize);
        int     byteLimitOffset = (int) ((offset + dataLength) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        int[][] blockAddresses = getBlockAddresses(lobID, blockOffset,
            blockLimit);
        int existingLimit = blockOffset;

        if (blockAddresses.length > 0) {
            existingLimit =
                blockAddresses[blockAddresses.length - 1][LOBS.BLOCK_OFFSET]
                + blockAddresses[blockAddresses.length - 1][LOBS.BLOCK_COUNT];
        }

        if (existingLimit < blockLimit) {
            createBlockAddresses(lobID, existingLimit,
                                 blockLimit - existingLimit);

            blockAddresses = getBlockAddresses(lobID, blockOffset, blockLimit);
            newBlocks      = true;
        }

        int currentDataOffset = 0;
        int currentDataLength = dataLength;

        try {
            for (int i = 0; i < blockAddresses.length; i++) {
                long currentBlockOffset =
                    (long) blockAddresses[i][LOBS.BLOCK_OFFSET] * lobBlockSize;
                long currentBlockLength =
                    (long) blockAddresses[i][LOBS.BLOCK_COUNT] * lobBlockSize;
                long currentBlockPosition =
                    (long) blockAddresses[i][LOBS.BLOCK_ADDR] * lobBlockSize;
                int padding = 0;

                if (offset > currentBlockOffset) {
                    currentBlockLength   -= (offset - currentBlockOffset);
                    currentBlockPosition += (offset - currentBlockOffset);
                }

                if (currentDataLength < currentBlockLength) {
                    if (newBlocks) {
                        padding =
                            (int) ((currentBlockLength - currentDataLength)
                                   % lobBlockSize);
                    }

                    currentBlockLength = currentDataLength;
                }

                getLobStore().setBlockBytes(dataBytes, currentBlockPosition,
                                            currentDataOffset,
                                            (int) currentBlockLength);

                if (padding != 0) {
                    ArrayUtil.fillArray(byteBuffer, 0, (byte) 0);
                    getLobStore().setBlockBytes(byteBuffer,
                                                currentBlockPosition
                                                + currentBlockLength, 0,
                                                    padding);
                }

                currentDataOffset += currentBlockLength;
                currentDataLength -= currentBlockLength;
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        storeModified = true;

        return ResultLob.newLobSetResponse(lobID, dataLength);
    }

    private Result setBytesIS(long lobID, InputStream inputStream,
                              long length, boolean isClob) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        if (compressLobs || cryptLobs) {
            return setBytesISCompressed(lobID, inputStream, length, isClob);
        } else {
            return setBytesISNormal(lobID, inputStream, length);
        }
    }

    private Result setBytesISNormal(long lobID, InputStream inputStream,
                                    long length) {

        long writeLength     = 0;
        int  blockLimit      = (int) (length / lobBlockSize);
        int  byteLimitOffset = (int) (length % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        createBlockAddresses(lobID, 0, blockLimit);

        int[][] blockAddresses = getBlockAddresses(lobID, 0, blockLimit);

        for (int i = 0; i < blockAddresses.length; i++) {
            for (int j = 0; j < blockAddresses[i][LOBS.BLOCK_COUNT]; j++) {
                int localLength = lobBlockSize;

                ArrayUtil.fillArray(byteBuffer, 0, (byte) 0);

                if (i == blockAddresses.length - 1
                        && j == blockAddresses[i][LOBS.BLOCK_COUNT] - 1) {
                    localLength = byteLimitOffset;
                }

                try {
                    int count = 0;

                    while (localLength > 0) {
                        int read = inputStream.read(byteBuffer, count,
                                                    localLength);

                        if (read == -1) {
                            return Result.newErrorResult(new EOFException());
                        }

                        localLength -= read;
                        count       += read;
                    }

                    writeLength += count;

                    // read more
                } catch (IOException e) {

                    // deallocate
                    return Result.newErrorResult(e);
                }

                try {
                    getLobStore().setBlockBytes(
                        byteBuffer, blockAddresses[i][LOBS.BLOCK_ADDR] + j, 1);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
        }

        storeModified = true;

        return ResultLob.newLobSetResponse(lobID, writeLength);
    }

    /**
     * returns new LOB length
     */
    public Result setBytes(long lobID, long offset, byte[] dataBytes,
                           int dataLength) {

        if (byteBuffer == null) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

            if (dataLength == 0) {
                return ResultLob.newLobSetResponse(lobID, length);
            }

            Result result = setBytesBA(lobID, offset, dataBytes, dataLength,
                                       false);

            if (result.isError()) {
                return result;
            }

            if (offset + dataLength > length) {
                length = offset + dataLength;
                result = setLength(lobID, length);

                if (result.isError()) {
                    return result;
                }
            }

            return ResultLob.newLobSetResponse(lobID, length);
        } finally {
            writeLock.unlock();
        }
    }

    public Result setBytesForNewBlob(long lobID, InputStream inputStream,
                                     long length) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        if (byteBuffer == null) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        writeLock.lock();

        try {
            Result result = setBytesIS(lobID, inputStream, length, false);

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * returns new full length of Clob
     */
    public Result setChars(long lobID, long offset, char[] chars,
                           int dataLength) {

        if (byteBuffer == null) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long length = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();

            if (dataLength == 0) {
                return ResultLob.newLobSetResponse(lobID, length);
            }

            byte[] bytes = ArrayUtil.charArrayToBytes(chars, dataLength);
            Result result = setBytesBA(lobID, offset * 2, bytes,
                                       dataLength * 2, true);

            if (result.isError()) {
                return result;
            }

            if (offset + dataLength > length) {
                length = offset + dataLength;
                result = setLength(lobID, length);

                if (result.isError()) {
                    return result;
                }
            }

            return ResultLob.newLobSetResponse(lobID, length);
        } finally {
            writeLock.unlock();
        }
    }

    public Result setCharsForNewClob(long lobID, InputStream inputStream,
                                     long length) {

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        if (byteBuffer == null) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        writeLock.lock();

        try {
            Result result = setBytesIS(lobID, inputStream, length * 2, false);

            if (result.isError()) {
                return result;
            }

            long newLength = ((ResultLob) result).getBlockLength();

            if (newLength < length) {
                Result trunc = truncate(lobID, newLength);
            }

            return result;
        } finally {
            writeLock.unlock();
        }
    }

    public Result truncate(long lobID, long offset) {

        if (byteBuffer == null) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }

        writeLock.lock();

        try {
            Object[] data = getLobHeader(lobID);

            if (data == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
            }

            long length     = ((Long) data[LOB_IDS.LOB_LENGTH]).longValue();
            long byteLength = offset;

            if (((Integer) data[LOB_IDS.LOB_TYPE]).intValue()
                    == Types.SQL_CLOB) {
                byteLength *= 2;
            }

            int blockOffset = (int) ((byteLength + lobBlockSize - 1)
                                     / lobBlockSize);
            ResultMetaData meta = deleteLobPartCall.getParametersMetaData();
            Object         params[] = new Object[meta.getColumnCount()];

            params[DELETE_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);
            params[DELETE_BLOCKS.BLOCK_OFFSET] = Integer.valueOf(blockOffset);
            params[DELETE_BLOCKS.BLOCK_LIMIT]  = ValuePool.INTEGER_MAX;
            params[DELETE_BLOCKS.TX_ID] =
                ValuePool.getLong(sysLobSession.getTransactionTimestamp());

            Result result =
                sysLobSession.executeCompiledStatement(deleteLobPartCall,
                    params, 0);

            setLength(lobID, offset);

            return ResultLob.newLobTruncateResponse(lobID, offset);
        } finally {
            writeLock.unlock();
        }
    }

    private Result setLength(long lobID, long length) {

        ResultMetaData meta     = updateLobLength.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[UPDATE_LENGTH.LOB_LENGTH] = ValuePool.getLong(length);
        params[UPDATE_LENGTH.LOB_ID]     = ValuePool.getLong(lobID);

        Result result = sysLobSession.executeCompiledStatement(updateLobLength,
            params, 0);

        return result;
    }

    /**
     * Executes in user session. No lock
     */
    public Result adjustUsageCount(Session session, long lobID, int delta) {

        ResultMetaData meta     = updateLobUsage.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[UPDATE_USAGE.BLOCK_COUNT] = ValuePool.getInt(delta);
        params[UPDATE_USAGE.LOB_ID]      = ValuePool.getLong(lobID);

        session.sessionContext.pushDynamicArguments(params);

        Result result = updateLobUsage.execute(session);

        session.sessionContext.pop();

        return result;
    }

    private int[][] getBlockAddresses(long lobID, int offset, int limit) {

        ResultMetaData meta     = getSpanningBlocks.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[GET_LOB_PART.LOB_ID]       = ValuePool.getLong(lobID);
        params[GET_LOB_PART.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[GET_LOB_PART.BLOCK_LIMIT]  = ValuePool.getInt(limit);

        sysLobSession.sessionContext.pushDynamicArguments(params);

        Result result = getSpanningBlocks.execute(sysLobSession);

        sysLobSession.sessionContext.pop();

        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        int[][]         blocks    = new int[size][3];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = navigator.getCurrent();

            blocks[i][LOBS.BLOCK_ADDR] =
                ((Integer) data[LOBS.BLOCK_ADDR]).intValue();
            blocks[i][LOBS.BLOCK_COUNT] =
                ((Integer) data[LOBS.BLOCK_COUNT]).intValue();
            blocks[i][LOBS.BLOCK_OFFSET] =
                ((Integer) data[LOBS.BLOCK_OFFSET]).intValue();
        }

        navigator.release();

        return blocks;
    }

    private void deleteBlockAddresses(long lobID, int offset, int limit) {

        ResultMetaData meta     = deleteLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[DELETE_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);
        params[DELETE_BLOCKS.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[DELETE_BLOCKS.BLOCK_LIMIT]  = ValuePool.getInt(limit);
        params[DELETE_BLOCKS.TX_ID] =
            ValuePool.getLong(sysLobSession.getTransactionTimestamp());

        Result result =
            sysLobSession.executeCompiledStatement(deleteLobPartCall, params,
                0);
    }

    private void divideBlockAddresses(long lobID, int offset) {

        ResultMetaData meta     = divideLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[DIVIDE_BLOCK.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[DIVIDE_BLOCK.LOB_ID]       = ValuePool.getLong(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(divideLobPartCall, params,
                0);
    }

    private Result createBlockAddresses(long lobID, int offset, int count) {

        ResultMetaData meta     = createLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[ALLOC_BLOCKS.BLOCK_COUNT]  = ValuePool.getInt(count);
        params[ALLOC_BLOCKS.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[ALLOC_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(createLobPartCall, params,
                0);

        return result;
    }

    private Result createFullBlockAddresses(long lobID, int offset,
            int count) {

        ResultMetaData meta = createSingleLobPartCall.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[ALLOC_BLOCKS.BLOCK_COUNT]  = ValuePool.getInt(count);
        params[ALLOC_BLOCKS.BLOCK_OFFSET] = ValuePool.getInt(offset);
        params[ALLOC_BLOCKS.LOB_ID]       = ValuePool.getLong(lobID);

        Result result =
            sysLobSession.executeCompiledStatement(createSingleLobPartCall,
                params, 0);

        return result;
    }

    private Result createPart(long lobID, long partOffset, int dataLength,
                              int byteLength, int blockOffset,
                              int blockCount) {

        ResultMetaData meta     = createPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[ALLOC_PART.BLOCK_COUNT]  = ValuePool.getInt(blockCount);
        params[ALLOC_PART.BLOCK_OFFSET] = ValuePool.getInt(blockOffset);
        params[ALLOC_PART.PART_OFFSET]  = ValuePool.getLong(partOffset);
        params[ALLOC_PART.PART_LENGTH]  = ValuePool.getLong(dataLength);
        params[ALLOC_PART.PART_BYTES]   = ValuePool.getLong(byteLength);
        params[ALLOC_PART.LOB_ID]       = ValuePool.getLong(lobID);

        Result result = sysLobSession.executeCompiledStatement(createPart,
            params, 0);

        return result;
    }

    private int getBlockAddress(int[][] blockAddresses, int blockOffset) {

        for (int i = 0; i < blockAddresses.length; i++) {
            if (blockAddresses[i][LOBS.BLOCK_OFFSET]
                    + blockAddresses[i][LOBS.BLOCK_COUNT] > blockOffset) {
                return blockAddresses[i][LOBS.BLOCK_ADDR]
                       - blockAddresses[i][LOBS.BLOCK_OFFSET] + blockOffset;
            }
        }

        return -1;
    }

    public int getLobCount() {

        writeLock.lock();

        try {
            sysLobSession.sessionContext.pushDynamicArguments(new Object[]{});

            Result result = getLobCount.execute(sysLobSession);

            sysLobSession.sessionContext.pop();

            RowSetNavigator navigator = result.getNavigator();
            boolean         next      = navigator.next();

            if (!next) {
                navigator.release();

                return 0;
            }

            Object[] data = navigator.getCurrent();

            return ((Number) data[0]).intValue();
        } finally {
            writeLock.unlock();
        }
    }

    public void synch() {

        if (storeModified) {
            if (lobStore != null) {
                writeLock.lock();

                try {
                    try {
                        lobStore.synch();
                    } catch (Throwable t) {}

                    storeModified = false;
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }

    private long[][] getParts(long lobID, long offset, long limit) {

        ResultMetaData meta     = getSpanningParts.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[GET_LOB_PART.LOB_ID]       = ValuePool.getLong(lobID);
        params[GET_LOB_PART.BLOCK_OFFSET] = ValuePool.getLong(offset);
        params[GET_LOB_PART.BLOCK_LIMIT]  = ValuePool.getLong(limit);

        sysLobSession.sessionContext.pushDynamicArguments(params);

        Result result = getSpanningParts.execute(sysLobSession);

        sysLobSession.sessionContext.pop();

        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        long[][]        blocks    = new long[size][6];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = navigator.getCurrent();

            for (int j = 0; j < blocks[i].length; j++) {
                blocks[i][j] = ((Number) data[j]).longValue();
            }
        }

        navigator.release();

        return blocks;
    }

    void inflate(byte[] data, int length, boolean isClob) {

        if (cryptLobs) {
            length = database.logger.getCrypto().decode(data, 0, length, data,
                    0);
        }

        try {
            inflater.setInput(data, 0, length);

            length = inflater.inflate(dataBuffer);

            inflater.reset();
        } catch (DataFormatException e) {

            //
        } catch (Throwable e) {}

        int limit = (int) ArrayUtil.getBinaryMultipleCeiling(length,
            lobBlockSize);

        for (int i = length; i < limit; i++) {
            dataBuffer[i] = 0;
        }
    }

    int deflate(byte[] data, int offset, int length, boolean isClob) {

        deflater.setInput(data, offset, length);
        deflater.finish();

        length = deflater.deflate(dataBuffer);

        deflater.reset();

        if (cryptLobs) {
            length = database.logger.getCrypto().encode(dataBuffer, 0, length,
                    dataBuffer, 0);
        }

        int limit = (int) ArrayUtil.getBinaryMultipleCeiling(length,
            lobBlockSize);

        for (int i = length; i < limit; i++) {
            dataBuffer[i] = 0;
        }

        return length;
    }

    private int compareBytesCompressed(long aID, long bID) {

        long[][] aParts = getParts(aID, 0, Long.MAX_VALUE);
        long[][] bParts = getParts(bID, 0, Long.MAX_VALUE);

        for (int i = 0; i < aParts.length && i < bParts.length; i++) {
            int aPartLength = (int) aParts[i][ALLOC_PART.PART_LENGTH];

            getPartBytesCompressedInBuffer(aID, aParts[i], false);

            byte[] aPartBytes = new byte[aPartLength];

            System.arraycopy(dataBuffer, 0, aPartBytes, 0, aPartLength);

            int bPartLength = (int) bParts[i][ALLOC_PART.PART_LENGTH];

            getPartBytesCompressedInBuffer(aID, bParts[i], false);

            int result = ArrayUtil.compare(aPartBytes, 0, aPartLength,
                                           byteBuffer, bPartLength);

            if (result != 0) {
                return result;
            }
        }

        if (aParts.length == bParts.length) {
            return 0;
        }

        return aParts.length > bParts.length ? 1
                                             : -1;
    }

    private int compareTextCompressed(Collation collation, long aID,
                                      long bID) {

        long[][] aParts = getParts(aID, 0, Long.MAX_VALUE);
        long[][] bParts = getParts(bID, 0, Long.MAX_VALUE);

        for (int i = 0; i < aParts.length && i < bParts.length; i++) {
            int aPartLength = (int) aParts[i][ALLOC_PART.PART_LENGTH];

            getPartBytesCompressedInBuffer(aID, aParts[i], true);

            String aString = new String(ArrayUtil.byteArrayToChars(dataBuffer,
                aPartLength));
            int bPartLength = (int) bParts[i][ALLOC_PART.PART_LENGTH];

            getPartBytesCompressedInBuffer(bID, bParts[i], true);

            String bString = new String(ArrayUtil.byteArrayToChars(dataBuffer,
                bPartLength));
            int diff = collation.compare(aString, bString);

            if (diff != 0) {
                return diff;
            }
        }

        if (aParts.length == bParts.length) {
            return 0;
        }

        return aParts.length > bParts.length ? 1
                                             : -1;
    }

    private Result setBytesISCompressed(long lobID, InputStream inputStream,
                                        long length, boolean isClob) {

        long   offset = 0;
        byte[] tempBuffer;

        if (length < largeLobBlockSize) {
            tempBuffer = new byte[(int) length];
        } else {
            tempBuffer = new byte[largeLobBlockSize];
        }

        while (true) {
            int localLength = tempBuffer.length;

            if (localLength > length - offset) {
                localLength = (int) (length - offset);
            }

            int count = 0;

            try {
                while (localLength > 0) {
                    int read = inputStream.read(tempBuffer, count,
                                                localLength);

                    if (read == -1) {
                        return Result.newErrorResult(new EOFException());
                    }

                    localLength -= read;
                    count       += read;
                }

                // read more
            } catch (IOException e) {

                // deallocate
                return Result.newErrorResult(e);
            }

            Result result = setBytesBACompressedPart(lobID, offset,
                tempBuffer, count, isClob);

            if (result.isError()) {
                return result;
            }

            offset += count;

            if (offset == length) {
                break;
            }
        }

        storeModified = true;

        return ResultLob.newLobSetResponse(lobID, length);
    }

    private Result setBytesBACompressed(long lobID, long offset,
                                        byte[] dataBytes, int dataLength,
                                        boolean isClob) {

        if (dataLength == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        if (dataLength <= largeLobBlockSize) {
            return setBytesBACompressedPart(lobID, offset, dataBytes,
                                            dataLength, isClob);
        }

        HsqlByteArrayInputStream is = new HsqlByteArrayInputStream(dataBytes,
            0, dataLength);

        return setBytesISCompressed(lobID, is, dataLength, isClob);
    }

    private Result setBytesBACompressedPart(long lobID, long offset,
            byte[] dataBytes, int dataLength, boolean isClob) {

        // get block offset after existing blocks and conmpressed block
        int    byteLength = deflate(dataBytes, 0, dataLength, isClob);
        long[] lastPart   = getLastPart(lobID);
        int blockOffset = (int) lastPart[ALLOC_PART.BLOCK_OFFSET]
                          + (int) lastPart[ALLOC_PART.BLOCK_COUNT];
        int blockCount = (byteLength + lobBlockSize - 1) / lobBlockSize;

        // check position
        long limit = lastPart[ALLOC_PART.PART_OFFSET]
                     + lastPart[ALLOC_PART.PART_LENGTH];

        if (limit != offset || limit % largeLobBlockSize != 0) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        Result result = createFullBlockAddresses(lobID, blockOffset,
            blockCount);

        if (result.isError()) {
            return result;
        }

        result = createPart(lobID, offset, dataLength, byteLength,
                            blockOffset, blockCount);

        if (result.isError()) {
            return result;
        }

        long blockByteOffset = blockOffset * lobBlockSize;
        int blockByteLength =
            (int) ArrayUtil.getBinaryMultipleCeiling(byteLength, lobBlockSize);

        setBytesBANormal(lobID, blockByteOffset, dataBuffer, blockByteLength);

        storeModified = true;

        return ResultLob.newLobSetResponse(lobID, dataLength);
    }

    private Result getBytesCompressed(long lobID, long offset, int length,
                                      boolean isClob) {

        byte[]   dataBytes = new byte[length];
        long[][] parts     = getParts(lobID, offset, offset + length);

        for (int i = 0; i < parts.length; i++) {
            long[] part       = parts[i];
            long   partOffset = part[ALLOC_PART.PART_OFFSET];
            int    partLength = (int) part[ALLOC_PART.PART_LENGTH];
            Result result = getPartBytesCompressedInBuffer(lobID, part,
                isClob);

            if (result.isError()) {
                return result;
            }

            ArrayUtil.copyBytes(partOffset, dataBuffer, 0, partLength, offset,
                                dataBytes, length);
        }

        return ResultLob.newLobGetBytesResponse(lobID, offset, dataBytes);
    }

    private Result getPartBytesCompressedInBuffer(long lobID, long[] part,
            boolean isClob) {

        int  blockOffset     = (int) part[ALLOC_PART.BLOCK_OFFSET];
        long partOffset      = part[ALLOC_PART.PART_OFFSET];
        int  partLength      = (int) part[ALLOC_PART.PART_LENGTH];
        int  partBytesLength = (int) part[ALLOC_PART.PART_BYTES];
        long blockByteOffset = blockOffset * lobBlockSize;
        Result result = getBytesNormal(lobID, blockByteOffset,
                                       partBytesLength);

        if (result.isError()) {
            return result;
        }

        byte[] byteBlock = ((ResultLob) result).getByteArray();

        inflate(byteBlock, partBytesLength, isClob);

        return ResultLob.newLobSetResponse(lobID, partLength);
    }

    private long[] getLastPart(long lobID) {

        ResultMetaData meta     = getLastPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[GET_LOB_PART.LOB_ID] = ValuePool.getLong(lobID);

        sysLobSession.sessionContext.pushDynamicArguments(params);

        Result result = getLastPart.execute(sysLobSession);

        sysLobSession.sessionContext.pop();

        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        long[]          blocks    = new long[6];

        if (size == 0) {
            blocks[ALLOC_PART.LOB_ID] = lobID;
        } else {
            navigator.absolute(0);

            Object[] data = navigator.getCurrent();

            for (int j = 0; j < blocks.length; j++) {
                blocks[j] = ((Number) data[j]).longValue();
            }
        }

        navigator.release();

        return blocks;
    }
}
