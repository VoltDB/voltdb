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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import org.hsqldb_voltpatches.Database;
import org.hsqldb_voltpatches.DatabaseURL;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.Statement;
import org.hsqldb_voltpatches.Table;
import org.hsqldb_voltpatches.Types;
import org.hsqldb_voltpatches.lib.HashMappedList;
import org.hsqldb_voltpatches.lib.HsqlByteArrayInputStream;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;
import org.hsqldb_voltpatches.lib.LineGroupReader;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultLob;
import org.hsqldb_voltpatches.result.ResultMetaData;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.BlobDataID;
import org.hsqldb_voltpatches.types.ClobData;
import org.hsqldb_voltpatches.types.ClobDataID;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class LobManager {

    static final String resourceFileName =
        "/org/hsqldb_voltpatches/resources/lob-schema.sql";
    static final String[] starters = new String[]{ "/*" };

    //
    Database database;
    LobStore lobStore;
    Session  sysLobSession;

    //
    //
    int lobBlockSize         = 1024 * 32;
    int totalBlockLimitCount = 1024 * 1024 * 1024;

    //
    Statement getLob;
    Statement getLobPart;
    Statement deleteLob;
    Statement deleteLobPart;
    Statement divideLobPart;
    Statement createLob;
    Statement createLobPart;
    Statement setLobLength;
    Statement setLobUsage;
    Statement getNextLobId;

    // LOBS columns
    private interface LOBS {

        int BLOCK_ADDR   = 0;
        int BLOCK_COUNT  = 1;
        int BLOCK_OFFSET = 2;
        int LOB_ID       = 3;
    }

    private interface ALLOC_BLOCKS {

        int BLOCK_COUNT  = 0;
        int BLOCK_OFFSET = 1;
        int LOB_ID       = 2;
    }

    //BLOCK_ADDR INT, BLOCK_COUNT INT, TX_ID BIGINT
    private static String initialiseBlocksSQL =
        "INSERT INTO SYSTEM_LOBS.BLOCKS VALUES(?,?,?)";
    private static String getLobSQL =
        "SELECT * FROM SYSTEM_LOBS.LOB_IDS WHERE LOB_ID = ?";
    private static String getLobPartSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND BLOCK_OFFSET >= ? AND BLOCK_OFFSET < ? ORDER BY BLOCK_OFFSET";

    // DELETE_BLOCKS(L_ID BIGINT, B_OFFSET INT, B_COUNT INT, TX_ID BIGINT)
    private static String deleteLobPartSQL =
        "CALL SYSTEM_LOBS.DELETE_BLOCKS(?,?,?,?)";
    private static String createLobSQL =
        "INSERT INTO SYSTEM_LOBS.LOB_IDS VALUES(?, ?, ?, ?)";
    private static String updateLobLengthSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_LENGTH = ? WHERE LOB_ID = ?";
    private static String createLobPartSQL =
        "CALL SYSTEM_LOBS.ALLOC_BLOCKS(?, ?, ?)";
    private static String divideLobPartSQL =
        "CALL SYSTEM_LOBS.DIVIDE_BLOCK(?, ?)";
    private static String getSpanningBlockSQL =
        "SELECT * FROM SYSTEM_LOBS.LOBS WHERE LOB_ID = ? AND ? > BLOCK_OFFSET AND ? < BLOCK_OFFSET + BLOCK_COUNT";
    private static String updateLobUsageSQL =
        "UPDATE SYSTEM_LOBS.LOB_IDS SET LOB_USAGE_COUNT = ? WHERE LOB_ID = ?";
    private static String getNextLobIdSQL =
        "VALUES NEXT VALUE FOR SYSTEM_LOBS.LOB_ID";
    private static String deleteLobSQL = "CALL SYSTEM_LOBS.DELETE_LOB(?, ?)";

    //    (OUT L_ADDR INT, IN B_COUNT INT, IN B_OFFSET INT, IN L_ID BIGINT, IN L_LENGTH BIGINT)
    public LobManager(Database database) {
        this.database = database;
    }

    public void createSchema() {

        sysLobSession = database.sessionManager.getSysLobSession();

        Session           session = sysLobSession;
        InputStream fis = getClass().getResourceAsStream(resourceFileName);
        InputStreamReader reader  = null;

        try {
            reader = new InputStreamReader(fis, "ISO-8859-1");
        } catch (Exception e) {}

        LineNumberReader lineReader = new LineNumberReader(reader);
        LineGroupReader  lg = new LineGroupReader(lineReader, starters);
        HashMappedList   map        = lg.getAsMap();

        lg.close();

        String    sql       = (String) map.get("/*lob_schema_definition*/");
        Statement statement = session.compileStatement(sql);
        Result    result    = statement.execute(session);
        Table table = database.schemaManager.getTable(session, "BLOCKS",
            "SYSTEM_LOBS");

//            table.isTransactional = false;
        getLob        = session.compileStatement(getLobSQL);
        getLobPart    = session.compileStatement(getLobPartSQL);
        createLob     = session.compileStatement(createLobSQL);
        createLobPart = session.compileStatement(createLobPartSQL);
        divideLobPart = session.compileStatement(divideLobPartSQL);
        deleteLob     = session.compileStatement(deleteLobSQL);
        deleteLobPart = session.compileStatement(deleteLobPartSQL);
        setLobLength  = session.compileStatement(updateLobLengthSQL);
        setLobUsage   = session.compileStatement(updateLobUsageSQL);
        getNextLobId  = session.compileStatement(getNextLobIdSQL);
    }

    public void initialiseLobSpace() {

        Statement statement =
            sysLobSession.compileStatement(initialiseBlocksSQL);
        Object[] args = new Object[3];

        args[0] = Integer.valueOf(0);
        args[1] = Integer.valueOf(totalBlockLimitCount);
        args[2] = Long.valueOf(0);

        sysLobSession.executeCompiledStatement(statement, args);
    }

    void initialiseLobStore() {}

    public void open() {

        if (DatabaseURL.isFileBasedDatabaseType(database.getType())) {
            lobStore = new LobStoreRAFile(database, lobBlockSize);
        } else {
            lobStore = new LobStoreMem(lobBlockSize);
        }
    }

    public void close() {}

    //
    private long getNewLobID(Session session) {

        Result result = getNextLobId.execute(session);

        if (result.isError()) {
            return 0;
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();

        if (!next) {
            navigator.close();

            return 0;
        }

        Object[] data = navigator.getCurrent();

        return ((Long) data[0]).longValue();
    }

    private Object[] getLobHeader(Session session, long lobID) {

        ResultMetaData meta     = getLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);

        session.sessionContext.pushDynamicArguments(params);

        Result result = getLob.execute(session);

        session.sessionContext.popDynamicArguments();

        if (result.isError()) {
            return null;
        }

        RowSetNavigator navigator = result.getNavigator();
        boolean         next      = navigator.next();

        if (!next) {
            navigator.close();

            return null;
        }

        Object[] data = navigator.getCurrent();

        return data;
    }

    public BlobData getBlob(Session session, long lobID) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return null;
        }

        BlobData blob = new BlobDataID(lobID);

        return blob;
    }

    public ClobData getClob(Session session, long lobID) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return null;
        }

        ClobData clob = new ClobDataID(lobID);

        return clob;
    }

    public long createBlob(long length) {

        long           lobID    = getNewLobID(sysLobSession);
        ResultMetaData meta     = createLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Long.valueOf(length);
        params[2] = Long.valueOf(1);
        params[3] = Integer.valueOf(Types.SQL_BLOB);

        Result result = sysLobSession.executeCompiledStatement(createLob,
            params);

        return lobID;
    }

    public long createClob(long length) {

        long           lobID    = getNewLobID(sysLobSession);
        ResultMetaData meta     = createLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Long.valueOf(length);
        params[2] = Long.valueOf(1);
        params[3] = Integer.valueOf(Types.SQL_CLOB);

        Result result = sysLobSession.executeCompiledStatement(createLob,
            params);

        return lobID;
    }

    public Result deleteLob(long lobID) {

        Session        session  = this.sysLobSession;
        ResultMetaData meta     = deleteLob.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Long.valueOf(0);

        Result result = session.executeCompiledStatement(deleteLob, params);

        return result;
    }

    public Result getLength(Session session, long lobID) {

        try {
            long length = getLengthValue(session, lobID);

            return ResultLob.newLobSetResponse(lobID, length);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }

    public long getLengthValue(Session session, long lobID) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            throw Error.error(ErrorCode.X_0F502);
        }

        long length = ((Long) data[1]).longValue();

        return length;
    }

    /**
     * Used for SUBSTRING
     */
    public Result getLob(Session session, long lobID, long offset,
                         long length) {
        throw Error.runtimeError(ErrorCode.U_S0500, "LobManager");
    }

    public Result createDuplicateLob(Session session, long lobID) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long   newLobID = getNewLobID(session);
        Object params[] = new Object[data.length];

        params[0] = Long.valueOf(newLobID);
        params[1] = data[1];
        params[2] = data[2];
        params[3] = data[3];

        Result result = session.executeCompiledStatement(createLob, params);

        if (result.isError()) {
            return result;
        }

        long length     = ((Long) data[1]).longValue();
        long byteLength = length;
        int  lobType    = ((Integer) data[1]).intValue();

        if (lobType == Types.SQL_CLOB) {
            byteLength *= 2;
        }

        int newBlockCount = (int) byteLength / lobBlockSize;

        if (byteLength % lobBlockSize != 0) {
            newBlockCount++;
        }

        createBlockAddresses(session, newLobID, 0, newBlockCount);

        // copy the contents
        int[][] sourceBlocks = getBlockAddresses(session, lobID, 0,
            Integer.MAX_VALUE);
        int[][] targetBlocks = getBlockAddresses(session, newLobID, 0,
            Integer.MAX_VALUE);

        try {
            copyBlockSet(sourceBlocks, targetBlocks);
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        return ResultLob.newLobSetResponse(newLobID, length);
    }

    private void copyBlockSet(int[][] source, int[][] target) {

        int sourceIndex = 0;
        int targetIndex = 0;

        while (true) {
            int sourceOffset = source[sourceIndex][LOBS.BLOCK_OFFSET]
                               + sourceIndex;
            int targetOffset = target[targetIndex][LOBS.BLOCK_OFFSET]
                               + targetIndex;
            byte[] bytes = lobStore.getBlockBytes(sourceOffset, 1);

            lobStore.setBlockBytes(bytes, targetOffset, 1);

            sourceOffset++;
            targetOffset++;

            if (sourceOffset == source[sourceIndex][LOBS.BLOCK_COUNT]) {
                sourceOffset = 0;

                sourceIndex++;
            }

            if (targetOffset == target[sourceIndex][LOBS.BLOCK_COUNT]) {
                targetOffset = 0;

                targetIndex++;
            }

            if (sourceIndex == source.length) {
                break;
            }
        }
    }

    public Result getChars(Session session, long lobID, long offset,
                           int length) {

        Result result = getBytes(session, lobID, offset * 2, length * 2);

        if (result.isError()) {
            return result;
        }

        byte[]                   bytes = ((ResultLob) result).getByteArray();
        HsqlByteArrayInputStream be    = new HsqlByteArrayInputStream(bytes);
        char[]                   chars = new char[bytes.length / 2];

        try {
            for (int i = 0; i < chars.length; i++) {
                chars[i] = be.readChar();
            }
        } catch (Exception e) {
            return Result.newErrorResult(e);
        }

        return ResultLob.newLobGetCharsResponse(lobID, offset, chars);
    }

    public Result getBytes(Session session, long lobID, long offset,
                           int length) {

        int blockOffset     = (int) (offset / lobBlockSize);
        int byteBlockOffset = (int) (offset % lobBlockSize);
        int blockLimit      = (int) ((offset + length) / lobBlockSize);
        int byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        int    dataBytesPosition = 0;
        byte[] dataBytes         = new byte[length];
        int[][] blockAddresses = getBlockAddresses(session, lobID,
            blockOffset, blockLimit);

        if (blockAddresses.length == 0) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        //
        int i = 0;
        int blockCount = blockAddresses[i][1]
                         - (blockAddresses[i][2] - blockOffset);

        if (blockAddresses[i][1] + blockAddresses[i][2] > blockLimit) {
            blockCount -= (blockAddresses[i][1] + blockAddresses[i][2]
                           - blockLimit);
        }

        byte[] bytes;

        try {
            bytes = lobStore.getBlockBytes(blockAddresses[i][0] + blockOffset,
                                           blockCount);
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
            blockCount = blockAddresses[i][1];

            if (blockAddresses[i][1] + blockAddresses[i][2] > blockLimit) {
                blockCount -= (blockAddresses[i][1] + blockAddresses[i][2]
                               - blockLimit);
            }

            try {
                bytes = lobStore.getBlockBytes(blockAddresses[i][0],
                                               blockCount);
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

    public Result setBytesBA(Session session, long lobID, byte[] dataBytes,
                             long offset, int length) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long oldLength       = ((Long) data[1]).longValue();
        int  blockOffset     = (int) (offset / lobBlockSize);
        int  byteBlockOffset = (int) (offset % lobBlockSize);
        int  blockLimit      = (int) ((offset + length) / lobBlockSize);
        int  byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        int[][] blockAddresses = getBlockAddresses(session, lobID,
            blockOffset, blockLimit);
        byte[] newBytes = new byte[(blockLimit - blockOffset) * lobBlockSize];

        if (blockAddresses.length > 0) {
            int blockAddress = blockAddresses[0][0]
                               + (blockOffset - blockAddresses[0][2]);

            try {
                byte[] block = lobStore.getBlockBytes(blockAddress, 1);

                System.arraycopy(block, 0, newBytes, 0, lobBlockSize);

                if (blockAddresses.length > 1) {
                    blockAddress =
                        blockAddresses[blockAddresses.length - 1][0]
                        + (blockLimit
                           - blockAddresses[blockAddresses.length - 1][2] - 1);
                    block = lobStore.getBlockBytes(blockAddress, 1);

                    System.arraycopy(block, 0, newBytes,
                                     blockLimit - blockOffset - 1,
                                     lobBlockSize);
                } else if (blockLimit - blockOffset > 1) {
                    blockAddress = blockAddresses[0][0]
                                   + (blockLimit - blockAddresses[0][2] - 1);
                    block = lobStore.getBlockBytes(blockAddress, 1);

                    System.arraycopy(block, 0, newBytes,
                                     (blockLimit - blockOffset - 1)
                                     * lobBlockSize, lobBlockSize);
                }
            } catch (HsqlException e) {
                return Result.newErrorResult(e);
            }

            // should turn into SP
            divideBlockAddresses(session, lobID, blockOffset);
            divideBlockAddresses(session, lobID, blockLimit);
            deleteBlockAddresses(session, lobID, blockOffset, blockLimit);
        }

        createBlockAddresses(session, lobID, blockOffset,
                             blockLimit - blockOffset);
        System.arraycopy(dataBytes, 0, newBytes, byteBlockOffset, length);

        blockAddresses = getBlockAddresses(session, lobID, blockOffset,
                                           blockLimit);

        //
        try {
            for (int i = 0; i < blockAddresses.length; i++) {
                lobStore.setBlockBytes(newBytes, blockAddresses[i][0],
                                       blockAddresses[i][1]);
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }

        if (offset + length > oldLength) {
            oldLength = offset + length;

            setLength(session, lobID, oldLength);
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    private Result setBytesIS(Session session, long lobID,
                              InputStream inputStream, long length) {

        int blockLimit      = (int) (length / lobBlockSize);
        int byteLimitOffset = (int) (length % lobBlockSize);

        if (byteLimitOffset == 0) {
            byteLimitOffset = lobBlockSize;
        } else {
            blockLimit++;
        }

        createBlockAddresses(session, lobID, 0, blockLimit);

        int[][] blockAddresses = getBlockAddresses(session, lobID, 0,
            blockLimit);
        byte[] dataBytes = new byte[lobBlockSize];

        for (int i = 0; i < blockAddresses.length; i++) {
            for (int j = 0; j < blockAddresses[i][1]; j++) {
                int localLength = lobBlockSize;

                if (i == blockAddresses.length - 1
                        && j == blockAddresses[i][1] - 1) {
                    localLength = byteLimitOffset;

// todo -- use block op
                    for (int k = localLength; k < lobBlockSize; k++) {
                        dataBytes[k] = 0;
                    }
                }

                try {
                    int count = 0;

                    while (localLength > 0) {
                        int read = inputStream.read(dataBytes, count,
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

                try {
                    lobStore.setBlockBytes(dataBytes,
                                           blockAddresses[i][0] + j, 1);
                } catch (HsqlException e) {
                    return Result.newErrorResult(e);
                }
            }
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    public Result setBytes(Session session, long lobID, byte[] dataBytes,
                           long offset) {

        if (dataBytes.length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long length = ((Long) data[1]).longValue();
        Result result = setBytesBA(session, lobID, dataBytes, offset,
                                   dataBytes.length);

        if (offset + dataBytes.length > length) {
            setLength(session, lobID, offset + dataBytes.length);
        }

        return result;
    }

    public Result setBytesForNewBlob(long lobID, InputStream inputStream,
                                     long length) {

        Session session = sysLobSession;

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Result result = setBytesIS(session, lobID, inputStream, length);

        return result;
    }

    public Result setChars(Session session, long lobID, long offset,
                           char[] chars) {

        if (chars.length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        long length = ((Long) data[1]).longValue();
        HsqlByteArrayOutputStream os =
            new HsqlByteArrayOutputStream(chars.length * 2);

        os.write(chars, 0, chars.length);

        Result result = setBytesBA(session, lobID, os.getBuffer(), offset * 2,
                                   os.getBuffer().length);

        if (result.isError()) {
            return result;
        }

        if (offset + chars.length > length) {
            result = setLength(session, lobID, offset + chars.length);

            if (result.isError()) {
                return result;
            }
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    public Result setCharsForNewClob(long lobID, InputStream inputStream,
                                     long length) {

        Session session = sysLobSession;

        if (length == 0) {
            return ResultLob.newLobSetResponse(lobID, 0);
        }

        Result result = setBytesIS(session, lobID, inputStream, length * 2);

        if (result.isError()) {
            return result;
        }

        return ResultLob.newLobSetResponse(lobID, 0);
    }

    public Result truncate(Session session, long lobID, long offset) {

        Object[] data = getLobHeader(session, lobID);

        if (data == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_0F502));
        }

        /** @todo 1.9.0 - double offset for clob */
        long length          = ((Long) data[1]).longValue();
        int  blockOffset     = (int) (offset / lobBlockSize);
        int  blockLimit      = (int) ((offset + length) / lobBlockSize);
        int  byteLimitOffset = (int) ((offset + length) % lobBlockSize);

        if (byteLimitOffset != 0) {
            blockLimit++;
        }

        ResultMetaData meta     = deleteLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(blockOffset);
        params[2] = Integer.valueOf(blockLimit);
        params[3] = Long.valueOf(session.getTransactionTimestamp());

        Result result = session.executeCompiledStatement(deleteLobPart,
            params);

        setLength(session, lobID, offset);

        return ResultLob.newLobTruncateResponse(lobID);
    }

    public Result setLength(Session session, long lobID, long length) {

        ResultMetaData meta     = setLobLength.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(length);
        params[1] = Long.valueOf(lobID);

        Result result = session.executeCompiledStatement(setLobLength, params);

        return result;
    }

    public Result adjustUsageCount(long lobID, int delta) {

        Object[] data  = getLobHeader(sysLobSession, lobID);
        int      count = ((Number) data[2]).intValue();

        if (count + delta == 0) {
            return deleteLob(lobID);
        }

        ResultMetaData meta     = setLobUsage.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(count + delta);
        params[1] = Long.valueOf(lobID);

        Result result = sysLobSession.executeCompiledStatement(setLobLength,
            params);

        return result;
    }

    int[][] getBlockAddresses(Session session, long lobID, int offset,
                              int limit) {

        ResultMetaData meta     = getLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);
        params[2] = Integer.valueOf(limit);

        session.sessionContext.pushDynamicArguments(params);

        Result result = getLobPart.execute(session);

        session.sessionContext.popDynamicArguments();

        RowSetNavigator navigator = result.getNavigator();
        int             size      = navigator.getSize();
        int[][]         blocks    = new int[size][3];

        for (int i = 0; i < size; i++) {
            navigator.absolute(i);

            Object[] data = navigator.getCurrent();

            blocks[i][0] = ((Integer) data[LOBS.BLOCK_ADDR]).intValue();
            blocks[i][1] = ((Integer) data[LOBS.BLOCK_COUNT]).intValue();
            blocks[i][2] = ((Integer) data[LOBS.BLOCK_OFFSET]).intValue();
        }

        navigator.close();

        return blocks;
    }

    void deleteBlockAddresses(Session session, long lobID, int offset,
                              int count) {

        ResultMetaData meta     = deleteLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);
        params[2] = Integer.valueOf(count);

        Result result = session.executeCompiledStatement(deleteLobPart,
            params);
    }

    void divideBlockAddresses(Session session, long lobID, int offset) {

        ResultMetaData meta     = divideLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[0] = Long.valueOf(lobID);
        params[1] = Integer.valueOf(offset);

        Result result = session.executeCompiledStatement(divideLobPart,
            params);
    }

    void createBlockAddresses(Session session, long lobID, int offset,
                              int count) {

        ResultMetaData meta     = createLobPart.getParametersMetaData();
        Object         params[] = new Object[meta.getColumnCount()];

        params[ALLOC_BLOCKS.BLOCK_COUNT]  = Integer.valueOf(count);
        params[ALLOC_BLOCKS.BLOCK_OFFSET] = Integer.valueOf(offset);
        params[ALLOC_BLOCKS.LOB_ID]       = Long.valueOf(lobID);

        Result result = session.executeCompiledStatement(createLobPart,
            params);
    }
}
