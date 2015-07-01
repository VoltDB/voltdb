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


package org.hsqldb_voltpatches.result;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;

import org.hsqldb_voltpatches.ColumnBase;
import org.hsqldb_voltpatches.Error;
import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.HsqlException;
import org.hsqldb_voltpatches.Session;
import org.hsqldb_voltpatches.SessionInterface;
import org.hsqldb_voltpatches.Statement;
import org.hsqldb_voltpatches.StatementTypes;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.DataOutputStream;
import org.hsqldb_voltpatches.navigator.RowSetNavigator;
import org.hsqldb_voltpatches.navigator.RowSetNavigatorClient;
import org.hsqldb_voltpatches.rowio.RowInputBinary;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.Type;

/**
 *  The primary unit of communication between Connection, Server and Session
 *  objects.
 *
 *  An HSQLDB Result object encapsulates all requests (such as to alter or
 *  query session settings, to allocate and execute statements, etc.) and all
 *  responses (such as exception indications, update counts, result sets and
 *  result set metadata). It also implements the HSQL wire protocol for
 *  comunicating all such requests and responses across the network.
 *  Uses a navigator for data.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class Result {

    public static final Result updateZeroResult =
        newResult(ResultConstants.UPDATECOUNT);
    public static final Result updateOneResult =
        newResult(ResultConstants.UPDATECOUNT);
    public static final Result updateTwoResult =
        newResult(ResultConstants.UPDATECOUNT);

    static {
        updateOneResult.setUpdateCount(1);
        updateTwoResult.setUpdateCount(2);
    }

    public static final ResultMetaData sessionAttributesMetaData =
        ResultMetaData.newResultMetaData(SessionInterface.INFO_LIMIT);

    static {
        for (int i = 0; i < Session.INFO_LIMIT; i++) {
            sessionAttributesMetaData.columns[i] = new ColumnBase(null, null,
                    null, null);
        }

        sessionAttributesMetaData.columns[Session.INFO_ID].setType(
            Type.SQL_INTEGER);
        sessionAttributesMetaData.columns[Session.INFO_INTEGER].setType(
            Type.SQL_INTEGER);
        sessionAttributesMetaData.columns[Session.INFO_BOOLEAN].setType(
            Type.SQL_BOOLEAN);
        sessionAttributesMetaData.columns[Session.INFO_VARCHAR].setType(
            Type.SQL_VARCHAR);
        sessionAttributesMetaData.prepareData();
    }

    private static final ResultMetaData emptyMeta =
        ResultMetaData.newResultMetaData(0);
    public static final Result emptyGeneratedResult =
        Result.newDataResult(emptyMeta);

    // type of result
    byte mode;

    // database ID
    int databaseID;

    // session ID
    long sessionID;

    // result id
    private long id;

    // database name for new connection
    private String databaseName;

    // user / password for new connection
    // error strings in error results
    private String mainString;
    private String subString;

    // vendor error code
    int errorCode;

    // the exception if this is an error
    private HsqlException exception;

    // prepared statement id
    long statementID;

    // statement type based on whether it returns an update count or a result set
    // type of session info requested
    int statementReturnType;

    // max rows (out)
    // update count (in)
    // time zone seconds (connect)
    private int updateCount;

    // fetch size (in)
    private int fetchSize;

    // secondary result
    private Result chainedResult;

    //
    private int lobCount;
    ResultLob   lobResults;

    /** A Result object's metadata */
    public ResultMetaData metaData;

    /** Additional meta data for parameters used in PREPARE_ACK results */
    public ResultMetaData parameterMetaData;

    /** Additional meta data for required generated columns */
    public ResultMetaData generatedMetaData;

    //
    public int rsScrollability;
    public int rsConcurrency;
    public int rsHoldability;

    //
    int generateKeys;

    // simple value for PSM
    Object valueData;

    //
    Statement statement;

    public static Result newResult(RowSetNavigator nav) {

        Result result = new Result();

        result.mode      = ResultConstants.DATA;
        result.navigator = nav;

        return result;
    }

    public static Result newResult(int type) {

        RowSetNavigator navigator = null;
        Result          result    = null;

        switch (type) {

            case ResultConstants.CALL_RESPONSE :
            case ResultConstants.EXECUTE :
                navigator = new RowSetNavigatorClient(1);
                break;

            case ResultConstants.UPDATE_RESULT :
                navigator = new RowSetNavigatorClient(1);
                break;

            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
                navigator = new RowSetNavigatorClient(4);
                break;

            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator = new RowSetNavigatorClient(1);
                break;

            case ResultConstants.BATCHEXECRESPONSE :
                navigator = new RowSetNavigatorClient(4);
                break;

            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATAROWS :
                break;

            case ResultConstants.LARGE_OBJECT_OP :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
            default :
        }

        result           = new Result();
        result.mode      = (byte) type;
        result.navigator = navigator;

        return result;
    }

    public static Result newResult(DataInput dataInput,
                                   RowInputBinary in)
                                   throws IOException, HsqlException {
        return newResult(null, dataInput.readByte(), dataInput, in);
    }

    public static Result newResult(Session session, int mode,
                                   DataInput dataInput,
                                   RowInputBinary in)
                                   throws IOException, HsqlException {

        try {
            if (mode == ResultConstants.LARGE_OBJECT_OP) {
                return ResultLob.newLob(dataInput, false);
            }

            Result result = newResult(session, dataInput, in, mode);

            return result;
        } catch (IOException e) {
            throw Error.error(ErrorCode.X_08000);
        }
    }

    public void readAdditionalResults(SessionInterface session,
                                      DataInputStream inputStream,
                                      RowInputBinary in)
                                      throws IOException, HsqlException {

        setSession(session);

        Result  currentResult = this;
        boolean hasLob        = false;

        while (true) {
            int addedResultMode = inputStream.readByte();

            if (addedResultMode == ResultConstants.LARGE_OBJECT_OP) {
                ResultLob resultLob = ResultLob.newLob(inputStream, false);

                if (session instanceof Session) {
                    ((Session) session).allocateResultLob(resultLob,
                                                          inputStream);
                }

                currentResult.addLobResult(resultLob);

                hasLob = true;

                continue;
            }

            if (hasLob) {
                hasLob = false;

                if (session instanceof Session) {
                    ((Session) session).registerResultLobs(currentResult);
                }
            }

            if (addedResultMode == ResultConstants.NONE) {
                return;
            }

            currentResult = newResult(null, inputStream, in, addedResultMode);

            addChainedResult(currentResult);
        }
    }

    public static void readExecuteProperties(Session session, Result result,
            DataInputStream dataInput, RowInputBinary in) {

        try {
            int length = dataInput.readInt();

            in.resetRow(0, length);

            byte[]    byteArray = in.getBuffer();
            final int offset    = 4;

            dataInput.readFully(byteArray, offset, length - offset);

            result.updateCount     = in.readInt();
            result.fetchSize       = in.readInt();
            result.statementID     = in.readLong();
            result.rsScrollability = in.readShort();
            result.rsConcurrency   = in.readShort();
            result.rsHoldability   = in.readShort();

            Statement statement =
                session.database.compiledStatementManager.getStatement(session,
                    result.statementID);

            result.statement = statement;
            result.metaData  = result.statement.getParametersMetaData();

            result.navigator.readSimple(in, result.metaData);
        } catch (IOException e) {
            throw Error.error(ErrorCode.X_08000);
        }
    }

    private static Result newResult(Session session, DataInput dataInput,
                                    RowInputBinary in,
                                    int mode)
                                    throws IOException, HsqlException {

        Result result = newResult(mode);
        int    length = dataInput.readInt();

        in.resetRow(0, length);

        byte[]    byteArray = in.getBuffer();
        final int offset    = 4;

        dataInput.readFully(byteArray, offset, length - offset);

        switch (mode) {

            case ResultConstants.GETSESSIONATTR :
                result.statementReturnType = in.readByte();
                break;

            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;

            case ResultConstants.PREPARE :
                result.setStatementType(in.readByte());

                result.mainString      = in.readString();
                result.rsScrollability = in.readShort();
                result.rsConcurrency   = in.readShort();
                result.rsHoldability   = in.readShort();
                result.generateKeys    = in.readByte();

                if (result.generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || result
                        .generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }
                break;

            case ResultConstants.CLOSE_RESULT :
                result.id = in.readLong();
                break;

            case ResultConstants.FREESTMT :
                result.statementID = in.readLong();
                break;

            case ResultConstants.EXECDIRECT :
                result.updateCount         = in.readInt();
                result.fetchSize           = in.readInt();
                result.statementReturnType = in.readByte();
                result.mainString          = in.readString();
                result.rsScrollability     = in.readShort();
                result.rsConcurrency       = in.readShort();
                result.rsHoldability       = in.readShort();
                result.generateKeys        = in.readByte();

                if (result.generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || result
                        .generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }
                break;

            case ResultConstants.CONNECT :
                result.databaseName = in.readString();
                result.mainString   = in.readString();
                result.subString    = in.readString();
                result.updateCount  = in.readInt();
                break;

            case ResultConstants.ERROR :
                result.mainString = in.readString();
                result.subString  = in.readString();
                result.errorCode  = in.readInt();
                break;

            case ResultConstants.CONNECTACKNOWLEDGE :
                result.databaseID = in.readInt();
                result.sessionID  = in.readLong();
                break;

            case ResultConstants.UPDATECOUNT :
                result.updateCount = in.readInt();
                break;

            case ResultConstants.ENDTRAN : {
                int type = in.readInt();

                result.setActionType(type);                     // endtran type

                switch (type) {

                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        result.mainString = in.readString();    // savepoint name
                        break;

                    case ResultConstants.TX_COMMIT :
                    case ResultConstants.TX_ROLLBACK :
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                int type = in.readInt();                        // attr type

                result.setConnectionAttrType(type);

                switch (type) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        result.mainString = in.readString();    // savepoint name
                        break;

                    //  case ResultConstants.SQL_ATTR_AUTO_IPD :
                    //      - always true
                    //  default: throw - case never happens
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }
            case ResultConstants.PREPARE_ACK :
                result.statementReturnType = in.readByte();
                result.statementID         = in.readLong();
                result.rsScrollability     = in.readShort();
                result.rsConcurrency       = in.readShort();
                result.rsHoldability       = in.readShort();
                result.metaData            = new ResultMetaData(in);
                result.parameterMetaData   = new ResultMetaData(in);
                break;

            case ResultConstants.CALL_RESPONSE :
                result.updateCount     = in.readInt();
                result.fetchSize       = in.readInt();
                result.statementID     = in.readLong();
                result.rsScrollability = in.readShort();
                result.rsConcurrency   = in.readShort();
                result.rsHoldability   = in.readShort();
                result.metaData        = new ResultMetaData(in);

                result.navigator.readSimple(in, result.metaData);
                break;

            case ResultConstants.EXECUTE :
                result.updateCount     = in.readInt();
                result.fetchSize       = in.readInt();
                result.statementID     = in.readLong();
                result.rsScrollability = in.readShort();
                result.rsConcurrency   = in.readShort();
                result.rsHoldability   = in.readShort();

                Statement statement =
                    session.database.compiledStatementManager.getStatement(
                        session, result.statementID);

                result.statement = statement;
                result.metaData  = result.statement.getParametersMetaData();

                result.navigator.readSimple(in, result.metaData);
                break;

            case ResultConstants.UPDATE_RESULT : {
                result.id = in.readLong();

                int type = in.readInt();

                result.setActionType(type);

                result.metaData = new ResultMetaData(in);

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                result.updateCount = in.readInt();
                result.fetchSize   = in.readInt();
                result.statementID = in.readLong();
                result.metaData    = new ResultMetaData(in);

                result.navigator.readSimple(in, result.metaData);

                break;
            }
            case ResultConstants.PARAM_METADATA : {
                result.metaData = new ResultMetaData(in);

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.REQUESTDATA : {
                result.id          = in.readLong();
                result.updateCount = in.readInt();
                result.fetchSize   = in.readInt();

                break;
            }
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATA : {
                result.id              = in.readLong();
                result.updateCount     = in.readInt();
                result.fetchSize       = in.readInt();
                result.rsScrollability = in.readShort();
                result.rsConcurrency   = in.readShort();
                result.rsHoldability   = in.readShort();
                result.metaData        = new ResultMetaData(in);
                result.navigator       = new RowSetNavigatorClient();

                result.navigator.read(in, result.metaData);

                break;
            }
            case ResultConstants.DATAROWS : {
                result.metaData  = new ResultMetaData(in);
                result.navigator = new RowSetNavigatorClient();

                result.navigator.read(in, result.metaData);

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "Result.newResult");
        }

        return result;
    }

    /**
     * For interval PSM return values
     */
    public static Result newPSMResult(int type, String label, Object value) {

        Result result = newResult(ResultConstants.VALUE);

        result.errorCode  = type;
        result.mainString = label;
        result.valueData  = value;

        return result;
    }

    /**
     * For SQLPREPARE
     * For parparation of SQL parepared statements.
     */
    public static Result newPrepareStatementRequest() {
        return newResult(ResultConstants.PREPARE);
    }

    /**
     * For SQLEXECUTE
     * For execution of SQL prepared statements.
     * The parameters are set afterwards as the Result is reused
     */
    public static Result newPreparedExecuteRequest(Type[] types,
            long statementId) {

        Result result = newResult(ResultConstants.EXECUTE);

        result.metaData    = ResultMetaData.newSimpleResultMetaData(types);
        result.statementID = statementId;

        result.navigator.add(ValuePool.emptyObjectArray);

        return result;
    }

    /**
     * For CALL_RESPONSE
     * For execution of SQL callable statements.
     */
    public static Result newCallResponse(Type[] types, long statementId,
                                         Object[] values) {

        Result result = newResult(ResultConstants.CALL_RESPONSE);

        result.metaData    = ResultMetaData.newSimpleResultMetaData(types);
        result.statementID = statementId;

        result.navigator.add(values);

        return result;
    }

    /**
     * For UPDATE_RESULT
     * The parameters are set afterwards as the Result is reused
     */
    public static Result newUpdateResultRequest(Type[] types, long id) {

        Result result = newResult(ResultConstants.UPDATE_RESULT);

        result.metaData = ResultMetaData.newUpdateResultMetaData(types);
        result.id       = id;

        result.navigator.add(new Object[]{});

        return result;
    }

    /**
     * For UPDATE_RESULT results
     * The parameters are set by this method as the Result is reused
     */
    public void setPreparedResultUpdateProperties(Object[] parameterValues) {

        if (navigator.getSize() == 1) {
            ((RowSetNavigatorClient) navigator).setData(0, parameterValues);
        } else {
            navigator.clear();
            navigator.add(parameterValues);
        }
    }

    /**
     * For SQLEXECUTE results
     * The parameters are set by this method as the Result is reused
     */
    public void setPreparedExecuteProperties(Object[] parameterValues,
            int maxRows, int fetchSize) {

        mode = ResultConstants.EXECUTE;

        if (navigator.getSize() == 1) {
            ((RowSetNavigatorClient) navigator).setData(0, parameterValues);
        } else {
            navigator.clear();
            navigator.add(parameterValues);
        }

        updateCount    = maxRows;
        this.fetchSize = fetchSize;
    }

    /**
     * For BATCHEXECUTE
     */
    public void setBatchedPreparedExecuteRequest() {

        mode = ResultConstants.BATCHEXECUTE;

        ((RowSetNavigatorClient) navigator).clear();

        updateCount    = 0;
        this.fetchSize = 0;
    }

    public void addBatchedPreparedExecuteRequest(Object[] parameterValues) {
        ((RowSetNavigatorClient) navigator).add(parameterValues);
    }

    /**
     * For BATCHEXECDIRECT
     */
    public static Result newBatchedExecuteRequest() {

        Type[] types  = new Type[]{ Type.SQL_VARCHAR };
        Result result = newResult(ResultConstants.BATCHEXECDIRECT);

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);

        return result;
    }

    /**
     * For BATCHEXERESPONSE for a BATCHEXECUTE or BATCHEXECDIRECT
     */
    public static Result newBatchedExecuteResponse(int[] updateCounts,
            Result generatedResult, Result e) {

        Result result = newResult(ResultConstants.BATCHEXECRESPONSE);

        result.addChainedResult(generatedResult);
        result.addChainedResult(e);

        Type[] types = new Type[]{ Type.SQL_INTEGER };

        result.metaData = ResultMetaData.newSimpleResultMetaData(types);

        Object[][] table = new Object[updateCounts.length][];

        for (int i = 0; i < updateCounts.length; i++) {
            table[i] = new Object[]{ ValuePool.getInt(updateCounts[i]) };
        }

        ((RowSetNavigatorClient) result.navigator).setData(table);

        return result;
    }

    public static Result newResetSessionRequest() {

        Result result = newResult(ResultConstants.RESETSESSION);

        return result;
    }

    public static Result newConnectionAttemptRequest(String user,
            String password, String database, int timeZoneSeconds) {

        Result result = newResult(ResultConstants.CONNECT);

        result.mainString   = user;
        result.subString    = password;
        result.databaseName = database;
        result.updateCount  = timeZoneSeconds;

        return result;
    }

    public static Result newConnectionAcknowledgeResponse(long sessionID,
            int databaseID) {

        Result result = newResult(ResultConstants.CONNECTACKNOWLEDGE);

        result.sessionID  = sessionID;
        result.databaseID = databaseID;

        return result;
    }

    public static Result getUpdateCountResult(int count) {

        switch (count) {

            case 0 :
                return Result.updateZeroResult;

            case 1 :
                return Result.updateOneResult;

            case 2 :
                return Result.updateTwoResult;

            default :
        }

        Result result = newResult(ResultConstants.UPDATECOUNT);

        result.updateCount = count;

        return result;
    }

    public static Result newUpdateCountResult(ResultMetaData meta, int count) {

        Result result     = newResult(ResultConstants.UPDATECOUNT);
        Result dataResult = newDataResult(meta);

        result.updateCount = count;

        result.addChainedResult(dataResult);

        return result;
    }

    public static Result newSingleColumnResult(ResultMetaData meta) {

        Result result = newResult(ResultConstants.DATA);

        result.metaData  = meta;
        result.navigator = new RowSetNavigatorClient();

        return result;
    }

    public static Result newSingleColumnResult(String colName, Type type) {

        Result result = newResult(ResultConstants.DATA);

        result.metaData            = ResultMetaData.newResultMetaData(1);
        result.metaData.columns[0] = new ColumnBase(null, null, null, colName);

        result.metaData.columns[0].setType(type);
        result.metaData.prepareData();

        //
        result.navigator = new RowSetNavigatorClient(8);

        return result;
    }

    public static Result newSingleColumnStringResult(String colName,
            String contents) {

        Result result = Result.newSingleColumnResult("OPERATION",
            Type.SQL_VARCHAR);
        LineNumberReader lnr =
            new LineNumberReader(new StringReader(contents));

        while (true) {
            String line = null;

            try {
                line = lnr.readLine();
            } catch (Exception e) {}

            if (line == null) {
                break;
            }

            result.getNavigator().add(new Object[]{ line });
        }

        return result;
    }

    public static Result newPrepareResponse(Statement statement) {

        Result r = newResult(ResultConstants.PREPARE_ACK);

        r.statement   = statement;
        r.statementID = statement.getID();

        int csType = statement.getType();

        r.statementReturnType =
            (csType == StatementTypes.SELECT_CURSOR || csType == StatementTypes
                .CALL) ? StatementTypes.RETURN_RESULT
                       : StatementTypes.RETURN_COUNT;
        r.metaData          = statement.getResultMetaData();
        r.parameterMetaData = statement.getParametersMetaData();

        return r;
    }

    public static Result newFreeStmtRequest(long statementID) {

        Result r = newResult(ResultConstants.FREESTMT);

        r.statementID = statementID;

        return r;
    }

    /**
     * For direct execution of SQL statements. The statement and other
     *  parameters are set afterwards as the Result is reused
     */
    public static Result newExecuteDirectRequest() {
        return newResult(ResultConstants.EXECDIRECT);
    }

    /**
     * For both EXECDIRECT and PREPARE
     */
    public void setPrepareOrExecuteProperties(String sql, int maxRows,
            int fetchSize, int statementReturnType, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability, int keyMode,
            int[] generatedIndexes, String[] generatedNames) {

        mainString               = sql;
        updateCount              = maxRows;
        this.fetchSize           = fetchSize;
        this.statementReturnType = statementReturnType;
        rsScrollability          = resultSetType;
        rsConcurrency            = resultSetConcurrency;
        rsHoldability            = resultSetHoldability;
        generateKeys             = keyMode;
        generatedMetaData =
            ResultMetaData.newGeneratedColumnsMetaData(generatedIndexes,
                generatedNames);
    }

    public static Result newSetSavepointRequest(String name) {

        Result result;

        result = newResult(ResultConstants.SETCONNECTATTR);

        result.setConnectionAttrType(ResultConstants.SQL_ATTR_SAVEPOINT_NAME);
        result.setMainString(name);

        return result;
    }

    public static Result newRequestDataResult(long id, int offset, int count) {

        Result result = newResult(ResultConstants.REQUESTDATA);

        result.id          = id;
        result.updateCount = offset;
        result.fetchSize   = count;

        return result;
    }

    public static Result newDataResult(ResultMetaData md) {

        Result result = newResult(ResultConstants.DATA);

        result.navigator = new RowSetNavigatorClient();
        result.metaData  = md;

        return result;
    }

    public void setDataResultConcurrency(boolean isUpdatable) {
        rsConcurrency = isUpdatable ? ResultConstants.CONCUR_UPDATABLE
                                    : ResultConstants.CONCUR_READ_ONLY;
    }

    public void setDataResultConcurrency(int resultSetConcurrency) {
        rsConcurrency = resultSetConcurrency;
    }

    public void setDataResultHoldability(int resultSetHoldability) {
        rsHoldability = resultSetHoldability;
    }

    public void setDataResultScrollability(int resultSetScrollability) {
        rsScrollability = resultSetScrollability;
    }

    /**
     * For DATA
     */
    public void setDataResultProperties(int maxRows, int fetchSize,
                                        int resultSetScrollability,
                                        int resultSetConcurrency,
                                        int resultSetHoldability) {

        updateCount     = maxRows;
        this.fetchSize  = fetchSize;
        rsScrollability = resultSetScrollability;
        rsConcurrency   = resultSetConcurrency;
        rsHoldability   = resultSetHoldability;
    }

    public static Result newDataHeadResult(SessionInterface session,
                                           Result source, int offset,
                                           int count) {

        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }

        Result result = newResult(ResultConstants.DATAHEAD);

        result.metaData = source.metaData;
        result.navigator = new RowSetNavigatorClient(source.navigator, offset,
                count);

        result.navigator.setId(source.navigator.getId());
        result.setSession(session);

        result.rsConcurrency   = source.rsConcurrency;
        result.rsHoldability   = source.rsHoldability;
        result.rsScrollability = source.rsScrollability;
        result.fetchSize       = source.fetchSize;

        return result;
    }

    public static Result newDataRowsResult(Result source, int offset,
                                           int count) {

        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }

        Result result = newResult(ResultConstants.DATAROWS);

        result.id       = source.id;
        result.metaData = source.metaData;
        result.navigator = new RowSetNavigatorClient(source.navigator, offset,
                count);

        return result;
    }

    public static Result newDataRowsResult(RowSetNavigator navigator) {

        Result result = newResult(ResultConstants.DATAROWS);

        result.navigator = navigator;

        return result;
    }

    /**
     * Result structure used for set/get session attributes
     */
    public static Result newSessionAttributesResult() {

        Result result = newResult(ResultConstants.DATA);

        result.navigator = new RowSetNavigatorClient(1);
        result.metaData  = sessionAttributesMetaData;

        result.navigator.add(new Object[SessionInterface.INFO_LIMIT]);

        return result;
    }

    public static Result newErrorResult(Throwable t) {
        return newErrorResult(t, null);
    }

    /** @todo 1.9.0 fredt - move the messages to Error.java */
    public static Result newErrorResult(Throwable t, String statement) {

        Result result = newResult(ResultConstants.ERROR);

        if (t instanceof HsqlException) {
            result.exception  = (HsqlException) t;
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();

            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }

            result.errorCode = result.exception.getErrorCode();
        } else if (t instanceof OutOfMemoryError) {

            // gc() at this point may clear the memory allocated so far

            /** @todo 1.9.0 - review if it's better to gc higher up the stack */
            System.gc();
            t.printStackTrace();

            result.exception  = Error.error(ErrorCode.OUT_OF_MEMORY);
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();
            result.errorCode  = result.exception.getErrorCode();
        } else {
            t.printStackTrace();

            result.exception  = Error.error(ErrorCode.GENERAL_ERROR);
            result.mainString = result.exception.getMessage() + " " + t;
            result.subString  = result.exception.getSQLState();
            result.errorCode  = result.exception.getErrorCode();

            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }
        }

        return result;
    }

    public void write(DataOutputStream dataOut,
                      RowOutputInterface rowOut)
                      throws IOException, HsqlException {

        rowOut.reset();
        rowOut.writeByte(mode);

        int startPos = rowOut.size();

        rowOut.writeSize(0);

        switch (mode) {

            case ResultConstants.GETSESSIONATTR :
                rowOut.writeByte(statementReturnType);
                break;

            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;

            case ResultConstants.PREPARE :
                rowOut.writeByte(statementReturnType);
                rowOut.writeString(mainString);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                rowOut.writeByte(generateKeys);

                if (generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }
                break;

            case ResultConstants.FREESTMT :
                rowOut.writeLong(statementID);
                break;

            case ResultConstants.CLOSE_RESULT :
                rowOut.writeLong(id);
                break;

            case ResultConstants.EXECDIRECT :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeByte(statementReturnType);
                rowOut.writeString(mainString);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                rowOut.writeByte(generateKeys);

                if (generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }
                break;

            case ResultConstants.CONNECT :
                rowOut.writeString(databaseName);
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeInt(updateCount);
                break;

            case ResultConstants.ERROR :
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeInt(errorCode);
                break;

            case ResultConstants.CONNECTACKNOWLEDGE :
                rowOut.writeInt(databaseID);
                rowOut.writeLong(sessionID);
                break;

            case ResultConstants.UPDATECOUNT :
                rowOut.writeInt(updateCount);
                break;

            case ResultConstants.ENDTRAN : {
                int type = getActionType();

                rowOut.writeInt(type);                     // endtran type

                switch (type) {

                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        rowOut.writeString(mainString);    // savepoint name
                        break;

                    case ResultConstants.TX_COMMIT :
                    case ResultConstants.TX_ROLLBACK :
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }
            case ResultConstants.PREPARE_ACK :
                rowOut.writeByte(statementReturnType);
                rowOut.writeLong(statementID);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                metaData.write(rowOut);
                parameterMetaData.write(rowOut);
                break;

            case ResultConstants.CALL_RESPONSE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                metaData.write(rowOut);
                navigator.writeSimple(rowOut, metaData);
                break;

            case ResultConstants.EXECUTE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                navigator.writeSimple(rowOut, metaData);
                break;

            case ResultConstants.UPDATE_RESULT :
                rowOut.writeLong(id);
                rowOut.writeInt(getActionType());
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;

            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                metaData.write(rowOut);
                navigator.writeSimple(rowOut, metaData);

                break;
            }
            case ResultConstants.PARAM_METADATA : {
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);

                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                int type = getConnectionAttrType();

                rowOut.writeInt(type);                     // attr type / updateCount

                switch (type) {

                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        rowOut.writeString(mainString);    // savepoint name
                        break;

                    // case ResultConstants.SQL_ATTR_AUTO_IPD // always true
                    // default: // throw, but case never happens
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }

                break;
            }
            case ResultConstants.REQUESTDATA : {
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);

                break;
            }
            case ResultConstants.DATAROWS :
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;

            case ResultConstants.DATAHEAD :
            case ResultConstants.DATA :
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeShort(rsScrollability);
                rowOut.writeShort(rsConcurrency);
                rowOut.writeShort(rsHoldability);
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }

        rowOut.writeIntData(rowOut.size() - startPos, startPos);
        dataOut.write(rowOut.getOutputStream().getBuffer(), 0, rowOut.size());

        int    count   = getLobCount();
        Result current = this;

        for (int i = 0; i < count; i++) {
            ResultLob lob = current.lobResults;

            lob.writeBody(dataOut);

            current = current.lobResults;
        }

        if (chainedResult == null) {
            dataOut.writeByte(ResultConstants.NONE);
        } else {
            chainedResult.write(dataOut, rowOut);
        }

        dataOut.flush();
    }

    public int getType() {
        return mode;
    }

    public boolean isData() {
        return mode == ResultConstants.DATA
               || mode == ResultConstants.DATAHEAD;
    }

    public boolean isError() {
        return mode == ResultConstants.ERROR;
    }

    public boolean isUpdateCount() {
        return mode == ResultConstants.UPDATECOUNT;
    }

    public boolean isSimpleValue() {
        return mode == ResultConstants.VALUE;
    }

    public boolean hasGeneratedKeys() {
        return mode == ResultConstants.UPDATECOUNT && chainedResult != null;
    }

    public HsqlException getException() {
        return exception;
    }

    public long getStatementID() {
        return statementID;
    }

    public void setStatementID(long statementId) {
        this.statementID = statementId;
    }

    public String getMainString() {
        return mainString;
    }

    public void setMainString(String sql) {
        this.mainString = sql;
    }

    public String getSubString() {
        return subString;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public Object getValueObject() {
        return valueData;
    }

    public void setValueObject(Object value) {
        valueData = value;
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setMaxRows(int count) {
        updateCount = count;
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    public void setFetchSize(int count) {
        fetchSize = count;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public int getConnectionAttrType() {
        return updateCount;
    }

    public void setConnectionAttrType(int type) {
        updateCount = type;
    }

    public int getActionType() {
        return updateCount;
    }

    public void setActionType(int type) {
        updateCount = type;
    }

    public long getSessionId() {
        return sessionID;
    }

    public void setSessionId(long id) {
        sessionID = id;
    }

    public void setSession(SessionInterface session) {

        if (navigator != null) {
            navigator.setSession(session);
        }
    }

    public int getDatabaseId() {
        return databaseID;
    }

    public void setDatabaseId(int id) {
        databaseID = id;
    }

    public long getResultId() {
        return id;
    }

    public void setResultId(long id) {

        this.id = id;

        if (navigator != null) {
            navigator.setId(id);
        }
    }

    public void setUpdateCount(int count) {
        updateCount = count;
    }

    public void setAsTransactionEndRequest(int subType, String savepoint) {

        mode        = ResultConstants.ENDTRAN;
        updateCount = subType;
        mainString  = savepoint == null ? ""
                                        : savepoint;
    }

    public Object[] getSingleRowData() {

        Object[] data = (Object[]) initialiseNavigator().getNext();

        data = (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                metaData.getColumnCount());

        return data;
    }

    public Object[] getParameterData() {
        return ((RowSetNavigatorClient) navigator).getData(0);
    }

    public Object[] getSessionAttributes() {
        return (Object[]) initialiseNavigator().getNext();
    }

    public void setResultType(int type) {
        mode = (byte) type;
    }

    public void setStatementType(int type) {
        statementReturnType = type;
    }

    public int getStatementType() {
        return statementReturnType;
    }

    public int getGeneratedResultType() {
        return generateKeys;
    }

    public ResultMetaData getGeneratedResultMetaData() {
        return generatedMetaData;
    }

    public Result getChainedResult() {
        return chainedResult;
    }

    public Result getUnlinkChainedResult() {

        Result result = chainedResult;

        chainedResult = null;

        return result;
    }

    public void addChainedResult(Result result) {

        Result current = this;

        while (current.chainedResult != null) {
            current = current.chainedResult;
        }

        current.chainedResult = result;
    }

    public int getLobCount() {
        return lobCount;
    }

    public ResultLob getLOBResult() {
        return lobResults;
    }

    public void addLobResult(ResultLob result) {

        Result current = this;

        while (current.lobResults != null) {
            current = current.lobResults;
        }

        current.lobResults = result;

        lobCount++;
    }

    public void clearLobResults() {
        lobResults = null;
        lobCount   = 0;
    }

//----------- Navigation
    RowSetNavigator navigator;

    public RowSetNavigator getNavigator() {
        return navigator;
    }

    public void setNavigator(RowSetNavigator navigator) {
        this.navigator = navigator;
    }

    public RowSetNavigator initialiseNavigator() {

        switch (mode) {

            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.EXECUTE :
            case ResultConstants.UPDATE_RESULT :
            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator.beforeFirst();

                return navigator;

            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
                navigator.reset();

                return navigator;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }
    }

    /************************* Volt DB Extensions *************************/
    public boolean hasError() { return mode == ResultConstants.ERROR; }
    /**********************************************************************/
}
