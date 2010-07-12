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


package org.hsqldb_voltpatches.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.hsqldb_voltpatches.ErrorCode;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;

/**
 * Base class for HSQLDB's implementations of java.sql.Statement and
 * java.sql.PreparedStatement. Contains common members and methods.
 *
 * @author fredt@usrs
 * @version 1.9.0
 * @since 1.9.0
 */

/**
 * JDBC specification.
 *
 * Closing the Statement closes the ResultSet instance returned. But:
 *
 * Statement can be executed multiple times and return several results. With
 * normal Statement objects, each execution can be for a completely different
 * query. PreparedStatement instances are specifically for multiple use over
 * multiple transactions.
 *
 * ResultSets may be held over commits and span several transactions.
 *
 * There is no real relation between the current state fo an Statement instance
 * and the various ResultSets that it may have returned for different queries.
 */
/**
 * @todo 1.9.0 - review the following issues:
 *
 * Does not always close ResultSet object directly when closed. Although RS
 * objects will eventually be closed when accessed, the change is not reflected
 * to the server, impacting ResultSets that are held.
 */
class JDBCStatementBase {

    /**
     * Whether this Statement has been explicitly closed.  A JDBCConnection
     * object now explicitly closes all of its open JDBC*Statement objects
     * when it is closed.
     */
    volatile boolean isClosed;

    /** Is escape processing enabled? */
    protected boolean isEscapeProcessing = true;

    /** The connection used to execute this statement. */
    protected JDBCConnection connection;

    /** The maximum number of rows to generate when executing this statement. */
    protected int maxRows;

    /** The number of rows returned in a chunk. */
    protected int fetchSize = 0;

    /** Direction of results fetched. */
    protected int fetchDirection = JDBCResultSet.FETCH_FORWARD;

    /** The result of executing this statement. */
    protected Result resultIn;

    /** Any error returned from a batch execute. */
    protected Result errorResult;

    /** The currently existing generated key Result */
    protected Result generatedResult;

    /** The result set type obtained by executing this statement. */
    protected int rsScrollability = JDBCResultSet.TYPE_FORWARD_ONLY;

    /** The result set concurrency obtained by executing this statement. */
    protected int rsConcurrency = JDBCResultSet.CONCUR_READ_ONLY;

    /** The result set holdability obtained by executing this statement. */
    protected int rsHoldability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;

    /** Used by this statement to communicate non-batched requests. */
    protected Result resultOut;

    /** Used by this statement to communicate batched execution requests */
    protected Result batchResultOut;

    /** The currently existing ResultSet object */
    protected JDBCResultSet currentResultSet;

    /** The currently existing ResultSet object for generated keys */
    protected JDBCResultSet generatedResultSet;

    /** The first warning in the chain. Null if there are no warnings. */
    protected SQLWarning rootWarning;

    /** Counter for ResultSet in getMoreResults(). */
    protected int resultSetCounter;

    /** Implementation in subclasses **/
    public synchronized void close() throws SQLException {}

    /**
     * An internal check for closed statements.
     *
     * @throws SQLException when the connection is closed
     */
    void checkClosed() throws SQLException {

        if (isClosed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }

        if (connection.isClosed) {
            close();
            throw Util.sqlException(ErrorCode.X_08503);
        }
    }

    /**
     * processes chained warnings and any generated columns result set
     */
    void performPostExecute() throws SQLException {

        resultOut.clearLobResults();

        generatedResult = null;

        if (resultIn == null) {
            return;
        }

        Result current = resultIn;

        while (current.getChainedResult() != null) {
            current = current.getUnlinkChainedResult();

            if (current.getType() == ResultConstants.WARNING) {
                SQLWarning w = Util.sqlWarning(current);

                if (rootWarning == null) {
                    rootWarning = w;
                } else {
                    rootWarning.setNextWarning(w);
                }
            } else if (current.getType() == ResultConstants.ERROR) {
                errorResult = current;
            } else if (current.getType() == ResultConstants.DATA) {
                generatedResult = current;
            }
        }

        if (resultIn.isData()) {
            currentResultSet = new JDBCResultSet(connection.sessionProxy,
                    this, resultIn, resultIn.metaData,
                    connection.connProperties);
        }
    }

    int getUpdateCount() throws SQLException {

        checkClosed();

        return (resultIn == null || resultIn.isData()) ? -1
                : resultIn.getUpdateCount();
    }


    ResultSet getResultSet() throws SQLException {

        checkClosed();

        ResultSet result = currentResultSet;
        currentResultSet = null;
        return result;
    }

    boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    /**
     * Note yet correct for multiple ResultSets. Should keep track of the
     * previous ResultSet objects to be able to close them
     */
    boolean getMoreResults(int current) throws SQLException {
        checkClosed();

        if (resultIn == null || !resultIn.isData()) {
            return false;
        }

        if (resultSetCounter == 0) {
            resultSetCounter++;
            return true;
        }

        if (currentResultSet != null && current != KEEP_CURRENT_RESULT) {
            currentResultSet.close();
        }

        resultIn = null;

        return false;
    }

    ResultSet getGeneratedResultSet() throws SQLException {

        if (generatedResultSet != null) {
            generatedResultSet.close();
        }

        if (generatedResult == null) {
            generatedResult = Result.emptyGeneratedResult;
        }
        generatedResultSet = new JDBCResultSet(connection.sessionProxy, null,
                generatedResult, generatedResult.metaData,
                connection.connProperties);

        return generatedResultSet;
    }

    /**
     * See comment for getMoreResults.
     */
    void closeResultData() throws SQLException {

        if (currentResultSet != null) {
            currentResultSet.close();
        }

        if (generatedResultSet != null) {
            generatedResultSet.close();
        }
        generatedResultSet = null;
        generatedResult    = null;
        resultIn           = null;
    }

    /**
     * JDBC 3 constants
     */
    static final int CLOSE_CURRENT_RESULT = 1;
    static final int KEEP_CURRENT_RESULT = 2;
    static final int CLOSE_ALL_RESULTS = 3;
    static final int SUCCESS_NO_INFO = -2;
    static final int EXECUTE_FAILED = -3;
    static final int RETURN_GENERATED_KEYS = 1;
    static final int NO_GENERATED_KEYS = 2;

}
