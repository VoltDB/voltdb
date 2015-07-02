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


package org.hsqldb_voltpatches.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.hsqldb_voltpatches.StatementTypes;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.result.Result;
import org.hsqldb_voltpatches.result.ResultConstants;

/**
 * Base class for HSQLDB's implementations of java.sql.Statement and
 * java.sql.PreparedStatement. Contains common members and methods.
 *
 * @author fredt@usrs
 * @version 2.3.0
 * @since 1.9.0
 * @revised JDK 1.7, HSQLDB 2.0.1
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
     * object now explicitly closes all of its open JDBC Statement objects
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

    /** The combined result set properties obtained by executing this statement. */
    protected int rsProperties;

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

    /** Query timeout in seconds */
    protected int queryTimeout;

    /** connection generation */
    int connectionIncarnation;

    /** Implementation in subclasses */
    public synchronized void close() throws SQLException {}

    /**
     * An internal check for closed statements.
     *
     * @throws SQLException when the connection is closed
     */
    void checkClosed() throws SQLException {

        if (isClosed) {
            throw JDBCUtil.sqlException(ErrorCode.X_07501);
        }

        if (connection.isClosed) {
            close();

            throw JDBCUtil.sqlException(ErrorCode.X_08503);
        }

        if (connectionIncarnation != connection.incarnation) {
            throw JDBCUtil.sqlException(ErrorCode.X_08503);
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

        rootWarning = null;

        Result current = resultIn;

        while (current.getChainedResult() != null) {
            current = current.getUnlinkChainedResult();

            if (current.getType() == ResultConstants.WARNING) {
                SQLWarning w = JDBCUtil.sqlWarning(current);

                if (rootWarning == null) {
                    rootWarning = w;
                } else {
                    rootWarning.setNextWarning(w);
                }
            } else if (current.getType() == ResultConstants.ERROR) {
                errorResult = current;
            } else if (current.getType() == ResultConstants.GENERATED) {
                generatedResult = current;
            } else if (current.getType() == ResultConstants.DATA) {
                resultIn.addChainedResult(current);
            }
        }

        if (rootWarning != null) {
            connection.setWarnings(rootWarning);
        }
    }

    int getUpdateCount() throws SQLException {

        checkClosed();

        return (resultIn == null || resultIn.isData()) ? -1
                                                       : resultIn
                                                       .getUpdateCount();
    }

    ResultSet getResultSet() throws SQLException {

        checkClosed();

        ResultSet result = currentResultSet;

        if(!connection.isCloseResultSet) {
            currentResultSet = null;
        }

        if (result == null) {

            // if statement has been used with executeQuery and the result is update count
            // return an empty result for 1.8 compatibility
            if (resultOut.getStatementType() == StatementTypes.RETURN_RESULT) {
                return JDBCResultSet.newEptyResultSet();
            }
        }

        return result;
    }

    boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }

    /**
     * Not yet correct for multiple ResultSets. Should keep track of all
     * previous ResultSet objects to be able to close them
     */
    boolean getMoreResults(int current) throws SQLException {

        checkClosed();

        if (resultIn == null) {
            return false;
        }

        resultIn = resultIn.getChainedResult();

        if (currentResultSet != null) {
            if( current != KEEP_CURRENT_RESULT) {
                currentResultSet.close();
            }
        }

        currentResultSet = null;

        if (resultIn != null) {
            currentResultSet = new JDBCResultSet(connection, this, resultIn,
                                                 resultIn.metaData);

            return true;
        }

        return false;
    }

    ResultSet getGeneratedResultSet() throws SQLException {

        if (generatedResultSet != null) {
            generatedResultSet.close();
        }

        if (generatedResult == null) {
            generatedResult = Result.emptyGeneratedResult;
        }

        generatedResultSet = new JDBCResultSet(connection, null,
                                               generatedResult,
                                               generatedResult.metaData);

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
        currentResultSet   = null;
    }

    /**
     * JDBC 3 constants
     */
    static final int CLOSE_CURRENT_RESULT  = 1;
    static final int KEEP_CURRENT_RESULT   = 2;
    static final int CLOSE_ALL_RESULTS     = 3;
    static final int SUCCESS_NO_INFO       = -2;
    static final int EXECUTE_FAILED        = -3;
    static final int RETURN_GENERATED_KEYS = 1;
    static final int NO_GENERATED_KEYS     = 2;

    //--------------------------JDBC 4.1 -----------------------------

    /**
     * Specifies that this {@code Statement} will be closed when all its
     * dependent result sets are closed. If execution of the {@code Statement}
     * does not produce any result sets, this method has no effect.
     * <p>
     * <strong>Note:</strong> Multiple calls to {@code closeOnCompletion} do
     * not toggle the effect on this {@code Statement}. However, a call to
     * {@code closeOnCompletion} does effect both the subsequent execution of
     * statements, and statements that currently have open, dependent,
     * result sets.
     *
     * @throws SQLException if this method is called on a closed
     * {@code Statement}
     * @since JDK 1.7 M11 2010/09/10 (b123), HSQLDB 2.0.1
     */
    public void closeOnCompletion() throws SQLException {
        checkClosed();
    }

    /**
     * Returns a value indicating whether this {@code Statement} will be
     * closed when all its dependent result sets are closed.
     * @return {@code true} if the {@code Statement} will be closed when all
     * of its dependent result sets are closed; {@code false} otherwise
     * @throws SQLException if this method is called on a closed
     * {@code Statement}
     * @since JDK 1.7 M11 2010/09/10 (b123), HSQLDB 2.0.1
     */
    public boolean isCloseOnCompletion() throws SQLException {

        checkClosed();

        return false;
    }
}
