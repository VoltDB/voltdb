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


package org.hsqldb_voltpatches.jdbc.pool;

import org.hsqldb_voltpatches.jdbc.JDBCConnection;

import java.sql.SQLException;
import java.sql.Savepoint;

// @(#)$Id: JDBCXAConnectionWrapper.java 5026 2012-07-14 20:02:27Z fredt $

/**
 * This is a wrapper class for JDBCConnection objects (not java.sql.XAConnection
 * objects).
 * Purpose of this class is to intercept and handle XA-related operations
 * according to chapter 12 of the JDBC 3.0 specification, by returning this
 * wrapped JDBCConnection to end-users.
 * Global transaction services and XAResources will not use this wrapper.
 * <P>
 * The new implementation extends JDBCConnection. A new object is created
 * based on the session / session proxy of the JDBCXAConnection object in the
 * constructor. (fredt)<p>
 *
 * @version 2.2.9
 * @since 2.0.0
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @see org.hsqldb_voltpatches.jdbc.JDBCConnection
 */
public class JDBCXAConnectionWrapper extends JDBCConnection {

    /*
     * A critical question:  One responsibility of this
     * class is to intercept invocations of commit(), rollback(),
     * savePoint methods, etc.  But, what about if user issues the
     * corresponding SQL commands?  What is the point to intercepting
     * Connection.commit() here if end-users can execute the SQL command
     * "COMMIT" and bypass interception?
     * Similarly, what about DDL commands that cause an explicit commit?
     *                                                - blaine
     * If we want, we can stop various statement categories from running
     * during an XA transaction. May do so in the future - fredt
     */

    /**
     * Interceptor method, because this method is prohibited within
     * any global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        validateNotWithinTransaction();
        super.setAutoCommit(autoCommit);
    }

    /**
     * Interceptor method, because this method is prohibited within
     * any global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public void commit() throws SQLException {
        validateNotWithinTransaction();
        super.commit();
    }

    /**
     * Interceptor method, because this method is prohibited within
     * any global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public void rollback() throws SQLException {
        validateNotWithinTransaction();
        super.rollback();
    }

    /**
     * Interceptor method, because this method is prohibited within
     * any global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        validateNotWithinTransaction();
        super.rollback(savepoint);
    }

    /**
     * Interceptor method, because this method is prohibited within
     * any global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public Savepoint setSavepoint() throws SQLException {

        validateNotWithinTransaction();

        return super.setSavepoint();
    }

    /**
     * Interceptor method, because this method is prohibited within
     * any global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public Savepoint setSavepoint(String name) throws SQLException {

        validateNotWithinTransaction();

        return super.setSavepoint(name);
    }

    /**
     * Interceptor method, because there may be XA implications to
     * calling the method within a global transaction.
     * See section 1.2.4 of the JDBC 3.0 spec.<p>
     *
     * HSQLDB does not allow changing the isolation level inside a transaction
     * of any kind.<p>
     */
    public void setTransactionIsolation(int level) throws SQLException {
        validateNotWithinTransaction();
        super.setTransactionIsolation(level);
    }

    //---------------------- NON-INTERFACE METHODS -----------------------------
    private JDBCXAResource xaResource;

    public JDBCXAConnectionWrapper(JDBCXAResource xaResource,
                                   JDBCXAConnection xaConnection,
                                   JDBCConnection databaseConnection)
                                   throws SQLException {
        // todo: Review JDBCXADataSource and this class.
        //       Under current implementation, because we do not pass a
        //       JDBCXAConnection instance to the constructor to pick
        //       up the connectionClosed event listener callback, calling
        //       close() on this wrapper connection does not reset the
        //       physical connection or set the inUse flag to false until
        //       the vending JDBCXAConnection itself is closed, which marks
        //       the end of its useful life.
        //
        //       In other words, due to this current implementation detail,
        //       JDBCXADataSource cannot cooperate with a pooling implementation
        //       to reuse physical connections.
        //       fixed - the event listener works
        super(databaseConnection, xaConnection);

        xaResource.setConnection(this);

        this.xaResource = xaResource;
    }

    /**
     * Throws a SQLException if within a Global transaction.
     *
     * @throws SQLException if within a Global transaction.
     */
    private void validateNotWithinTransaction() throws SQLException {

        if (xaResource.withinGlobalTransaction()) {
            throw new SQLException(
                "Method prohibited within a global transaction");
        }
    }
}
