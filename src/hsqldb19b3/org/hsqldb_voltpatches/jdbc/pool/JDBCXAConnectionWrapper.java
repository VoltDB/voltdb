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


package org.hsqldb_voltpatches.jdbc.pool;

import org.hsqldb_voltpatches.jdbc.JDBCConnection;

import java.sql.SQLException;
import java.sql.Savepoint;

// @(#)$Id: JDBCXAConnectionWrapper.java 771 2009-01-12 15:21:22Z fredt $

/**
 * This is a wrapper class for JDBCConnection objects (not XAConnection
 * object).
 * Purpose of this class is to intercept and handle XA-related operations
 * according to chapter 12 of the JDBC 3.0 specification, by returning this
 * wrapped JDBCConnection to end-users.
 * Global transaction services and XAResources will not use this wrapper.
 * It also supports pooling, by virtue of the parent class,
 * LifeTimeConnectionWrapper.
 * <P>
 * This class name would be very precise (the class being a wrapper for
 * XA-capable JDBC Connections), except that a "XAConnection" is an entirely
 * different thing from a JDBC java.sql.Connection.
 * I can think of no way to eliminate the ambiguity without using an
 * 80-character class name.
 * Best I think I can do is to clearly state here that
 * <b>This is a wrapper for XA-capable java.sql.Connections, not for
 *    javax.sql.XAConnection.</b>
 *
 * @since HSQLDB v. 1.9.0
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @see org.hsqldb_voltpatches.jdbc.JDBCConnection
 * @see org.hsqldb_voltpatches.jdbc.pool.LifeTimeConnectionWrapper
 */
public class JDBCXAConnectionWrapper extends LifeTimeConnectionWrapper {

    /*
     * A critical question:  One responsibility of this
     * class is to intercept invocations of commit(), rollback(),
     * savePoint methods, etc.  But, what about if user issues the
     * corresponding SQL commands?  What is the point to intercepting
     * Connection.commit() here if end-users can execute the SQL command
     * "COMMIT" and bypass interception?
     * Similarly, what about DDL commands that cause an explicit commit?
     *                                                - blaine
     */
    private JDBCXAResource xaResource;

    public JDBCXAConnectionWrapper(
            JDBCConnection connection, JDBCXAResource xaResource,
            ConnectionDefaults connectionDefaults) throws SQLException {

        /* Could pass in the creating XAConnection, which has methods to
         * get the connection and the xaResource, but this way cuts down
         * on the inter-dependencies a bit. */
        super(connection, connectionDefaults);

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
     * See section 1.2.4 of the JDBC 3.0 spec.
     */
    public void setTransactionIsolation(int level) throws SQLException {

        /* Goal at this time is to get a working XA DataSource.
         * After we have multiple transaction levels working, we can
         * consider how we want to handle attempts to change the level
         * within a global transaction. */
        validateNotWithinTransaction();
        super.setTransactionIsolation(level);
    }
}
