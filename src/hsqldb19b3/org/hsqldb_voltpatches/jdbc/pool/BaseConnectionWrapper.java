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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

import org.hsqldb_voltpatches.jdbc.Util;

//#ifdef JAVA6
import java.sql.ClientInfoStatus;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLXML;

//#endif JAVA6

/* $Id: BaseConnectionWrapper.java 2933 2009-03-20 13:05:06Z unsaved $ */

// boucherb@users 20051207 - patch 1.8.0.x initial JDBC 4.0 support work
// boucherb@users 20060523 - patch 1.9.0 full synch up to Mustang Build 84
// Revision 1.7  2006/07/12 12:45:54  boucherb
// - full synch up to Mustang b90

/**
 * A base class for the two different types of connection wrappers:
 * SessionConnectionWrapper and LifeTimeConnectionWrapper
 *
 * @author Jakob Jenkov
 */
public abstract class BaseConnectionWrapper implements java.sql.Connection {

    protected boolean isClosed = false;

    protected abstract Connection getConnection();

    protected void validate() throws SQLException {

        if (isClosed) {
            throw Util.connectionClosedException();
        }
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    public int getHoldability() throws SQLException {

        validate();

        return getConnection().getHoldability();
    }

    public int getTransactionIsolation() throws SQLException {

        validate();

        return getConnection().getTransactionIsolation();
    }

    public void clearWarnings() throws SQLException {
        validate();
        getConnection().clearWarnings();
    }

    public void commit() throws SQLException {
        validate();
        this.getConnection().commit();
    }

    public void rollback() throws SQLException {
        validate();
        this.getConnection().rollback();
    }

    public boolean getAutoCommit() throws SQLException {
        return getConnection().getAutoCommit();
    }

    public boolean isReadOnly() throws SQLException {

        validate();

        return getConnection().isReadOnly();
    }

    public void setHoldability(int holdability) throws SQLException {
        validate();
        this.getConnection().setHoldability(holdability);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        validate();
        this.getConnection().setTransactionIsolation(level);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        validate();
        this.getConnection().setAutoCommit(autoCommit);
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        validate();
        this.getConnection().setReadOnly(readOnly);
    }

    public String getCatalog() throws SQLException {

        validate();

        return this.getConnection().getCatalog();
    }

    public void setCatalog(String catalog) throws SQLException {
        validate();
        this.getConnection().setCatalog(catalog);
    }

    public DatabaseMetaData getMetaData() throws SQLException {

        validate();

        return getConnection().getMetaData();
    }

    public SQLWarning getWarnings() throws SQLException {

        validate();

        return this.getConnection().getWarnings();
    }

    public Savepoint setSavepoint() throws SQLException {

        validate();

        return this.getConnection().setSavepoint();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        validate();
        this.getConnection().releaseSavepoint(savepoint);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        validate();
        this.getConnection().rollback(savepoint);
    }

    public Statement createStatement() throws SQLException {

        validate();

        return getConnection().createStatement();
    }

    public Statement createStatement(
            int resultSetType, int resultSetConcurrency) throws SQLException {

        validate();

        return this.getConnection().createStatement(resultSetType,
                resultSetConcurrency);
    }

    public Statement createStatement(
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        validate();

        return this.getConnection().createStatement(resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    public Map getTypeMap() throws SQLException {

        validate();

        return this.getConnection().getTypeMap();
    }

    public void setTypeMap(Map map) throws SQLException {
        validate();
        this.getConnection().setTypeMap(map);
    }

    public String nativeSQL(String sql) throws SQLException {

        validate();

        return this.getConnection().nativeSQL(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {

        validate();

        return this.getConnection().prepareCall(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {

        validate();

        return this.getConnection().prepareCall(sql, resultSetType,
                resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        validate();

        return this.getConnection().prepareCall(sql, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {

        validate();

        return this.getConnection().prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql,
            int autoGeneratedKeys) throws SQLException {

        validate();

        return this.getConnection().prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency) throws SQLException {

        validate();

        return this.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {

        validate();

        return this.getConnection().prepareStatement(sql, resultSetType,
                resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql,
            int columnIndexes[]) throws SQLException {

        validate();

        return this.getConnection().prepareStatement(sql, columnIndexes);
    }

    public Savepoint setSavepoint(String name) throws SQLException {

        validate();

        return this.getConnection().setSavepoint(name);
    }

    public PreparedStatement prepareStatement(String sql,
            String columnNames[]) throws SQLException {

        validate();

        return this.getConnection().prepareStatement(sql, columnNames);
    }

    //------------------------- JDBC 4.0 -----------------------------------
//#ifdef JAVA6
    public Clob createClob() throws SQLException {

        validate();

        return this.getConnection().createClob();
    }

//#endif JAVA6
//#ifdef JAVA6
    public Blob createBlob() throws SQLException {

        validate();

        return this.getConnection().createBlob();
    }

//#endif JAVA6
//#ifdef JAVA6
    public NClob createNClob() throws SQLException {

        validate();

        return this.getConnection().createNClob();
    }

//#endif JAVA6
//#ifdef JAVA6
    public SQLXML createSQLXML() throws SQLException {

        validate();

        return this.getConnection().createSQLXML();
    }

//#endif JAVA6
//#ifdef JAVA6
    public boolean isValid(int timeout) throws SQLException {

        validate();

        return this.getConnection().isValid(timeout);
    }

//#endif JAVA6
//#ifdef JAVA6
    public void setClientInfo(String name,
                              String value) throws SQLClientInfoException {

        try {
            validate();
        } catch (SQLException e) {
            throw new SQLClientInfoException(e.getMessage(), e.getSQLState(),
                    e.getErrorCode(), (Map<String, ClientInfoStatus>) null, e);
        }
        this.getConnection().setClientInfo(name, value);
    }

//#endif JAVA6
//#ifdef JAVA6
    public void setClientInfo(
            Properties properties) throws SQLClientInfoException {

        try {
            validate();
        } catch (SQLException e) {
            throw new SQLClientInfoException(e.getMessage(), e.getSQLState(),
                    e.getErrorCode(), (Map<String, ClientInfoStatus>) null, e);
        }
        this.getConnection().setClientInfo(properties);
    }

//#endif JAVA6
//#ifdef JAVA6
    public String getClientInfo(String name) throws SQLException {

        validate();

        return this.getConnection().getClientInfo(name);
    }

//#endif JAVA6
//#ifdef JAVA6
    public Properties getClientInfo() throws SQLException {

        validate();

        return this.getConnection().getClientInfo();
    }

//#endif JAVA6
//#ifdef JDBC4BETA
/*
    public <T extends BaseQuery> T createQueryObject(java.lang.Class<T> ifc) throws SQLException {
        validate();
        return this.getConnection().createQueryObject(ifc);
    }
*/

//#endif JAVA6
//#ifdef JDBC4BETA
/*
    public <T extends BaseQuery> T createQueryObject(Class<T> ifc, Connection con) throws SQLException {
        validate();
        return this.getConnection().createQueryObject(ifc, con);
    }
*/

//#endif JAVA6
//#ifdef JAVA6
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        validate();

        if (this.isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }

//#endif JAVA6
//#ifdef JAVA6
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }

//#endif JAVA6
// --------------------------- Added: Mustang Build 80 -------------------------
//#ifdef JAVA6
    // renamed from createArray - b87
    public Array createArrayOf(String typeName,
                               Object[] elements) throws SQLException {

        validate();

        return this.getConnection().createArrayOf(typeName, elements);
    }

//#endif JAVA6
//#ifdef JAVA6
    public Struct createStruct(String typeName,
                               Object[] attributes) throws SQLException {

        validate();

        return this.getConnection().createStruct(typeName, attributes);
    }

//#endif JAVA6

    /************************* Volt DB Extensions *************************/

    public void setSchema(String schema) throws SQLException {
        throw new SQLException();
    }

    public String getSchema() throws SQLException {
        throw new SQLException();
    }

    public void abort(java.util.concurrent.Executor executor) throws SQLException {
        throw new SQLException();
    }

    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds)
            throws SQLException {
        throw new SQLException();
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLException();
    }
    /**********************************************************************/
}
