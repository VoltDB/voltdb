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

import java.sql.SQLException;
import java.sql.Connection;

/**
 * @author Jakob Jenkov
 */
public class ConnectionDefaults {

    private int     holdability          = -1;
    private int     transactionIsolation = -1;
    private boolean isAutoCommit         = true;
    private boolean isReadOnly           = false;
    private String  catalog              = null;

    public ConnectionDefaults() {
    }

    public ConnectionDefaults(Connection connection) throws SQLException {

        this.holdability          = connection.getHoldability();
        this.transactionIsolation = connection.getTransactionIsolation();
        this.isAutoCommit         = connection.getAutoCommit();
        this.isReadOnly           = connection.isReadOnly();
        this.catalog              = connection.getCatalog();
    }

    public int getHoldability() throws SQLException {
        return this.holdability;
    }

    public void setHoldability(int holdability) throws SQLException {
        this.holdability = holdability;
    }

    public int getTransactionIsolation() throws SQLException {
        return this.transactionIsolation;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
    }

    public boolean getAutoCommit() throws SQLException {
        return this.isAutoCommit;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.isAutoCommit = autoCommit;
    }

    public boolean isReadOnly() throws SQLException {
        return this.isReadOnly;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.isReadOnly = readOnly;
    }

    public String getCatalog() throws SQLException {
        return this.catalog;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.catalog = catalog;
    }

    public void setDefaults(Connection connection) throws SQLException {

        connection.setHoldability(this.holdability);

        if (this.transactionIsolation != Connection.TRANSACTION_NONE) {
            connection.setTransactionIsolation(this.transactionIsolation);
        }
        connection.setAutoCommit(this.isAutoCommit);
        connection.setReadOnly(this.isReadOnly);
        connection.setCatalog(this.catalog);
    }
}
