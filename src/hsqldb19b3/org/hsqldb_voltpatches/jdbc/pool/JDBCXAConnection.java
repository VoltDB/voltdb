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

import javax.sql.XAConnection;

import java.sql.SQLException;

import javax.transaction.xa.XAResource;

// @(#)$Id: JDBCXAConnection.java 771 2009-01-12 15:21:22Z fredt $

/**
 * HSQLDB Connection objects for use by a global transaction service manager.
 *
 * Other than the xaResource, this class deals with Connection Wrappers
 * not the real thing, but these correspond 1:1 with physical
 * JDBCConnections.
 *
 * @since HSQLDB v. 1.9.0
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @see javax.sql.XAConnection
 */
public class JDBCXAConnection extends JDBCPooledConnection implements XAConnection {

    JDBCXAResource xaResource;

    public JDBCXAConnection(JDBCXAConnectionWrapper xaConnectionWrapper,
                            JDBCXAResource xaResource) {

        super(xaConnectionWrapper);

        this.xaResource = xaResource;
    }

    public XAResource getXAResource() throws SQLException {
        return xaResource;
    }
}
