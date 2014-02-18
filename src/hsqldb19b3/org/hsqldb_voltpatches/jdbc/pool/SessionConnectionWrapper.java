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

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Wraps a connection from the connection pool during a session. A session lasts from the
 * connection is taken from the pool and until it is closed and returned to the pool. Each
 * time a connection is taken from the pool it is wrapped in a new SessionConnectionWrapper instance.
 * When the connection is closed the SessionConnectionWrapper is invalidated and thus that reference
 * to the connection is invalidated. There can only be one valid SessionConnectionWrapper
 * at any time per underlying connection.
 *
 * <br/><br/>
 * The SessionConnectionWrapper makes it possible to invalidate connection references independently
 * of each other, even if the different references point to the same underlying connection. This is useful
 * if a thread by accident keeps a reference to the connection after it is closed. If the thread
 * tries to call any methods on the connection the SessionConnectionWrapper will throw an SQLException.
 * Other threads, or even the same thread, that takes the connection from the pool subsequently will
 * get a new SessionConnectionWrapper. Therefore their reference to the connection is valid
 * even if earlier references have been invalidated.
 *
 * <br/><br/>
 * The SessionConnectionWrapper is also useful when a connection pool is to reclaim abandoned
 * connections (connections that by accident have not been closed). After having been out of
 * the pool and inactive for a certain time (set by the user),
 * the pool can decide that the connection must be abandoned. The pool will then close the
 * SessionConnectionWrapper and return the underlying connection to the pool.
 * If the thread that abandoned the connection tries to call any
 * methods on the SessionConnectionWrapper it will get an SQLException. The underlying connection
 * is still valid for the next thread that takes it from the pool.
 *
 *
 * @author Jakob Jenkov
 */
public class SessionConnectionWrapper extends BaseConnectionWrapper {

    protected long       latestActivityTime = 0;
    protected Connection connection         = null;

    public SessionConnectionWrapper(Connection connection) {

        this.connection = connection;

        updateLatestActivityTime();
    }

    protected Connection getConnection() {
        return this.connection;
    }

    public synchronized void updateLatestActivityTime() {
        this.latestActivityTime = System.currentTimeMillis();
    }

    public synchronized long getLatestActivityTime() {
        return latestActivityTime;
    }

    public void close() throws SQLException {

        this.isClosed = true;

        Connection temp = this.connection;

        this.connection = null;

        temp.close();
    }
}
