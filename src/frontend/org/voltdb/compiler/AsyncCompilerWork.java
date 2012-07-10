/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.io.Serializable;

public class AsyncCompilerWork implements Serializable {

    public interface AsyncCompilerWorkCompletionHandler {
        public void onCompletion(AsyncCompilerResult result);
    }
    private static final long serialVersionUID = 6588086204761949082L;

    final long replySiteId;

    final boolean shouldShutdown;
    final long clientHandle;
    final long connectionId;
    final String hostname;
    final boolean adminConnection;
    final transient public Object clientData;

    final AsyncCompilerWorkCompletionHandler completionHandler;

    public AsyncCompilerWork(long replySiteId, boolean shouldShutdown, long clientHandle,
            long connectionId, String hostname, boolean adminConnection,
            Object clientData,
            AsyncCompilerWorkCompletionHandler completionHandler)
    {
        this.replySiteId = replySiteId;
        this.shouldShutdown = shouldShutdown;
        this.clientHandle = clientHandle;
        this.connectionId = connectionId;
        this.hostname = hostname;
        this.adminConnection = adminConnection;
        this.clientData = clientData;
        this.completionHandler = completionHandler;
    }

    @Override
    public String toString() {
        String retval = "shouldShutdown:" + String.valueOf(shouldShutdown) + ", ";
        retval += "clientHandle:" + String.valueOf(clientHandle) + ", ";
        retval += "connectionId:" + String.valueOf(connectionId) + ", ";
        return retval;
    }
}
