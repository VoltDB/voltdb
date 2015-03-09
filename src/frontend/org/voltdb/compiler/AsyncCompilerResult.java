/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.io.Serializable;

import org.voltdb.AuthSystem;

public class AsyncCompilerResult implements Serializable, Cloneable {
    private static final long serialVersionUID = -1538141431615585812L;

    public long clientHandle = -1;
    public String errorMsg = null;
    public long connectionId = -1;
    public String hostname = "";
    public boolean adminConnection = false;
    public int expectedCatalogVersion = -1;
    public AuthSystem.AuthUser user = null;
    transient public Object clientData = null;

    /**
     * Build an error response based on the provided work.
     */
    public static AsyncCompilerResult makeErrorResult(AsyncCompilerWork work, String errMsg)
    {
        AsyncCompilerResult result = new AsyncCompilerResult();
        result.clientHandle = work.clientHandle;
        result.connectionId = work.connectionId;
        result.hostname = work.hostname;
        result.adminConnection = work.adminConnection;
        result.clientData = work.clientData;
        result.errorMsg = errMsg;
        result.user = work.user;
        return result;
    }

    @Override
    public String toString() {
        String retval = "clientHandle:" + String.valueOf(clientHandle) + ", ";
        retval += "connectionId:" + String.valueOf(connectionId) + ", ";
        retval += "\n  errorMsg: " + ((errorMsg != null) ? errorMsg : "null");
        return retval;
    }
}
