/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

public class AsyncCompilerResult implements Serializable {
    private static final long serialVersionUID = -1538141431615585812L;

    public long clientHandle = -1;
    public String errorMsg = null;
    public long connectionId = -1;
    public String hostname = "";
    public boolean adminConnection = false;
    public int expectedCatalogVersion = -1;
    transient public Object clientData = null;

    @Override
    public String toString() {
        String retval = "clientHandle:" + String.valueOf(clientHandle) + ", ";
        retval += "connectionId:" + String.valueOf(connectionId) + ", ";
        retval += "\n  errorMsg: " + ((errorMsg != null) ? errorMsg : "null");
        return retval;
    }
}
