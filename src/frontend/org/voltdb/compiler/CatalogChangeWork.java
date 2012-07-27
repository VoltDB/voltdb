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


public class CatalogChangeWork extends AsyncCompilerWork {
    private static final long serialVersionUID = -5257248292283453286L;

    final byte[] catalogBytes;
    final String deploymentString;

    public CatalogChangeWork(
            long replySiteId,
            long clientHandle, long connectionId, String hostname, boolean adminConnection,
            Object clientData, byte[] catalogBytes, String deploymentString,
            AsyncCompilerWorkCompletionHandler completionHandler)
    {
        super(replySiteId, false, clientHandle, connectionId, hostname,
              adminConnection, clientData,
              completionHandler);
        this.catalogBytes = catalogBytes.clone();
        this.deploymentString = deploymentString;
    }

}
