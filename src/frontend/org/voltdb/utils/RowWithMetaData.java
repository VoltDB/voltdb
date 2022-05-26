/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
package org.voltdb.utils;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;

//Processor queue to keep track of line data and number and such.
public class RowWithMetaData implements BulkLoaderSuccessCallback {

    final public Object rawLine;
    final public long lineNumber;
    final public ProcedureCallback procedureCallback;
    private static final VoltLogger log = new VoltLogger(RowWithMetaData.class.getName());

    public RowWithMetaData(Object rawLine, long ln) {
        this.rawLine = rawLine;
        this.lineNumber = ln;
        this.procedureCallback = null;
    }

    public RowWithMetaData(Object rawLine, long ln, ProcedureCallback cb) {
        this.rawLine = rawLine;
        this.lineNumber = ln;
        this.procedureCallback = cb;
    }

    @Override
    public void success(Object rowHandle, ClientResponse response) {
        if (procedureCallback != null) {
            try {
                procedureCallback.clientCallback(response);
            }
            catch (Exception e) {
                log.error("Exception in client callback", e);
            }
        }
    }


}
