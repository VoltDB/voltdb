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
package org.voltdb;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.network.WriteStream;

/**
 * Check if the parameters of the sysproc can be deserialized successfully.
 */
public class ParameterDeserializationPolicy extends InvocationAcceptancePolicy {
    public ParameterDeserializationPolicy(boolean isOn) {
        super(isOn);
    }

    @Override
    public boolean shouldAccept(AuthUser user,
                                StoredProcedureInvocation invocation,
                                Config sysProc,
                                WriteStream s) {
        try {
            invocation.getParams();
        } catch (RuntimeException e) {
            Writer result = new StringWriter();
            PrintWriter pw = new PrintWriter(result);
            e.printStackTrace(pw);
            final ClientResponseImpl errorResponse =
                new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                       new VoltTable[0],
                                       "Exception while deserializing procedure params\n" +
                                       result.toString(),
                                       invocation.clientHandle);
            s.enqueue(errorResponse);
            return false;
        }

        return true;
    }
}
