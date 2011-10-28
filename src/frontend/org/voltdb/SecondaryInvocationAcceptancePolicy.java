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
package org.voltdb;

import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.network.WriteStream;

/**
 * An invocation acceptance policy targeting all types of invocations in
 * secondary cluster. Secondary cluster only accepts read-only procedures from
 * normal clients, and write procedures from DR agent.
 */
public class SecondaryInvocationAcceptancePolicy extends InvocationAcceptancePolicy {
    public SecondaryInvocationAcceptancePolicy(boolean isOn) {
        super(isOn);
    }

    private boolean shouldAccept(StoredProcedureInvocation invocation,
                                 boolean isReadOnly, WriteStream s) {
        if (!isOn) {
            return true;
        }

        if (invocation.getType() == ProcedureInvocationType.ORIGINAL) {
            if (isReadOnly) {
                return true;
            } else {
                final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                               new VoltTable[0],
                                               "Write procedure " + invocation.procName +
                                               " is not allowed in secondary cluster",
                                               invocation.clientHandle);
                s.enqueue(errorResponse);
                return false;
            }
        } else {
            if (isReadOnly) {
                final ClientResponseImpl errorResponse =
                        new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                               new VoltTable[0],
                                               "Read replicated procedure " + invocation.procName +
                                               " is dropped from secondary cluster",
                                               invocation.clientHandle);
                s.enqueue(errorResponse);
                return false;
            } else {
                return true;
            }
        }
    }

    @Override
    public boolean shouldAccept(StoredProcedureInvocation invocation,
                                Procedure proc, WriteStream s) {
        if (invocation == null || proc == null) {
            return false;
        }
        return shouldAccept(invocation, proc.getReadonly(), s);
    }

    @Override
    public boolean shouldAccept(StoredProcedureInvocation invocation,
                                Config sysProc, WriteStream s) {
        if (invocation == null || sysProc == null) {
            return false;
        }
        return shouldAccept(invocation, sysProc.readOnly, s);
    }
}
