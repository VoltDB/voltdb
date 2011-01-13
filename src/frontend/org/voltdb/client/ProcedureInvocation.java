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

package org.voltdb.client;

import java.io.IOException;

import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;

/**
 * Client stored procedure invocation object. Server uses an internal
 * format compatible with this wire protocol format.
 */
class ProcedureInvocation implements FastSerializable {

    private final long m_clientHandle;
    private final String m_procName;
    private final ParameterSet m_parameters;

    ProcedureInvocation(long handle, String procName, Object... parameters) {
        super();
        m_clientHandle = handle;
        m_procName = procName;
        m_parameters = new ParameterSet();
        m_parameters.setParameters(parameters);
    }

    /** return the clientHandle value */
    long getHandle() {
        return m_clientHandle;
    }

    public String getProcName() {
        return m_procName;
    }

    /** This default deserializer is never used. */
    public void readExternal(FastDeserializer in) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /** Produce a serialization matching ExecutionSiteTask.createFromWireProtocol(). */
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeByte(0);//Version
        out.writeString(m_procName);
        out.writeLong(m_clientHandle);
        out.writeObject(m_parameters);
    }
}
