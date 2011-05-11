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

import java.io.IOException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Callable;

import java.nio.ByteBuffer;

import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.*;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 */
public class StoredProcedureInvocation implements FastSerializable {
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    String procName = null;
    ByteBuffer unserializedParams = null;

    FutureTask<ParameterSet> params;

    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    long clientHandle = -1;

    public StoredProcedureInvocation getShallowCopy()
    {
        StoredProcedureInvocation copy = new StoredProcedureInvocation();
        copy.clientHandle = clientHandle;
        copy.params = params;
        copy.procName = procName;
        if (unserializedParams != null)
        {
            copy.unserializedParams = unserializedParams.duplicate();
        }
        else
        {
            copy.unserializedParams = null;
        }

        return copy;
    }

    public void setProcName(String name) {
        procName = name;
    }

    public void setParams(final Object... parameters) {
        // convert the params to the expected types
        params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() {
                ParameterSet params = new ParameterSet();
                params.setParameters(parameters);
                return params;
            }
        });
        unserializedParams = null;
    }

    public String getProcName() {
        return procName;
    }

    public ParameterSet getParams() {
        params.run();
        try {
            return params.get();
        } catch (InterruptedException e) {
            hostLog.fatal("Interrupted while deserializing a parameter set");
            VoltDB.crashVoltDB();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public void setClientHandle(int aHandle) {
        clientHandle = aHandle;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    /** Read into an unserialized parameter buffer to extract a single parameter */
    Object getParameterAtIndex(int partitionIndex) {
        try {
            return ParameterSet.getParameterAtIndex(partitionIndex, unserializedParams);
        }
        catch (IOException ex) {
            throw new RuntimeException("Invalid partitionIndex", ex);
        }
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        in.readByte();//skip version
        procName = in.readString();
        clientHandle = in.readLong();
        // do not deserialize parameters in ClientInterface context
        unserializedParams = in.remainder();
        params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                FastDeserializer fds = new FastDeserializer(unserializedParams);
                return fds.readObject(ParameterSet.class);
            }
        });
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert(!((params == null) && (unserializedParams == null)));
        assert((params != null) || (unserializedParams != null));
        out.write(0);//version
        out.writeString(procName);
        out.writeLong(clientHandle);
        if (unserializedParams != null)
            out.write(unserializedParams.array(),
                      unserializedParams.position() + unserializedParams.arrayOffset(),
                      unserializedParams.remaining());
        else if (params != null) {
            out.writeObject(getParams());
        }
    }

    @Override
    public String toString() {
        String retval = "Invocation: " + procName + "(";
        ParameterSet params = getParams();
        if (params != null)
            for (Object o : params.toArray()) {
                retval += o.toString() + ", ";
            }
        else
            retval += "null";
        retval += ")";
        return retval;
    }

    public void getDumpContents(StringBuilder sb) {
        sb.append("Invocation: ").append(procName).append("(");
        ParameterSet params = getParams();
        if (params != null)
            for (Object o : params.toArray()) {
                sb.append(o.toString()).append(", ");
            }
        else
            sb.append("null");
        sb.append(")");
    }

    public ByteBuffer getSerializedParams() {
        return unserializedParams;
    }

    public void setSerializedParams(ByteBuffer serializedParams) {
        unserializedParams = serializedParams;
    }
}
