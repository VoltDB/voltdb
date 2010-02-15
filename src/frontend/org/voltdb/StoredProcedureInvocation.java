/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.nio.ByteBuffer;
import org.voltdb.messaging.*;
import org.voltdb.client.ProcedureCallback;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 */
public class StoredProcedureInvocation implements FastSerializable {

    String procName = null;
    ParameterSet params = null;
    ByteBuffer unserializedParams = null;

    /**
     * Time the client queued this transaction
     */
    long clientQueueTime = -1;

    /**
     * Time the ClientInterface read the transaction
     */
    long CIAcceptTime = -1;

    /**
     * Time the foreign host that would execute the transaction
     * received the transaction and delivered it into its local priority queue
     */
    long FHReceiveTime = -1;

    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    long clientHandle = -1;

    public long CIAcceptTime() { return CIAcceptTime; }
    public long clientQueueTime() { return clientQueueTime; }
    public long FHReceiveTime() { return FHReceiveTime; }

    public void setProcName(String name) {
        procName = name;
    }

    public void setParams(Object... parameters) {
        // convert the params to the expected types
        params = new ParameterSet();
        params.setParameters(parameters);
        unserializedParams = null;
    }

    public String getProcName() {
        return procName;
    }

    public ParameterSet getParams() {
        return params;
    }

    public void setClientHandle(int aHandle) {
        clientHandle = aHandle;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    /**
     * If created from ClientInterface within a single host,
     * will still need to deserialize the parameter set. This
     * optimization is not performed for multi-site transactions
     * (which require concurrent access to the task).
     */
     public void buildParameterSet() {
        if (unserializedParams != null) {
            assert (params == null);
            try {
                FastDeserializer fds = new FastDeserializer(unserializedParams);
                params = fds.readObject(ParameterSet.class);
                unserializedParams = null;
            }
            catch (IOException ex) {
                throw new RuntimeException("Invalid ParameterSet in Stored Procedure Invocation.");
            }
        }
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
        if (ProcedureCallback.measureLatency){
            clientQueueTime = in.readLong();
            CIAcceptTime = in.readLong();
            if (clientQueueTime != -1){
                if (CIAcceptTime == -1) {
                    CIAcceptTime = System.currentTimeMillis();
                } else {
                    FHReceiveTime = System.currentTimeMillis();
                }
            }
        }
        // do not deserialize parameters in ClientInterface context
        unserializedParams = in.remainder();
        params = null;
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert(!((params == null) && (unserializedParams == null)));
        assert((params != null) || (unserializedParams != null));
        out.write(0);//version
        out.writeString(procName);
        out.writeLong(clientHandle);
        if (ProcedureCallback.measureLatency) {
            out.writeLong(clientQueueTime);
            out.writeLong(CIAcceptTime);
        }
        if (params != null)
            out.writeObject(params);
        else if (unserializedParams != null)
            out.write(unserializedParams.array(),
                      unserializedParams.position() + unserializedParams.arrayOffset(),
                      unserializedParams.remaining());
    }

    @Override
    public String toString() {
        String retval = "Invocation: " + procName + "(";
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
        if (params != null)
            for (Object o : params.toArray()) {
                sb.append(o.toString()).append(", ");
            }
        else
            sb.append("null");
        sb.append(")");
    }
}
