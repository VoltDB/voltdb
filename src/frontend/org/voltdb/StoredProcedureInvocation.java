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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltdb.client.ProcedureInvocationType;
import org.voltcore.logging.VoltLogger;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 */
public class StoredProcedureInvocation implements FastSerializable, JSONString {
    @SuppressWarnings("unused")
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    ProcedureInvocationType type = ProcedureInvocationType.ORIGINAL;
    String procName = null;

    /*
     * The original txn ID the procedure invocation was assigned with. It's
     * saved here so that if the procedure needs it for determinism, we can
     * provide it again. -1 means not set.
     */
    long originalTxnId = -1;

    /*
     * This ByteBuffer is accessed from multiple threads concurrently.
     * Always duplicate it before reading
     */
    ByteBuffer serializedParams = null;

    FutureTask<ParameterSet> params;

    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    long clientHandle = -1;

    public StoredProcedureInvocation getShallowCopy()
    {
        StoredProcedureInvocation copy = new StoredProcedureInvocation();
        copy.type = type;
        copy.clientHandle = clientHandle;
        copy.params = params;
        copy.procName = procName;
        copy.originalTxnId = originalTxnId;
        if (serializedParams != null)
        {
            copy.serializedParams = serializedParams.duplicate();
        }
        else
        {
            copy.serializedParams = null;
        }

        return copy;
    }

    private void setType() {
        type = originalTxnId == -1 ? ProcedureInvocationType.ORIGINAL
                                   : ProcedureInvocationType.REPLICATED;
    }

    public void setProcName(String name) {
        procName = name;
    }

    public void setOriginalTxnId(long txnId) {
        originalTxnId = txnId;
        setType();
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
        serializedParams = null;
    }

    public ProcedureInvocationType getType() {
        return type;
    }

    public String getProcName() {
        return procName;
    }

    public long getOriginalTxnId() {
        return originalTxnId;
    }

    public ParameterSet getParams() {
        params.run();
        try {
            return params.get();
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Interrupted while deserializing a parameter set", false, e);
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

    /** Read into an serialized parameter buffer to extract a single parameter */
    Object getParameterAtIndex(int partitionIndex) {
        try {
            return ParameterSet.getParameterAtIndex(partitionIndex, serializedParams);
        }
        catch (IOException ex) {
            throw new RuntimeException("Invalid partitionIndex", ex);
        }
    }

    public int getSerializedSize()
    {
        int size = 1 // Version/type
            + 4 // proc name string length
            + procName.length()
            + 8; // clientHandle

        if (type == ProcedureInvocationType.REPLICATED)
        {
            size += 8; // original TXN ID for WAN replication procedures
        }

        if (serializedParams != null)
        {
            size += serializedParams.remaining();
        }
        else if (params != null)
        {
            size += getParams().getSerializedSize();
        }

        return size;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        assert(!((params == null) && (serializedParams == null)));
        assert((params != null) || (serializedParams != null));
        buf.put(type.getValue()); //version and type, version is currently 0
        if (type == ProcedureInvocationType.REPLICATED) {
            buf.putLong(originalTxnId);
        }
        buf.putInt(procName.length());
        buf.put(procName.getBytes());
        buf.putLong(clientHandle);
        if (serializedParams != null)
        {
            if (!serializedParams.isReadOnly())
            {
                buf.put(serializedParams.array(),
                        serializedParams.position() + serializedParams.arrayOffset(),
                        serializedParams.remaining());
            }
            else
            {
                buf.put(serializedParams);
            }
        }
        else if (params != null) {
            getParams().flattenToBuffer(buf);
        }
    }

    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        FastDeserializer in = new FastDeserializer(buf);
        byte version = in.readByte();// version number also embeds the type
        type = ProcedureInvocationType.typeFromByte(version);

        /*
         * If it's a replicated invocation, there should be two txn IDs
         * following the version byte. The first txn ID is the new txn ID, the
         * second one is the original txn ID.
         */
        if (type == ProcedureInvocationType.REPLICATED) {
            originalTxnId = in.readLong();
        }

        procName = in.readString();
        clientHandle = in.readLong();
        // do not deserialize parameters in ClientInterface context
        serializedParams = in.remainder();
        final ByteBuffer duplicate = serializedParams.duplicate();
        params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                FastDeserializer fds = new FastDeserializer(duplicate);
                return fds.readObject(ParameterSet.class);
            }
        });
    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        byte version = in.readByte();// version number also embeds the type
        type = ProcedureInvocationType.typeFromByte(version);

        /*
         * If it's a replicated invocation, there should be two txn IDs
         * following the version byte. The first txn ID is the new txn ID, the
         * second one is the original txn ID.
         */
        if (type == ProcedureInvocationType.REPLICATED) {
            originalTxnId = in.readLong();
        }

        procName = in.readString();
        clientHandle = in.readLong();
        // do not deserialize parameters in ClientInterface context
        serializedParams = in.remainder();
        final ByteBuffer duplicate = serializedParams.duplicate();
        params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                FastDeserializer fds = new FastDeserializer(duplicate);
                return fds.readObject(ParameterSet.class);
            }
        });
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        assert(!((params == null) && (serializedParams == null)));
        assert((params != null) || (serializedParams != null));
        out.write(type.getValue());//version and type, version is currently 0
        if (type == ProcedureInvocationType.REPLICATED) {
            out.writeLong(originalTxnId);
        }
        out.writeString(procName);
        out.writeLong(clientHandle);
        if (serializedParams != null)
            out.write(serializedParams.duplicate());
        else if (params != null) {
            out.writeObject(getParams());
        }
    }

    @Override
    public String toString() {
        String retval = type.name() + " Invocation: " + procName + "(";
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
        sb.append(type.name()).append("Invocation: ").append(procName).append("(");
        ParameterSet params = getParams();
        if (params != null)
            for (Object o : params.toArray()) {
                sb.append(o.toString()).append(", ");
            }
        else
            sb.append("null");
        sb.append(")");
    }

    /*
     * Store a copy of the parameters to the procedure in serialized form.
     * In a cluster there is no reason to throw away the serialized bytes
     * because it will be forwarded in most cases and there is no need to repeat the work.
     * Command logging also takes advantage of this to avoid reserializing the parameters.
     * In some cases the params will never have been serialized (null) because
     * the SPI is generated internally. A duplicate view of the buffer is returned
     * to make access thread safe. Can't return a read only view because ByteBuffer.array()
     * is invoked by the command log.
     */
    public ByteBuffer getSerializedParams() {
        if (serializedParams != null) {
            return serializedParams.duplicate();
        }
        return null;
    }

    public void setSerializedParams(ByteBuffer serializedParams) {
        this.serializedParams = serializedParams;
    }

    @Override
    public String toJSONString() {
        params.run();
        JSONStringer js = new JSONStringer();
        try {
            js.object();
            js.key("proc_name");
            js.value(procName);
            js.key("parameters");
            js.value(params.get());
            js.endObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialize an invocation to JSON.", e);
        }
        return js.toString();
    }
}
