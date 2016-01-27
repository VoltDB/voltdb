/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.json_voltpatches.JSONString;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.common.Constants;
import org.voltdb.messaging.FastDeserializer;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 */
public class StoredProcedureInvocation implements JSONString {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final int CURRENT_MOST_RECENT_VERSION = 1;

    ProcedureInvocationType type = ProcedureInvocationType.ORIGINAL;
    String procName = null;

    public static final long UNITIALIZED_ID = -1L;
    /*
     * The original txn ID and the timestamp the procedure invocation was
     * assigned with. They are saved here so that if the procedure needs them
     * for determinism, we can provide them again. -1 means not set.
     */
    long originalTxnId = UNITIALIZED_ID;
    long originalUniqueId = UNITIALIZED_ID;

    /*
     * This ByteBuffer is accessed from multiple threads concurrently.
     * Always duplicate it before reading
     */
    private ByteBuffer serializedParams = null;

    FutureTask<ParameterSet> params;

    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    long clientHandle = -1;

    int batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;

    public StoredProcedureInvocation getShallowCopy()
    {
        StoredProcedureInvocation copy = new StoredProcedureInvocation();
        copy.type = type;
        copy.clientHandle = clientHandle;
        copy.params = params;
        copy.procName = procName;
        copy.originalTxnId = originalTxnId;
        copy.originalUniqueId = originalUniqueId;
        if (serializedParams != null)
        {
            copy.serializedParams = serializedParams.duplicate();
        }
        else
        {
            copy.serializedParams = null;
        }

        copy.batchTimeout = batchTimeout;

        return copy;
    }

    private void setType() {
        if (originalTxnId == UNITIALIZED_ID && originalUniqueId == UNITIALIZED_ID) {
            if (BatchTimeoutOverrideType.isUserSetTimeout(batchTimeout)) {
                type = ProcedureInvocationType.VERSION1;
            } else {
                type = ProcedureInvocationType.ORIGINAL;
            }
        } else {
            type = ProcedureInvocationType.REPLICATED;
        }
    }

    public void setProcName(String name) {
        procName = name;
    }

    public void setOriginalTxnId(long txnId) {
        originalTxnId = txnId;
        setType();
    }

    public void setOriginalUniqueId(long uniqueId) {
        originalUniqueId = uniqueId;
        setType();
    }

    public void setParams(final Object... parameters) {
        // convert the params to the expected types
        params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() {
                ParameterSet params = ParameterSet.fromArrayWithCopy(parameters);
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

    public long getOriginalUniqueId() {
        return originalUniqueId;
    }

    public ParameterSet getParams() {
        params.run();
        try {
            return params.get();
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Interrupted while deserializing a parameter set", false, e);
        } catch (ExecutionException e) {
            // Don't rethrow Errors as RuntimeExceptions because we will eat their
            // delicious goodness later
            if (e.getCause() != null && e.getCause() instanceof Error) {
                throw (Error)e.getCause();
            }
            throw new RuntimeException(e);
        }
        return null;
    }

    public void setClientHandle(long aHandle) {
        clientHandle = aHandle;
    }

    public long getClientHandle() {
        return clientHandle;
    }

    /** Read into an serialized parameter buffer to extract a single parameter */
    Object getParameterAtIndex(int partitionIndex) {
        try {
            if (serializedParams != null) {
                return ParameterSet.getParameterAtIndex(partitionIndex, serializedParams.duplicate());
            } else {
                return params.get().getParam(partitionIndex);
            }
        }
        catch (Exception ex) {
            throw new RuntimeException("Invalid partitionIndex: " + partitionIndex, ex);
        }
    }

    public int getSerializedSize()
    {
        int timeoutSize = 0;
        if (type.getValue() >= BatchTimeoutOverrideType.BATCH_TIMEOUT_VERSION) {
            timeoutSize = 1 + (batchTimeout == BatchTimeoutOverrideType.NO_TIMEOUT ? 0 : 4);
        }

        int size = 1 // Version/type
            + timeoutSize // batch time out byte
            + 4 // proc name string length
            + procName.length()
            + 8; // clientHandle

        if (ProcedureInvocationType.isDeprecatedInternalDRType(type))
        {
            size += 8 + // original TXN ID for WAN replication procedures
                    8; // original timestamp for WAN replication procedures
        }

        if (serializedParams != null)
        {
            size += serializedParams.remaining();
        }
        else if (params != null)
        {
            ParameterSet pset = getParams();
            assert(pset != null);
            int serializedSize = pset.getSerializedSize();
            if ((pset.size() > 0) && (serializedSize <= 2)) {
                throw new IllegalStateException(String.format("Parameter set for invocation " +
                        "%s doesn't have the proper size (currently = %s)",
                        getProcName(), serializedSize));
            }
            size += pset.getSerializedSize();
        }

        return size;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        assert(!((params == null) && (serializedParams == null)));
        assert((params != null) || (serializedParams != null));
        buf.put(type.getValue()); //version and type
        if (ProcedureInvocationType.isDeprecatedInternalDRType(type)) {
            buf.putLong(originalTxnId);
            buf.putLong(originalUniqueId);
        }
        if (type.getValue() >= BatchTimeoutOverrideType.BATCH_TIMEOUT_VERSION) {
            if (batchTimeout == BatchTimeoutOverrideType.NO_TIMEOUT) {
                buf.put(BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT.getValue());
            } else {
                buf.put(BatchTimeoutOverrideType.HAS_OVERRIDE_FOR_BATCH_TIMEOUT.getValue());
                buf.putInt(batchTimeout);
            }
        }
        buf.putInt(procName.length());
        buf.put(procName.getBytes(Constants.UTF8ENCODING));
        buf.putLong(clientHandle);
        if (serializedParams != null)
        {
            if (serializedParams.hasArray())
            {
                // if position can be non-zero, then the dup/rewind logic below
                // would be wrong?
                assert(serializedParams.position() == 0);
                buf.put(serializedParams.array(),
                        serializedParams.position() + serializedParams.arrayOffset(),
                        serializedParams.remaining());
            }
            else
            {
                // duplicate for thread-safety
                assert(serializedParams.position() == 0);
                ByteBuffer dup = serializedParams.duplicate();
                dup.rewind();
                buf.put(dup);
            }
        }
        else if (params != null) {
            try {
                getParams().flattenToBuffer(buf);
            }
            catch (BufferOverflowException e) {
                hostLog.info("SP \"" + procName + "\" has thrown BufferOverflowException");
                hostLog.info(toString());
                throw e;
            }
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
        if (ProcedureInvocationType.isDeprecatedInternalDRType(type)) {
            originalTxnId = in.readLong();
            originalUniqueId = in.readLong();
        }

        if (version >= BatchTimeoutOverrideType.BATCH_TIMEOUT_VERSION) {
            BatchTimeoutOverrideType batchTimeoutType = BatchTimeoutOverrideType.typeFromByte(in.readByte());
            if (batchTimeoutType == BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT) {
                batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
            } else {
                batchTimeout = in.readInt();
                // Client side have already checked the batchTimeout value, but,
                // on server side, we should check non-negative batchTimeout value again
                // in case of someone is using a non-standard client.
                if (batchTimeout < 0) {
                    throw new IllegalArgumentException("Timeout value can't be negative." );
                }
            }
        }

        procName = in.readString().intern();
        clientHandle = in.readLong();
        // do not deserialize parameters in ClientInterface context
        serializedParams = in.remainder();
        final ByteBuffer duplicate = serializedParams.duplicate();
        params = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() throws Exception {
                return ParameterSet.fromByteBuffer(duplicate);
            }
        });
    }

    @Override
    public String toString() {
        String retval = type.name() + " Invocation: " + procName + "(";
        ParameterSet params = getParams();
        if (params != null)
            for (Object o : params.toArray()) {
                retval += String.valueOf(o) + ", ";
            }
        else
            retval += "null";
        retval += ")";
        retval += " type=" + String.valueOf(type);
        retval += " batchTimeout=" + BatchTimeoutOverrideType.toString(batchTimeout);
        retval += " clientHandle=" + String.valueOf(clientHandle);
        retval += " originalTxnId=" + String.valueOf(originalTxnId);
        retval += " originalUniqueId=" + String.valueOf(originalUniqueId);

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
            if (!procName.startsWith("@ApplyBinaryLog")) {
                js.key("parameters");
                js.value(params.get());
            }
            js.endObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialize an invocation to JSON.", e);
        }
        return js.toString();
    }

    public int getBatchTimeout() {
        return batchTimeout;
    }

    public void setBatchTimeout(int timeout) {
        batchTimeout = timeout;
    }
}
