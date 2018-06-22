/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.voltdb.catalog.Table;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ProcedureInvocationExtensions;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.common.Constants;
import org.voltdb.utils.SerializationHelper;

/**
 * Represents a serializeable bundle of procedure name and parameters. This
 * is the object that is sent by the client library to call a stored procedure.
 *
 * Note, the client (java) serializes a ProcedureInvocation, which is deserialized
 * by the server as a StoredProcedureInvocation. This avoids dragging some extra
 * code into the client. The point is that the serialization of these classes
 * need to be in sync.
 *
 * Note also there are a few places that need to be updated if the serialization
 * is changed. See getSerializedSize().
 *
 */
public class StoredProcedureInvocation implements JSONString {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final byte CURRENT_MOST_RECENT_VERSION = ProcedureInvocationType.VERSION2.getValue();

    ProcedureInvocationType type = ProcedureInvocationType.ORIGINAL;
    private String procName = null;
    private byte m_procNameBytes[] = null;

    public static final long UNITIALIZED_ID = -1L;

    /*
     * This ByteBuffer is accessed from multiple threads concurrently.
     * Always duplicate it before reading
     */
    private ByteBuffer serializedParams = null;

    FutureTask<ParameterSet> params;

    /** A descriptor provided by the client, opaque to the server,
        returned to the client in the ClientResponse */
    long clientHandle = -1;

    private int m_batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
    private boolean m_allPartition = false;

    public StoredProcedureInvocation getShallowCopy()
    {
        StoredProcedureInvocation copy = new StoredProcedureInvocation();
        copy.type = type;
        copy.clientHandle = clientHandle;
        copy.params = params;
        copy.procName = procName;
        copy.m_procNameBytes = m_procNameBytes;
        if (serializedParams != null)
        {
            copy.serializedParams = serializedParams.duplicate();
        }
        else
        {
            copy.serializedParams = null;
        }

        copy.m_batchTimeout = m_batchTimeout;
        copy.m_allPartition = m_allPartition;

        return copy;
    }

    public void setProcName(String name) {
        if (name == null) {
            throw new IllegalArgumentException("SPI setProcName(String name) doesn't accept NULL.");
        }
        procName = name;
        m_procNameBytes = null;
    }

    public void setProcName(byte[] name) {
        if (name == null) {
            throw new IllegalArgumentException("SPI setProcName(byte[] name) doesn't accept NULL.");
        }
        procName = null;
        m_procNameBytes = name;
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
        if (procName == null) {
            assert(m_procNameBytes != null);
            procName = new String(m_procNameBytes, Constants.UTF8ENCODING);
        }
        return procName;
    }

    public byte[] getProcNameBytes() {
        if (m_procNameBytes == null) {
            assert(procName != null);
            m_procNameBytes = procName.getBytes(Constants.UTF8ENCODING);
        }
        return m_procNameBytes;
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

    public int getBatchTimeout() {
        return m_batchTimeout;
    }

    public void setBatchTimeout(int timeout) {
        m_batchTimeout = timeout;
    }

    public void setAllPartition(boolean allPartition) {
        m_allPartition = allPartition;
    }

    public boolean getAllPartition() {
        return m_allPartition;
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

    /**
     *
     * NOTE: If you change this method, you may have to fix
     * getLoadVoltTablesMagicSeriazlizedSize below too.
     * Also line 38 of PartitionDRGatewayImpl.java
     * Also line 30 of AbstactDRTupleStream.h
     * Also line 38 of InvocationBuffer.java
     */
    public int getSerializedSize()
    {
        // compute the size
        int size = getSerializedSizeWithoutParams() +
            getSerializedParamSize(); // parameters
        assert(size > 0); // sanity

        // MAKE SURE YOU SEE COMMENT ON TOP OF METHOD!!!
        return size;
    }

    public int getSerializedSizeWithoutParams()
    {
        // get extension sizes - if not present, size is 0 for each
        // 6 is one byte for ext type, one for size, and 4 for integer value
        int batchExtensionSize = m_batchTimeout != BatchTimeoutOverrideType.NO_TIMEOUT ? 6 : 0;
        // 2 is one byte for ext type, one for size
        int allPartitionExtensionSize = m_allPartition ? 2 : 0;

        // compute the size
        int size =
                1 + // type
                        4 + getProcNameBytes().length + // procname
                        8 + // client handle
                        1 + // extension count
                        batchExtensionSize + allPartitionExtensionSize;
        return size;
    }

    /**
     * Get the serialized size of this SPI in the original serialization version.
     * This is currently used by DR.
     */
    public int getSerializedSizeForOriginalVersion()
    {
        return (1 + // type
                4 + getProcNameBytes().length + // procname
                8 + // client handle
                getSerializedParamSize());
    }

    private int getSerializedParamSize()
    {
        // get params size
        int serializedParamSize = 0;
        if (serializedParams != null) {
            serializedParamSize = serializedParams.remaining();
        }
        else if (params != null) {
            ParameterSet pset = getParams();
            assert(pset != null);
            serializedParamSize = pset.getSerializedSize();
            if ((pset.size() > 0) && (serializedParamSize <= 2)) {
                throw new IllegalStateException(String.format("Parameter set for invocation " +
                                                              "%s doesn't have the proper size (currently = %s)",
                                                              getProcName(), serializedParamSize));
            }
        }
        else {
            // illegal state
            throw new IllegalStateException("StoredProcedureInvocation instance params in invalid state.");
        }
        return serializedParamSize;
    }

    /**
     * Hack for SyncSnapshotBuffer. Note that this is using the ORIGINAL (version 0) serialization format.
     * Moved to this file from that one so you might see it sooner than I did.
     * If you change the serialization, you have to change this too.
     */
    public static int getLoadVoltTablesMagicSeriazlizedSize(Table catTable, boolean isPartitioned) {

        // code below is used to compute the right value slowly
        /*
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName("@LoadVoltTableSP");
        if (isPartitioned) {
            spi.setParams(0, catTable.getTypeName(), null);
        }
        else {
            spi.setParams(0, catTable.getTypeName(), null);
        }
        int size = spi.getSerializedSizeForOriginalVersion() + 4;
        int realSize = size - catTable.getTypeName().getBytes(Constants.UTF8ENCODING).length;
        System.err.printf("@LoadVoltTable** padding size: %d or %d\n", size, realSize);
        return size;
        */

        // Magic size of @LoadVoltTable* StoredProcedureInvocation
        int tableNameLengthInBytes =
                catTable.getTypeName().getBytes(Constants.UTF8ENCODING).length;
        int metadataSize = 41 + // serialized size for original version
                           tableNameLengthInBytes;
        if (isPartitioned) {
            metadataSize += 5;
        }
        return metadataSize;
    }

    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        assert((params != null) || (serializedParams != null));

        // for self-check assertion
        int startPosition = buf.position();

        // write current format version only (we read all old formats)
        buf.put(CURRENT_MOST_RECENT_VERSION);

        SerializationHelper.writeVarbinary(getProcNameBytes(), buf);

        buf.putLong(clientHandle);

        // there are two possible extensions, count which apply
        byte extensionCount = 0;
        if (m_batchTimeout != BatchTimeoutOverrideType.NO_TIMEOUT) ++extensionCount;
        if (m_allPartition) ++extensionCount;
        // write the count as one byte
        buf.put(extensionCount);
        // write any extensions that apply
        if (m_batchTimeout != BatchTimeoutOverrideType.NO_TIMEOUT) {
            ProcedureInvocationExtensions.writeBatchTimeoutWithTypeByte(buf, m_batchTimeout);
        }
        if (m_allPartition) {
            ProcedureInvocationExtensions.writeAllPartitionWithTypeByte(buf);
        }

        serializeParams(buf);

        int len = buf.position() - startPosition;
        assert(len == getSerializedSize());
    }

    /**
     * Serializes this SPI in the original serialization version.
     * This is currently used by DR.
     */
    public void flattenToBufferForOriginalVersion(ByteBuffer buf) throws IOException
    {
        assert((params != null) || (serializedParams != null));

        // for self-check assertion
        int startPosition = buf.position();

        buf.put(ProcedureInvocationType.ORIGINAL.getValue());

        SerializationHelper.writeVarbinary(getProcNameBytes(), buf);

        buf.putLong(clientHandle);

        serializeParams(buf);

        int len = buf.position() - startPosition;
        assert(len == getSerializedSizeForOriginalVersion());
    }

    private void serializeParams(ByteBuffer buf) throws IOException
    {
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
        byte version = buf.get();// version number also embeds the type
        // this will throw for an unexpected type, like the DRv1 type, for example
        type = ProcedureInvocationType.typeFromByte(version);
        m_procNameBytes = null;
        // set these to defaults so old versions don't worry about them
        m_batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
        m_allPartition = false;

        switch (type) {
            case ORIGINAL:
                initOriginalFromBuffer(buf);
                break;
            case VERSION1:
                initVersion1FromBuffer(buf);
                break;
            case VERSION2:
                initVersion2FromBuffer(buf);
                break;
        }
    }

    private void initOriginalFromBuffer(ByteBuffer buf) throws IOException {
        byte[] procNameBytes = SerializationHelper.getVarbinary(buf);
        if (procNameBytes == null) {
            throw new IOException("Procedure name cannot be null in invocation deserialization.");
        }
        if (procNameBytes.length == 0) {
            throw new IOException("Procedure name cannot be length zero in invocation deserialization.");
        }
        setProcName(procNameBytes);
        clientHandle = buf.getLong();
        // do not deserialize parameters in ClientInterface context
        initParameters(buf);
    }

    private void initVersion1FromBuffer(ByteBuffer buf) throws IOException {
        BatchTimeoutOverrideType batchTimeoutType = BatchTimeoutOverrideType.typeFromByte(buf.get());
        if (batchTimeoutType == BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT) {
            m_batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
        } else {
            m_batchTimeout = buf.getInt();
            // Client side have already checked the batchTimeout value, but,
            // on server side, we should check non-negative batchTimeout value again
            // in case of someone is using a non-standard client.
            if (m_batchTimeout < 0) {
                throw new IllegalArgumentException("Timeout value can't be negative." );
            }
        }

        // the rest of the format is the same as the original
        initOriginalFromBuffer(buf);
    }

    private void initVersion2FromBuffer(ByteBuffer buf) throws IOException {
        byte[] procNameBytes = SerializationHelper.getVarbinary(buf);
        if (procNameBytes == null) {
            throw new IOException("Procedure name cannot be null in invocation deserialization.");
        }
        if (procNameBytes.length == 0) {
            throw new IOException("Procedure name cannot be length zero in invocation deserialization.");
        }
        setProcName(procNameBytes);

        clientHandle = buf.getLong();

        // default values for extensions
        m_batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
        // read any invocation extensions and skip any we don't recognize
        int extensionCount = buf.get();

        // this limits things a bit, but feels worth it in terms of being a possible way
        // to stumble on a bug
        if (extensionCount < 0) {
            throw new IOException("SPI extension count was < 0: possible corrupt network data.");
        }
        if (extensionCount > 30) {
            throw new IOException("SPI extension count was > 30: possible corrupt network data.");
        }

        for (int i = 0; i < extensionCount; ++i) {
            final byte type = ProcedureInvocationExtensions.readNextType(buf);
            switch (type) {
            case ProcedureInvocationExtensions.BATCH_TIMEOUT:
                m_batchTimeout = ProcedureInvocationExtensions.readBatchTimeout(buf);
                break;
            case ProcedureInvocationExtensions.ALL_PARTITION:
                // note this always returns true as it's just a flag
                m_allPartition = ProcedureInvocationExtensions.readAllPartition(buf);
                break;
            default:
                ProcedureInvocationExtensions.skipUnknownExtension(buf);
                break;
            }
        }

        // do not deserialize parameters in ClientInterface context
        initParameters(buf);
    }

    private void initParameters(ByteBuffer buf) {
        serializedParams = buf.slice();
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
        retval += " batchTimeout=" + BatchTimeoutOverrideType.toString(m_batchTimeout);
        retval += " clientHandle=" + String.valueOf(clientHandle);

        return retval;
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
            js.keySymbolValuePair("proc_name", procName);
            js.keySymbolValuePair("client_handle", clientHandle);
            // @ApplyBinaryLog is exempted because it's often
            // got a large binary payload and this is annoying for testing
            // also users shouldn't ever directly call it
            if (!procName.startsWith("@ApplyBinaryLog")) {
                js.key("parameters").value(params.get());
            }
            js.endObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to serialize an invocation to JSON.", e);
        }
        return js.toString();
    }
}
