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

package org.voltdb.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.voltdb.ParameterSet;
import org.voltdb.utils.SerializationHelper;

/**
 * Client stored procedure invocation object. Server uses an internal
 * format compatible with this wire protocol format.
 */
public class ProcedureInvocation {

    public static final byte CURRENT_MOST_RECENT_VERSION = ProcedureInvocationType.VERSION2.getValue();

    private final long m_clientHandle;
    private final String m_procName;
    private byte[] m_procNameBytes;
    private final int m_batchTimeout; // milliseconds
    private final ParameterSet m_parameters;
    private final int m_partitionDestination;
    private final int m_requestPriority;
    private int m_requestTimeout; // microseconds

    // pre-cache this for serialization
    // this duplicates some other code, but it's nice to keep the client code
    //  self-contained (see Constants.UTF8ENCODING)
    private static final Charset UTF8Encoding = Charset.forName("UTF-8");

    // Not partition-specific
    public static final int NO_PARTITION = -1;

    // No priority field will be marshalled
    public static final int NO_PRIORITY = -1;

    // No timeout - same value used for batch and request timeouts
    public static final int NO_TIMEOUT = BatchTimeoutOverrideType.NO_TIMEOUT;

    // No optional arguments
    public ProcedureInvocation(long handle, String procName, Object... parameters) {
        this(handle, NO_TIMEOUT, NO_PARTITION, NO_PRIORITY, procName, parameters);
    }

    // With batch timeout
    public ProcedureInvocation(long handle, int batchTimeout, String procName, Object... parameters) {
        this(handle, batchTimeout, NO_PARTITION, NO_PRIORITY, procName, parameters);
    }

    // With batch timeout and partition
    public ProcedureInvocation(long handle, int batchTimeout, int partitionDestination, String procName,
            Object... parameters) {
        this(handle, batchTimeout, partitionDestination, NO_PRIORITY, procName, parameters);
    }

    // With batch timeout, partition, request priority
    public ProcedureInvocation(long handle, int batchTimeout, int partitionDestination, int requestPrio,
                                String procName, Object... parameters) {
        if (batchTimeout < 0 && batchTimeout != NO_TIMEOUT) {
            throw new IllegalArgumentException("Timeout value can't be negative.");
        }

        // Careful: highest prio has lowest numerical value
        if (requestPrio != NO_PRIORITY && (requestPrio < Priority.HIGHEST_PRIORITY || requestPrio > Priority.LOWEST_PRIORITY)) {
            throw new IllegalArgumentException(String.format("Request priority must be in range %d to %d",
                                                             Priority.HIGHEST_PRIORITY, Priority.LOWEST_PRIORITY));
        }

        m_clientHandle = handle;
        m_procName = procName;
        m_parameters = (parameters != null
                            ? ParameterSet.fromArrayWithCopy(parameters)
                            : ParameterSet.emptyParameterSet());
        m_batchTimeout = batchTimeout;
        m_partitionDestination = partitionDestination;
        m_requestPriority = requestPrio;
        m_requestTimeout = NO_TIMEOUT; // updated later
    }

    /** return the clientHandle value */
    long getHandle() {
        return m_clientHandle;
    }

    public String getProcName() {
        return m_procName;
    }

    public int getPassedParamCount() {
        return m_parameters.size();
    }

    public Object getPartitionParamValue(int index) {
        return m_parameters.getParam(index);
    }

    public long getClientHandle() {
        return m_clientHandle;
    }

    public int getBatchTimeout() {
        return m_batchTimeout;
    }

    public boolean hasPartitionDestination() {
        return m_partitionDestination != NO_PARTITION;
    }

    public int getPartitionDestination() {
        return m_partitionDestination;
    }

    public boolean hasRequestPriority() {
        return m_requestPriority != NO_PRIORITY;
    }

    public int getRequestPriority() {
        return m_requestPriority;
    }

    public boolean hasRequestTimeout() {
        return m_requestTimeout != NO_TIMEOUT;
    }

    public int getRequestTimeout() {
        return m_requestTimeout;
    }

    public void setRequestTimeout(int tmo) {
        if (tmo <= 0 && tmo != NO_TIMEOUT) {
            throw new IllegalArgumentException("Request timeout value must be positive.");
        }
        m_requestTimeout = tmo;
    }

    public int getSerializedSize() {
        // convert proc name to bytes if needed
        if (m_procNameBytes == null) {
            m_procNameBytes = m_procName.getBytes(UTF8Encoding);
        }

        // get extension sizes - if not present, size is 0 for each
        // 6 is one byte for ext type, one for size, and 4 for integer value
        int batchExtensionSize = m_batchTimeout != NO_TIMEOUT ? 6 : 0;

        // send allPartition too
        // 2 is one byte for ext type, one for size +
        // 6 is one byte for ext type, one for size, and 4 for integer value
        int partitionDestinationSize = hasPartitionDestination() ? 8 : 0;

        // the request priority if present
        // 3 is one byte for ext type, one for size, one for byte value
        int prioritySize = hasRequestPriority() ? 3 : 0;

        // the request timeout if present
        // 6 is one byte for ext type, one for size, 4 for integer value
        int reqTmoSize = hasRequestTimeout() ? 6 : 0;

        int size =
            1 + // type
            4 + m_procNameBytes.length + // procname
            8 + // client handle
            1 + // extension count
            batchExtensionSize + partitionDestinationSize + prioritySize + reqTmoSize + // extensions
            m_parameters.getSerializedSize(); // parameters
        assert(size > 0); // sanity
        return size;
    }

    public ByteBuffer flattenToBuffer(ByteBuffer buf) throws IOException {
        // convert proc name to bytes if needed
        if (m_procNameBytes == null) {
            m_procNameBytes = m_procName.getBytes(UTF8Encoding);
        }

        buf.put(CURRENT_MOST_RECENT_VERSION); //Version

        SerializationHelper.writeVarbinary(m_procNameBytes, buf);

        buf.putLong(m_clientHandle);

        // there are several possible extensions, count which apply
        byte extensionCount = 0;
        if (m_batchTimeout != NO_TIMEOUT) {
            ++extensionCount;
        }
        if (hasPartitionDestination()) {
            extensionCount += 2;
        }
        if (hasRequestPriority()) {
            ++extensionCount;
        }
        if (hasRequestTimeout()) {
           ++extensionCount;
        }

        // write the count as one byte
        buf.put(extensionCount);

        // write any extensions that apply
        if (m_batchTimeout != NO_TIMEOUT) {
            ProcedureInvocationExtensions.writeBatchTimeoutWithTypeByte(buf, m_batchTimeout);
        }
        if (hasPartitionDestination()) {
            ProcedureInvocationExtensions.writeAllPartitionWithTypeByte(buf);
            ProcedureInvocationExtensions.writePartitionDestinationWithTypeByte(buf, m_partitionDestination);
        }
        if (hasRequestPriority()) {
            ProcedureInvocationExtensions.writeRequestPriorityWithTypeByte(buf, m_requestPriority);
        }
        if (hasRequestTimeout()) {
            ProcedureInvocationExtensions.writeRequestTimeoutWithTypeByte(buf, m_requestTimeout);
        }

        m_parameters.flattenToBuffer(buf);

        return buf;
    }
}
