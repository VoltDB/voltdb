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
    private byte m_procNameBytes[] = null;
    private final int m_batchTimeout;
    private final ParameterSet m_parameters;
    private final boolean m_allPartition;

    // pre-cache this for serialization
    // this duplicates some other code, but it's nice to keep the client code
    //  self-contained (see Constants.UTF8ENCODING)
    private static final Charset UTF8Encoding = Charset.forName("UTF-8");

    public ProcedureInvocation(long handle, String procName, Object... parameters) {
        this(handle, BatchTimeoutOverrideType.NO_TIMEOUT, false, procName, parameters);
    }

    public ProcedureInvocation(long handle, int batchTimeout, String procName, Object... parameters) {
        this(handle, batchTimeout, false, procName, parameters);
    }

    public ProcedureInvocation(long handle, int batchTimeout, boolean allPartition, String procName, Object... parameters) {
        if ((batchTimeout < 0) && (batchTimeout != BatchTimeoutOverrideType.NO_TIMEOUT)) {
            throw new IllegalArgumentException("Timeout value can't be negative." );
        }

        m_clientHandle = handle;
        m_procName = procName;
        m_parameters = (parameters != null
                            ? ParameterSet.fromArrayWithCopy(parameters)
                            : ParameterSet.emptyParameterSet());

        m_batchTimeout = batchTimeout;
        m_allPartition = allPartition;
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

    public boolean getAllPartition() {
        return m_allPartition;
    }

    public int getSerializedSize() {
        // convert proc name to bytes if needed
        if (m_procNameBytes == null) {
            m_procNameBytes = m_procName.getBytes(UTF8Encoding);
        }

        // get extension sizes - if not present, size is 0 for each
        // 6 is one byte for ext type, one for size, and 4 for integer value
        int batchExtensionSize = m_batchTimeout != BatchTimeoutOverrideType.NO_TIMEOUT ? 6 : 0;
        // 2 is one byte for ext type, one for size
        int allPartitionExtensionSize = m_allPartition ? 2 : 0;

        int size =
            1 + // type
            4 + m_procNameBytes.length + // procname
            8 + // client handle
            1 + // extension count
            batchExtensionSize + allPartitionExtensionSize + // extensions
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

        m_parameters.flattenToBuffer(buf);

        return buf;
    }
}
