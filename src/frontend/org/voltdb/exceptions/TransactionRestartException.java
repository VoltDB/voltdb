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

package org.voltdb.exceptions;

import com.google_voltpatches.common.collect.Maps;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.voltdb.client.ClientResponse;

/**
 * This exception is used in IV2 MPI repair to terminate the currently running
 * MP transaction at the MPI.  We have a separate exception type so that we can
 * use that to create a unique ClientResponse status so that the running
 * procedure can determine whether or not it is being restarted or whether it
 * completed successfully.
 */
public class TransactionRestartException extends SerializableException {
    public static final long serialVersionUID = 0L;
    private long m_txnId;
    private boolean m_misrouted = false;
    // need for restartFragment locally, don't need to be serialized
    private final List<Long> m_iv2Masters;
    private final Map<Integer, Long> m_partitionMasters;

    public TransactionRestartException(String message, long txnId) {
        super(message);
        m_txnId = txnId;
        m_iv2Masters = new ArrayList<>();
        m_partitionMasters = Maps.newHashMap();
    }

    public TransactionRestartException(ByteBuffer b) {
        super(b);
        m_txnId = b.getLong();
        m_misrouted = b.get() == 1;
        m_iv2Masters = new ArrayList<>();
        m_partitionMasters = Maps.newHashMap();
    }

    public long getTxnId()
    {
        return m_txnId;
    }

    public void setMisrouted(boolean misrouted) {
        m_misrouted = misrouted;
    }

    public boolean isMisrouted() {
        return m_misrouted;
    }

    public void updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters) {
        m_iv2Masters.clear();
        m_iv2Masters.addAll(replicas);
        m_partitionMasters.clear();
        m_partitionMasters.putAll(partitionMasters);
    }

    public List<Long> getMasterList() {
        return m_iv2Masters;
    }

    public Map<Integer, Long> getPartitionMasterMap() {
        return m_partitionMasters;
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.TransactionRestartException;
    }

    @Override
    protected int p_getSerializedSize() {
        return 8 + 1;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        b.putLong(m_txnId);
        b.put(m_misrouted ? (byte) 1 : (byte) 0);
    }

    @Override
    public byte getClientResponseStatus() {
        if (isMisrouted()) {
            return ClientResponse.TXN_MISROUTED;
        } else {
            return ClientResponse.TXN_RESTART;
        }
    }

    @Override
    public String getShortStatusString() {
        if (isMisrouted()) {
            return "TRANSACTION MISROUTED";
        } else {
            return "TRANSACTION RESTART";
        }
    }
}
