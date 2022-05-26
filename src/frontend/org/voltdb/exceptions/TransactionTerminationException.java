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

import java.nio.ByteBuffer;

import org.voltdb.client.ClientResponse;

/**
 * This exception is used in IV2 MPI to terminate the currently running
 * MP transaction at the MPI while a node is being shutdown
 */
public class TransactionTerminationException extends SerializableException {
    public static final long serialVersionUID = 0L;
    private long m_txnId;

    public TransactionTerminationException(String message, long txnId) {
        super(message);
        m_txnId = txnId;
    }

    public TransactionTerminationException(ByteBuffer b) {
        super(b);
        m_txnId = b.getLong();
    }

    public long getTxnId()
    {
        return m_txnId;
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.TransactionTerminationException;
    }

    @Override
    protected int p_getSerializedSize() {
        return 8;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        b.putLong(m_txnId);
    }

    @Override
    public byte getClientResponseStatus() {
        return ClientResponse.UNEXPECTED_FAILURE;
    }

    @Override
    public String getShortStatusString() {
        return "Transaction Interrupted";
    }
}
