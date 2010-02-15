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

package org.voltdb.exceptions;

import java.nio.ByteBuffer;

/**
 * Exceptions that are intended to be caught by the user in a stored procedure are SQLExceptions
 * or extend SQLException. Normal operation of Volt should continue after a SQLException
 * has been caught by the stored procedure or passed back to the client in the failure response.
 */
public class SQLException extends SerializableException {

    public static final long serialVersionUID = 0L;

    /**
     * Conctructor that deserializes the SQLState code and error message from a ByteBuffer
     * @param buffer ByteBuffer containing a serialized representation of the exception.
     */
    public SQLException(ByteBuffer buffer) {
        super(buffer);
        final byte sqlStateBytes[] = new byte[5];
        buffer.get(sqlStateBytes);
        m_sqlState = new String(sqlStateBytes);
    }

    /**
     * Retrieve the SQLState code for the error that generated this exception.
     * @return Five character SQLState code.
     */
    public String getSQLState() { return m_sqlState; }

    /**
     * Storage for the five character SQLState code
     */
    private final String m_sqlState;

    /**
     * Return the amount of storage necesary to store the 5 character SQL state, 2 byte string length, and message.
     */
    @Override
    protected short p_getSerializedSize() {
        return (short)(5);// sqlState + messageBytesLength + messageBytes
    }

    /**
     * Serialize the five character SQLState, 2 byte string length, and detail message to the
     * provided ByteBuffer
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        b.put(m_sqlState.getBytes());
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.SQLException;
    }
}
