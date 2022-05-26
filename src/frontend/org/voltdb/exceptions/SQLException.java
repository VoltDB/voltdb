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

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.voltdb.client.ClientResponse;

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
        m_sqlState = new byte[5];
        buffer.get(m_sqlState);
        String state = getSQLState();
        assert(state.length() == 5);
    }

    public SQLException(String sqlState, String message) {
        super(message);
        assert(sqlState.length() == 5);
        byte[] sqlStateBytes = null;
        sqlStateBytes = sqlState.getBytes(StandardCharsets.UTF_8);
        assert(sqlStateBytes.length == 5);
        m_sqlState = sqlStateBytes;
    }

    public SQLException(String sqlState) {
        assert(sqlState.length() == 5);
        byte[] sqlStateBytes = null;
        sqlStateBytes = sqlState.getBytes(StandardCharsets.UTF_8);
        assert(sqlStateBytes.length == 5);
        m_sqlState = sqlStateBytes;
    }

    /**
     * Retrieve the SQLState code for the error that generated this exception.
     * @return Five character SQLState code.
     */
    public String getSQLState()
    {
        String state = null;
        try {
            state = new String(m_sqlState, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        return state;
    }

    /**
     * Storage for the five character SQLState code
     */
    private final byte[] m_sqlState;

    /**
     * Return the amount of storage necessary to store the 5 character SQL state, SerializableException accounts for the rest
     */
    @Override
    protected int p_getSerializedSize() {
        return 5;// messageBytes
    }

    /**
     * Serialize the five character SQLState to the provided ByteBuffer
     */
    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        assert (m_sqlState.length == 5);
        b.put(m_sqlState);
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.SQLException;
    }

    @Override
    public byte getClientResponseStatus() {
        return ClientResponse.GRACEFUL_FAILURE;
    }

    @Override
    public String getShortStatusString() {
        return "SQL ERROR";
    }
}
