/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import com.google_voltpatches.common.base.Charsets;

/**
 * SpecifiedException contains the exact status code and message the thrower wants
 * shoved into the ClientResponse that the user sees inside, say, a ProcCallException.
 *
 * It's useful in sysprocs to return instead of a VoltAbortException. VAE is always
 * interpreted as an unexpected failure that will print a stack trace inside a
 * sysproc. This allows a VoltDB developer to return GRACEFUL_FAILURE for example,
 * with a clear error message instead of a stack trace.
 *
 */
public class SpecifiedException extends SerializableException {

    // stupid java demands this!
    private static final long serialVersionUID = -2453748851762681430L;

    // core ClientResponseImpl features
    byte m_status;
    byte[] m_statusStringBytes;

    public SpecifiedException(byte status, String statusString) {
        m_status = status;
        m_statusStringBytes = statusString.getBytes(Charsets.UTF_8);
    }

    public SpecifiedException(ByteBuffer buffer) {
        super(buffer);
        m_status = buffer.get();
        int statusStringBytesLenth = buffer.getInt();
        m_statusStringBytes = new byte[statusStringBytesLenth];
        buffer.get(m_statusStringBytes);
    }

    @Override
    public String toString() {
        return getMessage();
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.SpecifiedException;
    }

    @Override
    protected int p_getSerializedSize() {
        // ... + 8 + string prefix + string length + ...
        return super.p_getSerializedSize()
            + 1 // status byte
            + 4 // status string length
            + m_statusStringBytes.length;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        super.p_serializeToBuffer(b);
        b.put(m_status);
        b.putInt(m_statusStringBytes.length);
        b.put(m_statusStringBytes);
    }

    @Override
    public String getMessage() {
        return new String(m_statusStringBytes, Charsets.UTF_8);
    }

    public byte getStatus() {
        return m_status;
    }
}
