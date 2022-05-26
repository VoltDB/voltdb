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
 * Thrown when it has been decided that a query is taking too
 * long and needs to stop.
 */
public class InterruptException extends SerializableException {
    public static final long serialVersionUID = 0L;

    public InterruptException(int errorCode) {
        super();
    }

    public InterruptException(ByteBuffer b) {
        super(b);
    }

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.InterruptException;
    }

    @Override
    protected int p_getSerializedSize() {
        return 0;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
    }

    @Override
    public byte getClientResponseStatus() {
        return ClientResponse.GRACEFUL_FAILURE;
    }

    @Override
    public String getShortStatusString() {
        return "Transaction Interrupted";
    }
}
