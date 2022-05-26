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

/**
 * Exceptions thrown by native Execution Engine
 * that should only be caught by Volt
 * We don't throw exceptions directly from JNI for portability.
 * Instead, this Exception has an error code which specifies the
 * reason of the exception. Generally these should
 * be errors that are not caused by the user and that are unexpected.
 * They may be fatal and bring down the cluster, but they dont' have to
 * if the code throwing the Exception knows that no data has been corrupted
 * and that operation can continue.
 */
public class EEException extends SerializableException {
    public static final long serialVersionUID = 0L;

    public EEException(int errorCode) {
        super();
        this.m_errorCode = errorCode;
    }

    public EEException(ByteBuffer b) {
        super(b);
        this.m_errorCode = b.getInt();
    }

    public int getErrorCode() { return m_errorCode;}
    private final int m_errorCode;

    @Override
    protected SerializableExceptions getExceptionType() {
        return SerializableExceptions.EEException;
    }

    @Override
    protected int p_getSerializedSize() {
        return 4;
    }

    @Override
    protected void p_serializeToBuffer(ByteBuffer b) {
        b.putInt(m_errorCode);
    }
}
