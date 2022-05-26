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

package org.voltcore.network;

import java.io.IOException;

public class TLSException extends RuntimeException {

    private static final long serialVersionUID = -8248288352509258766L;

    public TLSException() {
    }

    public TLSException(String message) {
        super(message);
    }

    public TLSException(Throwable cause) {
        super(cause);
    }

    public TLSException(String message, Throwable cause) {
        super(message, cause);
    }

    public static TLSException inChain(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof TLSException) {
                return (TLSException)cause;
            }
            cause = cause.getCause();
        }
        return null;
    }

    public static IOException ioCause(Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            if (cause instanceof IOException) {
                return (IOException)cause;
            }
            cause = cause.getCause();
        }
        return null;
    }
}
