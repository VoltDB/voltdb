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

package org.voltdb.exportclient;

public class ExportClientException extends Exception {

    private static final long serialVersionUID = 8519169468665494915L;

    enum Type {
        AUTH_FAILURE,
        DISCONNECT_UNEXPECTED,
        DISCONNECT_UPDATE,
        USER_ERROR
    }

    public final Type type;

    public ExportClientException(Throwable t) {
        super(t);
        type = Type.USER_ERROR;
    }
    public ExportClientException(Type type, String message) {
        super(message);
        this.type = type;
    }

    public ExportClientException(Type type, String message, Exception cause) {
        super(message, cause);
        this.type = type;
    }
}
