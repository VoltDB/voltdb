/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

/**
 * This exception can be thrown by any of the synchronous
 * {@link Client2} methods that call a VoltDB procedure. See
 * for example {@link Client2#callProcedureSync(String,Object...)}.
 * <p>
 * A <code>GeneralException</code> is used to wrap an unanticipated
 * checked exception, turning it into an unchecked exception.
 * This is a necessary tchnicality when the checked exception has
 * not been listed in a <code>throws</code> list.
 * <p>
 * Use {@link Throwable#getCause()} to retrieve the checked
 * exception.
 * <p>
 * This exception is used only for <code>Client2</code> clients.
 */
public class GeneralException extends RuntimeException {
    private static final long serialVersionUID = 7749774789125927801L;
    GeneralException(Throwable cause) {
        super(cause);
    }
}
