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
 * This exception can be thrown by any of the {@link Client2} methods
 * that call a VoltDB procedure. In practice, it is more likely to
 * be encountered when using an async method. See, for example,
 * {@link Client2#callProcedureAsync(String,Object...)}.
 * <p>
 * The exception indicates that the client has exceeded the hard request limit
 * on concurrent requests. The limit can be changed by use of
 * {@link Client2Config#clientRequestLimit(int)} prior to constructing
 * the {@link Client2} object.
 * <p>
 * For <code>callProcedureAsync</code> variants, the exception is
 * delivered through the <code>CompletableFuture</code>, as is usual.
 * <p>
 * For <code>callProcedureSync</code> variants, the exception is raised
 * directly. However, unless highly multi-threaded, a synchronous
 * caller is unlikely to be able to exceed the limit. For this
 * reason, the <code>RequestLimitException</code> is not defined
 * as a checked exception.
 */
public class RequestLimitException extends RuntimeException {
    private static final long serialVersionUID = 1770135057483867828L;
    RequestLimitException(String msg) {
        super(msg);
    }
}
