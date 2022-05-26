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

import java.util.concurrent.TimeUnit;

/**
 * Container for per-call options for a <code>Client2</code> VoltDB
 * client. A <code>Client2CallOptions</code> can be used with
 * {@link Client2#callProcedureAsync(Client2CallOptions,String,Object...) callProcedureAsync} or
 * {@link Client2#callProcedureSync(Client2CallOptions,String,Object...) callProcedureSync} to
 * pass in override options for specific calls.
 * <p>
 * Using a single options class helps avoid combinatorial
 * explosion of options.
 * <p>
 * Options not explicity set will assume the values set up
 * when the client was configured. VoltDB recommends using
 * <code>Client2Config</code> to set values that will remain
 * unchanged for the life of the client, with this
 * <code>Client2CallOptions</code> class being used only
 * when variance between calls is needed.
 *
 * @see Client2
 * @see Client2Config
 */
public class Client2CallOptions {

    // All these have package access for use by Client2Impl
    // All times are in nanoseconds

    Long clientTimeout;
    Long queryTimeout;
    Integer requestPriority;

    /**
     * The constructor. All options are initialized
     * to their configured or default values, as appropriate.
     */
    public Client2CallOptions() {
    }

    /**
     * Sets the client-side timeout for a procedure call.
     * A zero or negative value means there is no limit.
     * <p>
     * If a call has received no response from VoltDB in the specified
     * time, it will be completed with a timeout error.
     *
     * @param timeout the timeout interval
     * @param unit the units in which the timeout was expressed
     * @return this
     */
    public Client2CallOptions clientTimeout(long timeout, TimeUnit unit) {
        clientTimeout = timeout > 0 ? unit.toNanos(timeout) : Long.MAX_VALUE;
        return this;
    }

    /**
     * Sets the server-side query timeout for a read-only procedure call
     * or batch of procedure calls. This has no effect on read-write
     * transactions. A zero or negative value means there is no limit.
     * <p>
     * This timeout may override the value specified for <code>querytimeout</code>
     * in the <code>systemsettings</code> element of the VoltDB cluster
     * configuration file. Admin privilege is required to set a timeout
     * longer than the global value.
     *
     * @param timeout the timeout interval
     * @param unit the units in which the timeout was expressed
     * @return this
     */
    public Client2CallOptions queryTimeout(long timeout, TimeUnit unit) {
        queryTimeout = timeout > 0 ? unit.toNanos(timeout) : Long.MAX_VALUE;
        return this;
    }

    /**
     * Sets the request priority for a procedure call.
     * <p>
     * The valid priority range is from {@link Client2Config#HIGHEST_PRIORITY}
     * to {@link Client2Config#LOWEST_PRIORITY}, inclusive. Higher priorities
     * have lower numerical values.
     *
     * @param prio priority
     * @return this
     */
    public Client2CallOptions requestPriority(int prio) {
        requestPriority = Client2Config.checkRequestPriority(prio);
        return this;
    }
}
