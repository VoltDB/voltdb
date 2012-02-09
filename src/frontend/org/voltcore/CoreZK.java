/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltcore;

/**
 * CoreZK provides constants for all voltcore-registered
 * ZooKeeper paths.
 */
public class CoreZK {
    private static final String root = "/core";

    // hosts in the current system (ephemeral)
    public static final String hosts = "/core/hosts";
    public static final String hosts_host = "/core/hosts/host";
    public static final String readyhosts = "/core/readyhosts";
    public static final String readyhosts_host = "/core/readyhosts/host";

    // hosts since beginning of time (persistent)
    public static final String hostids = "/core/hostids";
    public static final String hostids_host = "/core/hostids/host";

    // Persistent nodes (mostly directories) to create on startup
    public static final String[] ZK_HIERARCHY = {
        root, hosts, readyhosts, hostids
    };

}
