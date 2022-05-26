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

import java.net.InetAddress;

/**
 * Returns a usable loopback address.
 *
 * This is the single point of truth for this address in
 * the VoltDB server. If there is future need, we can build
 * in alternative arrangements in this one place.
 */
public class LoopbackAddress {

    private static String loopbackAddress;

    private static synchronized void initAddrOnce() {
        if (loopbackAddress == null) {
            loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();
        }
    }

    public static String get() {
        initAddrOnce();
        return loopbackAddress;
    }
}
