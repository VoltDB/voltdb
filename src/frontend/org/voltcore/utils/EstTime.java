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

package org.voltcore.utils;


/**
 * A class for retrieving an estimated indication of the current time.
 * Very fast since the field is updated less frequently and only updated occasionally.
 * Should be cachable by the CPU and the cache will only be invalidated when it is updated.
 * Thanks MESI!
 *
 * CAVEAT The time is not guaranteed to always increase (can move backwards) if there
 * are multiple VoltNetwork's present in the process.
 */
public class EstTime {

    static volatile long m_now = System.currentTimeMillis();

    static {
        try {
            Class.forName("org.voltcore.utils.EstTimeUpdater");
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static long currentTimeMillis() {
        return m_now;
    }
}
