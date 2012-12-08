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

package org.voltcore.utils;

import org.voltdb.VoltDB;
import org.voltdb.utils.MiscUtils;

public class PortGenerator {
    private int nextPort = 12000;
    private static int portOffset = 100;    // Shift ports away from defaults for testing
    private int nextCport = VoltDB.DEFAULT_PORT+portOffset;
    private int nextAport = VoltDB.DEFAULT_ADMIN_PORT+portOffset;

    final int MIN_STATIC_PORT = 10000;
    final int MAX_STATIC_PORT = 49151;

    public synchronized void setNext(int port) {
        nextPort = port;
    }

    /** Return the next bindable port */
    public synchronized int next() {
        while(nextPort <= MAX_STATIC_PORT) {
            int port = nextPort++;
            if (MiscUtils.isBindable(port)) {
                return port;
            }
        }
        throw new RuntimeException("Exhausted all possible ports");
    }
    public synchronized int nextClient() {
        while(nextCport <= MAX_STATIC_PORT) {
            int port = nextCport++;
            if (MiscUtils.isBindable(port)) {
                return port;
            }
        }
        throw new RuntimeException("Exhausted all possible client ports");
    }
    public synchronized int nextAdmin() {
        while(nextAport >= MIN_STATIC_PORT) {
            int port = nextAport--;
            if (MiscUtils.isBindable(port)) {
                return port;
            }
        }
        throw new RuntimeException("Exhausted all possible admin ports");
    }
    public synchronized void reset() {
        nextCport = VoltDB.DEFAULT_PORT+portOffset;
        nextAport = VoltDB.DEFAULT_ADMIN_PORT+portOffset;
    }
}
