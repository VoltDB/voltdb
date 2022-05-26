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

import org.voltdb.VoltDB;
import org.voltdb.utils.MiscUtils;

public class PortGenerator {
    private static int portOffset = 100; // Shift ports away from defaults for testing
    private int nextPort = 12000;
    private int nextCport;
    private int nextAport;
    private int nextKport;

    final int MIN_STATIC_PORT = 10000;
    final int MAX_STATIC_PORT = 49151;

    public PortGenerator() {
        reset();
    }

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

    public synchronized int nextHttp() {
        while(nextAport >= MIN_STATIC_PORT) {
            int port = nextAport--;
            if (MiscUtils.isBindable(port)) {
                return port;
            }
        }
        throw new RuntimeException("Exhausted all possible http ports");
    }

    public synchronized int nextTopics() {
        while (nextCport <= MAX_STATIC_PORT) {
            int port = nextKport++;
            if (MiscUtils.isBindable(port)) {
                return port;
            }
        }
        throw new RuntimeException("Exhausted all possible topics ports");
    }

    public synchronized void reset() {
        nextCport = VoltDB.DEFAULT_PORT + portOffset;
        nextAport = VoltDB.DEFAULT_ADMIN_PORT + portOffset;
        nextKport = VoltDB.DEFAULT_TOPICS_PORT + portOffset;
    }
}
