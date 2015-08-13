/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.importclient.kafka;

public final class Gap {
    long c = 0;
    long s = -1L;
    final long [] lag;

    public Gap(int leeway) {
        if (leeway <= 0) {
            throw new IllegalArgumentException("leeways is zero or negative");
        }
        lag = new long[leeway];
    }

    public Gap() {
        this(1024);
    }

    public synchronized void submit(long offset) {
        if (s == -1L && offset >= 0) {
            lag[idx(offset)] = c = s = offset;
        }
        if (offset > s) {
            s = offset;
        }
    }

    private final int idx(long offset) {
        return (int)offset % lag.length;
    }

    public synchronized void resetTo(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        lag[idx(offset)] = s = c = offset;
    }

    public synchronized long commit(long offset) {
        if (offset <= s && offset > c) {
            int ggap = (int)Math.min(lag.length, offset-c);
            if (ggap == lag.length) {
                c = offset - lag.length + 1;
                lag[idx(c)] = c;
            }
            lag[idx(offset)] = offset;
            while (ggap > 0 && lag[idx(c)]+1 == lag[idx(c+1)]) {
                ++c;
            }
        }
        return c;
    }
}
