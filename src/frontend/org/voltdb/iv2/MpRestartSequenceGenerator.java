/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.iv2;

public class MpRestartSequenceGenerator {
    // bit sizes for each of the fields in the 64-bit id
    // note, these add up to 63 bits to make dealing with
    // signed / unsigned conversions easier.
    // scale up to 1024-node cluster, should be enough for now.
    static final long NODEID_BITS = 10;
    static final long COUNTER_BITS = 54;

    static final long NODEID_MAX_VALUE = (1L << NODEID_BITS) - 1L;
    static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    private int m_leaderElectorId;
    private long m_counter = 0;

    public MpRestartSequenceGenerator(int nodeId) {
        m_leaderElectorId = nodeId;
    }

    public long getNextSeqNum() {
        m_counter++;
        long counter = m_counter;
        return makeSequenceNumber(m_leaderElectorId, counter);
    }

    private long makeSequenceNumber(int nodeId, long counter) {
        assert (nodeId <= NODEID_MAX_VALUE);
        assert (counter <= COUNTER_MAX_VALUE);
        long seq = nodeId << COUNTER_BITS;
        seq |= counter;
        return seq;
    }

}
