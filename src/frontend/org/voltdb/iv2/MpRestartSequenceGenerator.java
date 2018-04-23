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
    // having up to 1,000,000 MPI promotions, should be enough for now.
    static final long NODEID_BITS = 20;
    static final long REPAIR_BITS = 1;
    static final long COUNTER_BITS = 22;

    static final long NODEID_MAX_VALUE = (1L << NODEID_BITS) - 1L;
    static final long REPAIR_MAX_VALUE = (1L << REPAIR_BITS) - 1L;
    static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    private final long m_highOrderFields;
    private long m_counter = 0;

    public MpRestartSequenceGenerator(int nodeId, boolean forRestart) {
        assert (nodeId <= NODEID_MAX_VALUE);
        m_highOrderFields = nodeId << (COUNTER_BITS + REPAIR_BITS)
                          | (forRestart ? (1 << NODEID_BITS) : 0);
    }

    public long getNextSeqNum() {
        m_counter++;
        long counter = m_counter;
        return makeSequenceNumber(counter);
    }

    private long makeSequenceNumber(long counter) {
        assert (counter <= COUNTER_MAX_VALUE);
        long seq = m_highOrderFields;
        seq |= counter;
        return seq;
    }

    public static long getSequence(long restartSeqId) {
        return restartSeqId & COUNTER_MAX_VALUE;
    }

    public static boolean isForRestart(long restartSeqId) {
        return ((restartSeqId >> COUNTER_BITS) & REPAIR_MAX_VALUE) == 1;
    }

    public static int getNodeId(long restartSeqId) {
        return (int) (restartSeqId >> (COUNTER_BITS + REPAIR_BITS));
    }

    public static String restartSeqIdToString(long restartSeqId)
    {
        return "(" + MpRestartSequenceGenerator.getNodeId(restartSeqId) + ":" +
                MpRestartSequenceGenerator.getSequence(restartSeqId) + ")";
    }

    public static void restartSeqIdToString(long restartSeqId, StringBuilder sb)
    {
        sb.append("(").append(MpRestartSequenceGenerator.getNodeId(restartSeqId)).append(":");
        sb.append(MpRestartSequenceGenerator.getSequence(restartSeqId));
        if (isForRestart(restartSeqId)) {
            sb.append("R)");
        }
        else {
            sb.append(")");
        }
    }
}
