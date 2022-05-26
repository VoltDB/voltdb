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

package org.voltdb.iv2;

import org.voltdb.messaging.CompleteTransactionMessage;

public class MpRestartSequenceGenerator {
    // bit sizes for each of the fields in the 64-bit id
    // note, these add up to 63 bits to make dealing with
    // signed / unsigned conversions easier.
    // having MPI promotions for up to 1,000,000 Rejoined nodes, should be enough
    // for now. Restart flag reset means that this came from a SPI leader promotion
    static final long LEADERID_BITS = 20;
    static final long RESTART_BITS = 1;
    static final long COUNTER_BITS = 42;

    static final long LEADERID_MAX_VALUE = (1L << LEADERID_BITS) - 1L;
    static final long RESTART_MAX_VALUE = (1L << RESTART_BITS) - 1L;
    static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    private final long m_highOrderFields;
    private long m_counter = 0;

    public MpRestartSequenceGenerator(int leaderId, boolean forRestart) {
        assert (leaderId <= LEADERID_MAX_VALUE);
        m_highOrderFields = ((long)leaderId << (COUNTER_BITS + RESTART_BITS))
                          | (forRestart ? (1L << COUNTER_BITS) : 0);
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
        return ((restartSeqId >> COUNTER_BITS) & RESTART_MAX_VALUE) == 1;
    }

    public static int getLeaderId(long restartSeqId) {
        return (int) (restartSeqId >> (COUNTER_BITS + RESTART_BITS));
    }

    public static String restartSeqIdToString(long restartSeqId)
    {
        if (restartSeqId == CompleteTransactionMessage.INITIAL_TIMESTAMP) {
            return "(INITIAL)";
        }
        return "(" + MpRestartSequenceGenerator.getLeaderId(restartSeqId) + ":" +
                MpRestartSequenceGenerator.getSequence(restartSeqId) + (isForRestart(restartSeqId) ? "R)" : ")");
    }

    public static void restartSeqIdToString(long restartSeqId, StringBuilder sb)
    {
        if (restartSeqId == CompleteTransactionMessage.INITIAL_TIMESTAMP) {
            sb.append("(INITIAL)");
            return;
        }
        sb.append("(").append(MpRestartSequenceGenerator.getLeaderId(restartSeqId)).append(":");
        sb.append(MpRestartSequenceGenerator.getSequence(restartSeqId));
        if (isForRestart(restartSeqId)) {
            sb.append("R)");
        }
        else {
            sb.append(")");
        }
    }
}
