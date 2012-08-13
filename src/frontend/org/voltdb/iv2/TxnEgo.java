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

package org.voltdb.iv2;

import java.util.Calendar;
import java.util.Date;

/**
 * Encapsulates an Iv2 transaction id, timestamp and random number seed.
 */
final public class TxnEgo {

    // The transaction id layout.
    static final long UNUSED_SIGN_BITS = 1;
    static final long SEQUENCE_BITS = 49;
    static final long PARTITIONID_BITS = 14;

    // maximum values for the fields
    static final long SEQUENCE_MAX_VALUE = (1L << SEQUENCE_BITS) - 1L;
    static final int PARTITIONID_MAX_VALUE = (1 << PARTITIONID_BITS) - 1;

    // (Copy/Pasted (on purpose) from voltdb.TransactionIdManager)
    // The legacy transaction id included 40-bits of timestamp starting
    // from VOLT_EPOCH: time in millis since 1/1/2008 at 12am. Iv2 ids
    // are seeded from 12/31/2014 to guarantee uniqueness with previously
    // generated legacy ids.
    static final long VOLT_EPOCH = getEpoch();
    final public static long getEpoch() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(0);
        c.set(2008, 0, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.ZONE_OFFSET, 0);
        c.set(Calendar.DST_OFFSET, 0);
        long retval = c.getTimeInMillis();
        return retval;
    }

    // Create the starting SEQUENCE value. Must shift this left
    // to put the legacy transaction id seed in the most significant
    // 40-bits of the 49 bit sequence number (where it existed bit-wise
    // in the legacy id).
    static final long SEQUENCE_ZERO = (getSequenceZero() - getEpoch());
    private final static long getSequenceZero() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(0);
        c.set(2015, 0, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.ZONE_OFFSET, 0);
        c.set(Calendar.DST_OFFSET, 0);
        long retval = c.getTimeInMillis();
        return retval;
    }

    // per TxnEgo data.
    private final long m_txnId;
    private final long m_wallClock;

    /**
     * Make the zero-valued (initial) TxnEgo for a partition
     */
    public static TxnEgo makeZero(final long partitionId)
    {
        return new TxnEgo(SEQUENCE_ZERO, partitionId);
    }

    /**
     * Make the next sequence-valued TxnEgo
     */
    public TxnEgo makeNext()
    {
        return new TxnEgo(getSequence() + 1, getPartitionId());
    }

    public TxnEgo(long txnId) {
        this(getSequence(txnId), getPartitionId(txnId));
    }

    TxnEgo(long sequence, long partitionId)
    {
        if (sequence < SEQUENCE_ZERO) {
            throw new IllegalArgumentException("Invalid sequence value "
                    + sequence + " is less than minimum allowed value "
                    + SEQUENCE_ZERO);
        }
        else if (sequence > SEQUENCE_MAX_VALUE) {
            throw new IllegalArgumentException("Invalid sequence value "
                    + sequence + " is greater than maximum allowed value "
                    + SEQUENCE_MAX_VALUE);
        }
        else if (partitionId < 0) {
            throw new IllegalArgumentException("Invalid partitionId value "
                    + partitionId + ". Must be greater than or equal to 0.");
        }
        else if (partitionId > PARTITIONID_MAX_VALUE) {
            throw new IllegalArgumentException("Invalid partitionId value "
                    + partitionId + " is greater than maximum allowed value "
                    + PARTITIONID_MAX_VALUE);
        }

        m_txnId = (sequence << PARTITIONID_BITS) | partitionId;
        m_wallClock = System.currentTimeMillis();
    }

    final public long getTxnId()
    {
        return m_txnId;
    }

    final public long getWallClock()
    {
        return m_wallClock;
    }

    private static long getSequence(long txnId) {
        return txnId >> PARTITIONID_BITS;
    }

    public static long getPartitionId(long txnId) {
        return txnId & PARTITIONID_MAX_VALUE;
    }

    final public long getPartitionId()
    {
        return m_txnId & PARTITIONID_MAX_VALUE;
    }

    public long getSequence() {
        long seq = m_txnId >> PARTITIONID_BITS;
        return seq;
    }

    /**
     * Get a string representation of the TxnId
     */
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("TxnId: ").append(getTxnId());
        sb.append("  Sequence: ").append(getSequence());
        sb.append("  PartitionId: ").append(getPartitionId());
        sb.append("  Wallclock: ").append(getWallClock());
        Date date = new Date(getWallClock());
        sb.append("  Date: ").append(date.toString());
        return sb.toString();
    }

    public String toBitString() {
        String retval = "";
        long mask = 0x8000000000000000L;
        for(int i = 0; i < 64; i++) {
            if ((getTxnId() & mask) == 0) retval += "0";
            else retval += "1";
            mask >>>= 1;
        }
        return retval;
    }
}
