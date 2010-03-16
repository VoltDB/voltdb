/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.util.Calendar;
import java.util.Date;

/**
 * <p>The TransactionIdManager creates Transaction ids that
 * get assigned to VoltDB timestamps. A transaction id contains
 * three fields, the time of creation, a counter to ensure local
 * ordering, and the siteid of the generating site.</p>
 *
 * <p>This class also contains methods to examine the embedded values of
 * transaction ids.</p>
 *
 * <p>If the clocks of two different machines are reasonably in sync,
 * txn ids created at the same time on different machines will be reasonably
 * close in value. Thus transaction ids can be used for a global ordering.</p>
 *
 */
public class TransactionIdManager {
    // bit sizes for each of the fields in the 64-bit id
    // note, these add up to 63 bits to make dealing with
    // signed / unsigned conversions easier.
    static final long TIMESTAMP_BITS = 40;
    static final long COUNTER_BITS = 13;
    static final long INITIATORID_BITS = 10;

    // VOLT_EPOCH holds the time in millis since 1/1/2008 at 12am.
    // The current time - VOLT_EPOCH should fit nicely in 40 bits
    // of memory.
    static final long VOLT_EPOCH = getEpoch();
    public static long getEpoch() {
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2008, 0, 1);
        return c.getTimeInMillis();
    }

    // maximum values for the fields
    // used for bit-shifts and error checking
    static final long TIMESTAMP_MAX_VALUE = (1L << TIMESTAMP_BITS) - 1L;
    static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    static final long INITIATORID_MAX_VALUE = (1L << INITIATORID_BITS) - 1L;

    // the local siteid
    long initiatorId;
    // the time of the previous txn id generation
    long lastUsedTime = -1;
    // the number of txns generated during the same value
    // for System.currentTimeMillis()
    long counterValue = 0;

    // remembers the last txn generated
    long lastTxnId = 0;

    /**
     * Initialize the TransactionIdManager for this site
     * @param siteId The siteId of the current site.
     */
    public TransactionIdManager(int initiatorId) {
        this.initiatorId = initiatorId;
    }

    /**
     * Generate a unique id that contains a timestamp, a counter
     * and a siteid packed into a 64-bit long value. Subsequent calls
     * to this method will return strictly larger long values.
     * @return The newly generated transaction id.
     */
    public long getNextUniqueTransactionId() {
        long currentTime = System.currentTimeMillis();
        if (currentTime == lastUsedTime) {
            // increment the counter for this millisecond
            counterValue++;

            // handle the case where we've run out of counter values
            // for this particular millisecond (feels unlikely)
            if (counterValue > COUNTER_MAX_VALUE) {
                // spin until the next millisecond
                while (currentTime == lastUsedTime)
                    currentTime = System.currentTimeMillis();
                // reset the counter and lastUsedTime for the new millisecond
                lastUsedTime = currentTime;
                counterValue = 0;
            }
        }
        else {
            // reset the counter and lastUsedTime for the new millisecond
            if (currentTime < lastUsedTime) {
                System.err.println("Initiator time moved backwards from: " + lastUsedTime + " to " + currentTime);
                VoltDB.crashVoltDB();
            }
            lastUsedTime = currentTime;
            counterValue = 0;
        }

        lastTxnId = makeIdFromComponents(currentTime, counterValue, initiatorId);

        return lastTxnId;
    }

    static long makeIdFromComponents(long ts, long seqNo, long initiatorId) {
        // compute the time in millis since VOLT_EPOCH
        long txnId = ts - VOLT_EPOCH;
        // verify all fields are the right size
        assert(txnId <= TIMESTAMP_MAX_VALUE);
        assert(seqNo <= COUNTER_MAX_VALUE);
        assert(initiatorId <= INITIATORID_MAX_VALUE);

        // put this time value in the right offset
        txnId = txnId << (COUNTER_BITS + INITIATORID_BITS);
        // add the counter value at the right offset
        txnId |= seqNo << INITIATORID_BITS;
        // finally add the siteid at the end
        txnId |= initiatorId;

        return txnId;
    }

    /**
     * Given a transaction id, return the time of its creation
     * by examining the embedded timestamp.
     * @param txnId The transaction id value to examine.
     * @return The Date object representing the time this transaction
     * id was created.
     */
    public static Date getDateFromTransactionId(long txnId) {
        long time = txnId >> (COUNTER_BITS + INITIATORID_BITS);
        time += VOLT_EPOCH;
        return new Date(time);
    }

    /**
     * Given a transaction id, return the time of its creation
     * by examining the embedded timestamp.
     * @param txnId The transaction id value to examine.
     * @return The integer representing the time this transaction
     * id was created.
     */
    public static long getTimestampFromTransactionId(long txnId) {
        long time = txnId >> (COUNTER_BITS + INITIATORID_BITS);
        time += VOLT_EPOCH;
        return time;
    }

    /**
     * Given a transaction id, return the embedded site id.
     * @param txnId The transaction id value to examine.
     * @return The site id embedded within the transaction id.
     */
    public static long getInitiatorIdFromTransactionId(long txnId) {
        return txnId & INITIATORID_MAX_VALUE;
    }

    public static long getSequenceNumberFromTransactionId(long txnId) {
        long seq = txnId >> INITIATORID_BITS;
        seq = seq & COUNTER_MAX_VALUE;
        return seq;
    }

    /**
     * Get the last txn id generated.
     * @return The last txn id generated.
     */
    public long getLastTxnId() {
        return lastTxnId;
    }

    /**
     * Get a string representation of the TxnId
     */
    public static String toString(long txnId) {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("Timestamp: ").append(getTimestampFromTransactionId(txnId));
        sb.append(".").append(getSequenceNumberFromTransactionId(txnId));
        sb.append(" InititatorId: ").append(getInitiatorIdFromTransactionId(txnId));
        return sb.toString();
    }

    public static String toBitString(long txnId) {
        String retval = "";
        long mask = 0x8000000000000000L;
        for(int i = 0; i < 64; i++) {
            if ((txnId & mask) == 0) retval += "0";
            else retval += "1";
            mask >>>= 1;
        }
        return retval;
    }
}
