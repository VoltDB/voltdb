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

package org.voltdb.iv2;

import java.util.Calendar;
import java.util.Date;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;

/**
 * <p>The UniqueIdGenerator creates unique ids that
 * is assigned to VoltDB transactions. A unique id contains
 * three fields, the time of creation, a counter to ensure local
 * ordering, and the partition id of the generating site.</p>
 *
 * <p>This class also contains methods to examine the embedded values of
 * unique ids.</p>
 *
 * <p>If the clocks of two different machines are reasonably in sync,
 * unique ids created at the same time on different machines will be reasonably
 * close in value.</p>
 *
 */
public class UniqueIdGenerator {
    // bit sizes for each of the fields in the 64-bit id
    // note, these add up to 63 bits to make dealing with
    // signed / unsigned conversions easier.
    static final long TIMESTAMP_BITS = 40;
    static final long COUNTER_BITS = 9;
    static final long PARTITIONID_BITS = 14;

    // EE datefunction.h file defines these constant values also.

    // VOLT_EPOCH holds the time in millis since 1/1/2008 at 12am.
    // The current time - VOLT_EPOCH should fit nicely in 40 bits
    // of memory.
    static final long VOLT_EPOCH = getEpoch();
    public static long getEpoch() {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(0);
        c.set(2008, 0, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.set(Calendar.ZONE_OFFSET, 0);
        c.set(Calendar.DST_OFFSET, 0);
        long retval = c.getTimeInMillis();
        return retval;
    }

    // maximum values for the fields
    // used for bit-shifts and error checking
    static final long TIMESTAMP_MAX_VALUE = (1L << TIMESTAMP_BITS) - 1L;
    static final long COUNTER_MAX_VALUE = (1L << COUNTER_BITS) - 1L;
    static final long PARTITIONID_MAX_VALUE = (1L << PARTITIONID_BITS) - 1L;

    // the local siteid
    long partitionId;
    // the time of the previous unique id generation
    long lastUsedTime = -1;
    // the number of unique ids generated during the same value
    // for System.currentTimeMillis()
    long counterValue = 0;

    // remembers the last unique id generated
    long lastUniqueId = 0;


    // salt used for testing to simulate clock skew
    // when non-zero, getting the time will be adjusted
    // by this value
    long m_timestampTestingSalt = 0;

    /*
     * If we detect time moving backwards, we will generate this offset
     * and add it to the system time so that time still moves forwards.
     *
     * It will take a cluster restart to clear this offset.
     */
    private long m_backwardsTimeAdjustmentOffset = 0;

    private final long BACKWARD_TIME_FORGIVENESS_WINDOW_MS = VoltDB.BACKWARD_TIME_FORGIVENESS_WINDOW_MS;

    public interface Clock {
        long get();
        void sleep(long millis) throws InterruptedException;
    }

    private final Clock m_clock;

    public UniqueIdGenerator(long partitionId, long timestampTestingSalt) {
        this(partitionId, timestampTestingSalt, new Clock() {
            @Override
            public long get() {
                return System.currentTimeMillis();
            }

            @Override
            public void sleep(long millis) throws InterruptedException {
                Thread.sleep(millis);
            }
        });
    }

    /**
     * Initialize the UniqueIdManager for this site
     * @param partitionId The partitionId of the site generating ids
     * @param timestampTestingSalt Value of the salt used to skew a clock in testing.
     */
    public UniqueIdGenerator(long partitionId, long timestampTestingSalt, Clock clock) {
        this.partitionId = partitionId;

        m_timestampTestingSalt = timestampTestingSalt;
        m_clock = clock;

        // warn if running with a simulated clock skew
        // this should only be used for testing
        if (m_timestampTestingSalt != 0) {
            VoltLogger log = new VoltLogger("HOST");
            log.warn(String.format("Partition (id=%d) running in test mode with non-zero timestamp testing value: %d",
                     partitionId, timestampTestingSalt));
        }
    }

    public void updateMostRecentlyGeneratedUniqueId(long uniqueId) {
        final long partitionId = UniqueIdGenerator.getPartitionIdFromUniqueId(uniqueId);
        if (this.partitionId != partitionId) {
            String msg =
                    "UniqueIdGenerator for partition " + this.partitionId +
                    " received a unique id from partition " + partitionId;
            throw new RuntimeException(msg);
        }
        lastUniqueId = Math.max(lastUniqueId, uniqueId);
        counterValue = UniqueIdGenerator.getSequenceNumberFromUniqueId(lastUniqueId);
        lastUsedTime = UniqueIdGenerator.getTimestampFromUniqueId(lastUniqueId);
    }

    /**
     * Generate a unique id that contains a timestamp, a counter
     * and a siteid packed into a 64-bit long value. Subsequent calls
     * to this method will return strictly larger long values.
     * @return The newly generated unique id.
     */
    public long getNextUniqueId() {
        // get the current time, usually the salt value is zero
        // in testing it is used to simulate clock skew
        long currentTime = m_clock.get() + m_timestampTestingSalt;
        long currentTimePlusOffset = currentTime + m_backwardsTimeAdjustmentOffset;
        if (currentTime == lastUsedTime) {
            // increment the counter for this millisecond
            counterValue++;

            // handle the case where we've run out of counter values
            // for this particular millisecond (feels unlikely)
            if (counterValue > COUNTER_MAX_VALUE) {
                // spin until the next millisecond
                while (currentTime == lastUsedTime)
                    currentTime = m_clock.get();
                // reset the counter and lastUsedTime for the new millisecond
                lastUsedTime = currentTime;
                counterValue = 0;
            }
        }
        else {
            // reset the counter and lastUsedTime for the new millisecond
            if (currentTime < lastUsedTime && currentTimePlusOffset < lastUsedTime) {
                /*
                 * Time moved backwards, if was less than three seconds, spin to let it catch up
                 * otherwise calculate an offset to add to the system clock in order to use it to
                 * continue moving forward
                 */
                VoltLogger log = new VoltLogger("HOST");
                double diffSeconds = (lastUsedTime - currentTime) / 1000.0;
                String msg = String.format("UniqueIdGenerator time moved backwards from: %d to %d, a difference of %.2f seconds.",
                        lastUsedTime, currentTime, diffSeconds);
                log.error(msg);
                System.err.println(msg);
                // if the diff is less than some specified amount of time, wait a bit
                if ((lastUsedTime - currentTime) < BACKWARD_TIME_FORGIVENESS_WINDOW_MS) {
                    log.info("This node will delay any stored procedures sent to it.");
                    log.info(String.format("This node will resume full operation in  %.2f seconds.", diffSeconds));

                    long count = BACKWARD_TIME_FORGIVENESS_WINDOW_MS;
                    // note, the loop should stop once lastUsedTime is PASSED, not current
                    while ((currentTime <= lastUsedTime) && (count-- > 0)) {
                        try {
                            m_clock.sleep(1);
                        } catch (InterruptedException e) {}
                        currentTime = m_clock.get();
                    }
                    // if the loop above ended because it ran too much, time is pretty darn wonky.
                    // Going to let it crash in this instance
                    if (count < 0) {
                        VoltDB.crashLocalVoltDB("VoltDB was unable to recover after the system time was externally negatively adusted. " +
                                "It is possible that there is a serious system time or NTP error. ", false, null);
                    }
                }
                // Calculate an offset that will keep time moving forward at the rate of the system clock
                // but at the current time, not the time the system clock shifted back too.
                else {
                    m_backwardsTimeAdjustmentOffset = lastUsedTime - currentTime + 1;
                    //Since we calculated a new offset, recalculate the current time + the offset
                    currentTimePlusOffset = currentTime + m_backwardsTimeAdjustmentOffset;
                    //Should satisfy this constraint now
                    assert(currentTimePlusOffset > lastUsedTime);
                    double offsetSeconds = m_backwardsTimeAdjustmentOffset / 1000.0;
                    msg = String.format(
                            "Continuing operation by adding an offset of %.2f to system time. " +
                            "This means the time and unique IDs provided by VoltProcedure " +
                            " (getUniqueId, getTransactionId, getTransactionTime) " +
                            "will not correctly reflect wall clock time as reported by the system clock." +
                            " For severe shifts you could see duplicate " +
                            "IDs or time moving backwards when the server is" +
                            " restarted causing the offset to be discarded.",
                            offsetSeconds);
                    log.error(msg);
                    System.err.println(msg);
                }
            } else if (currentTime > lastUsedTime && m_backwardsTimeAdjustmentOffset != 0) {
                //Actual wall clock time is correct, blast away the offset
                //and switch to current time
                m_backwardsTimeAdjustmentOffset = 0;
                currentTimePlusOffset = currentTime;
                VoltLogger log = new VoltLogger("HOST");
                log.error("Host clock seems to have adjusted again to make the offset unecessary");
                System.err.println("Host clock seems to have adjusted again to make the offset unecessary");
            }
            currentTime = currentTimePlusOffset;
            lastUsedTime = currentTime;
            counterValue = 0;
        }

        lastUniqueId = makeIdFromComponents(currentTime, counterValue, partitionId);

        return lastUniqueId;
    }

    public static long makeIdFromComponents(long ts, long seqNo, long partitionId) {
        // compute the time in millis since VOLT_EPOCH
        long uniqueId = ts - VOLT_EPOCH;
        // verify all fields are the right size
        assert(uniqueId <= TIMESTAMP_MAX_VALUE);
        assert(seqNo <= COUNTER_MAX_VALUE);
        assert(partitionId <= PARTITIONID_MAX_VALUE);

        // put this time value in the right offset
        uniqueId = uniqueId << (COUNTER_BITS + PARTITIONID_BITS);
        // add the counter value at the right offset
        uniqueId |= seqNo << PARTITIONID_BITS;
        // finally add the siteid at the end
        uniqueId |= partitionId;

        return uniqueId;
    }

    /**
     * Given a unique id, return the time of its creation
     * by examining the embedded timestamp.
     * @param uniqueId The unique id value to examine.
     * @return The Date object representing the time this unique
     * id was created.
     */
    public static Date getDateFromUniqueId(long uniqueId) {
        long time = uniqueId >> (COUNTER_BITS + PARTITIONID_BITS);
        time += VOLT_EPOCH;
        return new Date(time);
    }

    /**
     * Given a unique id, return the time of its creation
     * by examining the embedded timestamp.
     * @param uniqueId The unique id value to examine.
     * @return The integer representing the time this unique
     * id was created.
     */
    public static long getTimestampFromUniqueId(long uniqueId) {
        long time = uniqueId >> (COUNTER_BITS + PARTITIONID_BITS);
        time += VOLT_EPOCH;
        return time;
    }

    /**
     * Given a unique id, return the embedded site id.
     * @param uniqueId The unique id value to examine.
     * @return The site id embedded within the unique id.
     */
    public static long getPartitionIdFromUniqueId(long uniqueId) {
        return uniqueId & PARTITIONID_MAX_VALUE;
    }

    public static long getSequenceNumberFromUniqueId(long uniqueId) {
        long seq = uniqueId >> PARTITIONID_BITS;
        seq = seq & COUNTER_MAX_VALUE;
        return seq;
    }

    /**
     * Get the last unique id generated.
     * @return The last unique id generated.
     */
    public long getLastUniqueId() {
        return lastUniqueId;
    }

    public long getLastUsedTime() {
        return lastUsedTime;
    }

    /**
     * Get a string representation of the uniqueId
     */
    public static String toString(long uniqueId) {
        final StringBuilder sb = new StringBuilder(128);
        sb.append("UniqueId: ").append(uniqueId);
        sb.append(" Timestamp: ").append(getTimestampFromUniqueId(uniqueId));
        sb.append(".").append(getSequenceNumberFromUniqueId(uniqueId));
        sb.append(" PartitionId: ").append(getPartitionIdFromUniqueId(uniqueId));
        sb.append(" Date: ").append(getDateFromUniqueId(uniqueId));
        return sb.toString();
    }

    public static String toBitString(long uniqueId) {
        String retval = "";
        long mask = 0x8000000000000000L;
        for(int i = 0; i < 64; i++) {
            if ((uniqueId & mask) == 0) retval += "0";
            else retval += "1";
            mask >>>= 1;
        }
        return retval;
    }
}
