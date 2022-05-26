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
package org.voltdb.importclient.kafka.util;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.importer.CommitTracker;


/**
 * Track the committed data and the offset for a topic and partition.
 *
 */
final public class DurableTracker implements CommitTracker {

    private final int m_gapFullWait = Integer.getInteger("KAFKA_IMPORT_GAP_WAIT", 2_000);
    private static final VoltLogger LOGGER = new VoltLogger("KAFKAIMPORTER");
    private static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;

    //The safe offset to be sent acknowledgement to Kafka
    long safeOffset = 0;

    //The maximal offset among the offsets of messages which have been successfully committed to VoltDB.
    long submittedOffset = -1L;

    //The offset when the number of committed offsets is more than the capacity of committedOffsets
    long offerOffset = -1L;

    long firstOffset = -1L;

    //Used to record the committed offsets
    final long [] committedOffsets;
    final String topic;
    final int partition;
    final String consumerGroup;
    private boolean firstOffsetCommitted = false;
    public DurableTracker(int leeway, String topic, int partition) {
        if (leeway <= 0) {
            throw new IllegalArgumentException("leeways is zero or negative");
        }
        committedOffsets = new long[leeway];
        this.topic = topic;
        this.partition = partition;
        this.consumerGroup = "";
    }

    public DurableTracker(int leeway, String topic, int partition, String consumerGroup) {
        if (leeway <= 0) {
            throw new IllegalArgumentException("leeways is zero or negative");
        }
        committedOffsets = new long[leeway];
        this.topic = topic;
        this.partition = partition;
        this.consumerGroup = consumerGroup;
    }


     //submit an offset while consuming a message and record the maximal submitted offset
    @Override
    public synchronized void submit(long offset) {
        if (submittedOffset == -1L && offset >= 0) {
            committedOffsets[idx(offset)] = safeOffset = submittedOffset = offset;
        }
        if (firstOffset == -1L) {
            firstOffset = offset;
        }
        if ((offset - safeOffset) >= committedOffsets.length) {
            offerOffset = offset;
            try {
                wait(m_gapFullWait);
            } catch (InterruptedException e) {
                LOGGER.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS,
                        Level.WARN, e, "CommitTracker wait was interrupted for group " + consumerGroup + " topic " + topic + " partition " + partition);
            }
        }
        if (offset > submittedOffset) {
            submittedOffset = offset;
        }
    }

    private final int idx(long offset) {
        return (int)(offset % committedOffsets.length);
    }

    @Override
    public synchronized void resetTo(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        committedOffsets[idx(offset)] = submittedOffset = safeOffset = offset;
        offerOffset = -1L;
    }

    //This is invoked in a stored procedure callback, indicating the message has been successfully persisted to
    //VoltDB. It will be recorded in committedOffsets and calculate the offset-safeOffset, which is safe to commit to Kafka
    @Override
    public synchronized long commit(long offset) {
        if (offset <= submittedOffset && offset > safeOffset) {
            int ggap = (int)Math.min(committedOffsets.length, offset-safeOffset);
            if (ggap == committedOffsets.length) {
                LOGGER.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS ,Level.WARN,
                        null, "CommitTracker moving topic commit point from %d to %d for topic "
                                + topic + " partition " + partition + " group:" + consumerGroup, safeOffset, (offset - committedOffsets.length + 1)
                        );
                safeOffset = offset - committedOffsets.length + 1;
                committedOffsets[idx(safeOffset)] = safeOffset;
            }
            committedOffsets[idx(offset)] = offset;
            while (ggap > 0 && committedOffsets[idx(safeOffset)]+1 == committedOffsets[idx(safeOffset+1)]) {
                ++safeOffset;
            }
            if (offerOffset >=0 && (offerOffset-safeOffset) < committedOffsets.length) {
                offerOffset = -1L;
                notify();
            }
        }

        if (offset == firstOffset) {
            firstOffsetCommitted = true;
        }
        return safeOffset;
    }

    @Override
    public long getSafe() {
        //an edge case that the very first offset is not committed but is assumed to be safe
        //no offset is safe for commit.
        if (safeOffset == firstOffset && !firstOffsetCommitted) {
            return -1;
        }

        return safeOffset;
    }
}
