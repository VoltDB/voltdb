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

    long c = 0, s = -1L, offer = -1L, firstOffset = -1L;
    final long [] lag;
    final String topic;
    final int partition;
    final String consumerGroup;
    private boolean firstOffsetCommitted = false;
    public DurableTracker(int leeway, String topic, int partition) {
        if (leeway <= 0) {
            throw new IllegalArgumentException("leeways is zero or negative");
        }
        lag = new long[leeway];
        this.topic = topic;
        this.partition = partition;
        this.consumerGroup = "";
    }

    public DurableTracker(int leeway, String topic, int partition, String consumerGroup) {
        if (leeway <= 0) {
            throw new IllegalArgumentException("leeways is zero or negative");
        }
        lag = new long[leeway];
        this.topic = topic;
        this.partition = partition;
        this.consumerGroup = consumerGroup;
    }

    @Override
    public synchronized void submit(long offset) {
        if (s == -1L && offset >= 0) {
            lag[idx(offset)] = c = s = offset;
        }
        if (firstOffset == -1L) {
            firstOffset = offset;
        }
        if ((offset - c) >= lag.length) {
            offer = offset;
            try {
                wait(m_gapFullWait);
            } catch (InterruptedException e) {
                LOGGER.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS,
                        Level.WARN, e, "CommitTracker wait was interrupted for group " + consumerGroup + " topic " + topic + " partition " + partition);
            }
        }
        if (offset > s) {
            s = offset;
        }
    }

    private final int idx(long offset) {
        return (int)(offset % lag.length);
    }

    @Override
    public synchronized void resetTo(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        lag[idx(offset)] = s = c = offset;
        offer = -1L;
    }

    @Override
    public synchronized long commit(long offset) {
        if (offset <= s && offset > c) {
            int ggap = (int)Math.min(lag.length, offset-c);
            if (ggap == lag.length) {
                LOGGER.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS ,Level.WARN,
                        null, "CommitTracker moving topic commit point from %d to %d for topic "
                                + topic + " partition " + partition + " group:" + consumerGroup, c, (offset - lag.length + 1)
                        );
                c = offset - lag.length + 1;
                lag[idx(c)] = c;
            }
            lag[idx(offset)] = offset;
            while (ggap > 0 && lag[idx(c)]+1 == lag[idx(c+1)]) {
                ++c;
            }
            if (offer >=0 && (offer-c) < lag.length) {
                offer = -1L;
                notify();
            }
        }

        if (offset == firstOffset) {
            firstOffsetCommitted = true;
        }
        return c;
    }

    @Override
    public long getSafe() {
        //an edge case that the very first offset is not committed but is assumed to be safe
        //no offset is safe for commit.
        if (c == firstOffset && !firstOffsetCommitted) {
            return -1;
        }

        return c;
    }
}
