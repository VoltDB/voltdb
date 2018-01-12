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

    long c = 0, s = -1L, offer = -1L;
    final long [] lag;
    final String topic;
    final int partition;

    public DurableTracker(int leeway, String topic, int partition) {
        if (leeway <= 0) {
            throw new IllegalArgumentException("leeways is zero or negative");
        }
        lag = new long[leeway];
        this.topic = topic;
        this.partition = partition;
    }

    @Override
    public synchronized void submit(long offset) {
        if (s == -1L && offset >= 0) {
            lag[idx(offset)] = c = s = offset;
        }
        if ((offset - c) >= lag.length) {
            offer = offset;
            try {
                wait(m_gapFullWait);
            } catch (InterruptedException e) {
                LOGGER.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS,
                        Level.WARN, e, "Gap tracker wait was interrupted for topic " + topic + " partition " + partition);
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
                        null, "Gap tracker moving topic commit point from %d to %d for topic "
                                + topic + " partition " + partition, c, (offset - lag.length + 1)
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
        return c;
    }
}
