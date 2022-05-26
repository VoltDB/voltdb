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

import org.apache.log4j.Logger;
import org.voltcore.utils.EstTime;

/**
 * KafkaCommitPolicy determines how offsets are committed to kafka.
 * @author akhanzode
 */
public enum KafkaCommitPolicy {

    TIME,
    NONE;

    private static final Logger m_logger = Logger.getLogger("IMPORT");
    public static final KafkaCommitPolicy fromString(String policy) {
        if (policy == null) return NONE;
        if (policy.toUpperCase().equals("NONE")) return NONE;

        try {
            // If it ends in "ms", or we can parse it as a number, it's a time value.
            if (policy.endsWith("ms")) return TIME;
            Long.parseLong(policy);
            return TIME;
        }
        catch (Exception e) {
            // It's not a number, ignore.
        }
        return NONE;
    };
    public static final long fromStringTriggerValue(String policy, KafkaCommitPolicy prop) {
        try {
            switch (prop) {
            case TIME:
                return policy == null ? 0 : Long.parseLong(policy.replaceAll("ms", ""));
            default:
                break;
            }
        } catch (NumberFormatException nfex) {
            m_logger.warn("Failed to parse commit policy: " + policy, nfex);
        }
        return 0;
    }

    public static boolean shouldCommit(KafkaCommitPolicy policy, long triggerValue, long lastCommitted) {
        switch(policy) {
        case TIME:
            return (EstTime.currentTimeMillis() > (lastCommitted + triggerValue));
        default:
            break;
        }
        return true;
    }
}
