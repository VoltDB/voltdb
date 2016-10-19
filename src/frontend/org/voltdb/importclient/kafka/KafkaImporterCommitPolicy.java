/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.importclient.kafka;

import org.apache.log4j.Logger;

/**
 * KafkaImporterCommitPolicy determines how offsets are committed to kafka.
 * @author akhanzode
 */
public enum KafkaImporterCommitPolicy {

        TIME,
        NONE;

        private static final Logger m_logger = Logger.getLogger("IMPORT");
        public static final KafkaImporterCommitPolicy fromString(String policy) {
            if (policy == null) return NONE;
            if (policy.endsWith("ms")) return TIME;
            return NONE;
        };
        public static final long fromStringTriggerValue(String policy, KafkaImporterCommitPolicy prop) {
            try {
                switch (prop) {
                    case TIME:
                        return policy == null ? 0 : Long.parseLong(policy.replaceAll("ms", ""));
                }
            } catch (NumberFormatException nfex) {
                m_logger.warn("Failed to parse commit policy: " + policy, nfex);
            }
            return 0;
        };

}
