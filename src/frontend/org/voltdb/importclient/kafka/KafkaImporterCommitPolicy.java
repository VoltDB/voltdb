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

/**
 * KafkaImporterCommitPolicy determines how offsets are committed to kafka.
 * @author akhanzode
 */
public enum KafkaImporterCommitPolicy {

        COMMIT_POLICY_TIME,
        COMMIT_POLICY_BYTES,
        COMMIT_POLICY_TRANSACTION,
        COMMIT_POLICY_NONE;

        public static final KafkaImporterCommitPolicy fromString(String policy) {
            if (policy == null) return COMMIT_POLICY_NONE;
            if (policy.endsWith("ms")) return COMMIT_POLICY_TIME;
            if (policy.endsWith("t")) return COMMIT_POLICY_TRANSACTION;
            if (policy.endsWith("b")) return COMMIT_POLICY_BYTES;
            return COMMIT_POLICY_NONE;
        };
        public static final long fromStringTriggerValue(String policy, KafkaImporterCommitPolicy prop) {
            switch (prop) {
                case COMMIT_POLICY_TIME:
                    return Long.parseLong(policy.replaceAll("ms", ""));
                case COMMIT_POLICY_BYTES:
                    return Long.parseLong(policy.replaceAll("b", ""));
                case COMMIT_POLICY_TRANSACTION:
                    return Long.parseLong(policy.replaceAll("t", ""));
            }
            return 0;
        };

}
