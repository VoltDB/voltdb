/* This file is part of VoltDB.
 * Copyright (C) 2020-2022 Volt Active Data Inc.
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

package org.voltdb.e3.topics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.compiler.deploymentfile.PropertiesType;
import org.voltdb.compiler.deploymentfile.PropertyType;
import org.voltdb.utils.PBDUtils;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Class to take in topics configuration parameters specified in deployment file and get it to a form easily usable in
 * the topics subsystem.
 */
public class TopicsConfiguration extends TypedPropertiesBase<TopicsConfiguration.Entry<?>> {

    // TODO make this configurable, but the places which use this currently do not have an instance of this class
    // 10 MB is the default which kafka has
    public static final int MAX_BATCH_SIZE = 10 * 1024 * 1024;

    /**
     * Defines all TopicsConfiguration parameters, with their types and default values.
     */
    public static class Entry<T> extends TypedPropertiesBase.KeyBase<T> {

        private static final Map<String, Entry<?>> s_validConfigs = new HashMap<>();

        /* Other configuration parameters to consider in the future:
         * max.connections
         * max.connections.per.ip
         * message.max.bytes
         * offsets.retention.minutes
         * offsets.retention.check.interval.ms
         * offset.metadata.max.bytes
         */
        public static final Entry<Integer> NETWORK_THREAD_COUNT = new Entry<>("network.thread.count", Integer.class, 20);
        public static final Entry<String> CLUSTER_ID = new Entry<>("cluster.id", String.class);
        public static final Entry<Integer> COMPOUND_PROCEDURE_TIMEOUT = new Entry<>("producer.compound.procedure.timeout.ms",
                Integer.class, BatchTimeoutOverrideType.NO_TIMEOUT);
        public static final Entry<Integer> GROUP_MAX_SESSION_TIMEOUT = new Entry<>("group.max.session.timeout.ms", Integer.class, 1_800_000);
        public static final Entry<Integer> GROUP_MIN_SESSION_TIMEOUT = new Entry<>("group.min.session.timeout.ms", Integer.class, 6000);
        public static final Entry<Integer> GROUP_MAX_SIZE = new Entry<>("group.max.size", Integer.class, Integer.MAX_VALUE);
        public static final Entry<Integer> GROUP_INITIAL_REBALANCE_DELAY_MS = new Entry<>("group.initial.rebalance.delay.ms", Integer.class, 3000);
        public static final Entry<Integer> GROUP_MAX_LOAD_SIZE = new Entry<>("group.load.max.size", Integer.class,
                32 * 1024 * 1024);
        public static final Entry<Integer> OFFSET_RETENTION_CHECK = new Entry<>("offsets.retention.check.interval.ms",
                Integer.class, 600_000);
        public static final Entry<Integer> OFFSET_RETENTION = new Entry<>("offsets.retention.minutes", Integer.class,
                10080 /* 7 days */);
        public static final Entry<Integer> RETENTION_POLICY_THREADS = new Entry<Integer>("retention.policy.threads",
                Integer.class, 2, numThreads -> {
                    if (numThreads == null || numThreads < 1 || numThreads > 20) {
                        throw new IllegalArgumentException("Number of PBD retention threads must be between 1 and 20");
                    }
                });
        public static final Entry<Integer> QUOTA_MAX_THROTTLE_MS = new Entry<>("quota.throttle.max_ms", Integer.class,
                1000, Entry::mustBePositive);
        public static final Entry<Long> QUOTA_REQUEST_BPS = new Entry<>("quota.request.bytes_per_second", Long.class,
                -1L, Entry::mustBePositive);
        public static final Entry<Long> QUOTA_RESPONSE_BPS = new Entry<>("quota.response.bytes_per_second", Long.class,
                -1L, Entry::mustBePositive);
        public static final Entry<Integer> QUOTA_REQUEST_PROCESS_PCT = new Entry<>("quota.request.processing_percent",
                Integer.class, -1, Entry::mustBePositive);
        public static final Entry<Integer> COMPACTION_THREADS = new Entry<>("log.cleaner.threads", Integer.class, 1,
                Entry::mustBePositive);
        public static final Entry<Long> COMPACTION_DELETE_RETENTION_MS = new Entry<>("log.cleaner.delete.retention.ms",
                Long.class, 86_400_000L, Entry::mustBePositive);
        public static final Entry<Long> COMPACTION_BUFFER_SIZE = new Entry<>("log.cleaner.dedupe.buffer.size",
                Long.class, 134_217_728L, Entry::mustBePositive);

        public static final Entry<Long> TOPIC_SEGMENT_ROLL_TIME = new SegmentRollTimeEntry();

        private Entry(String propertyName, Class<T> clazz) {
            this(propertyName, clazz, null);
        }

        private Entry(String propertyName, Class<T> clazz, T defValue) {
            this(propertyName, clazz, defValue, null);
        }

        private Entry(String propertyName, Class<T> clazz, T defValue, Consumer<? super T> validator) {
            super(propertyName, clazz, defValue, validator);
            Entry<?> prev = s_validConfigs.put(propertyName, this);
            assert prev == null;
        }
    }

    /**
     * Uses {@link PBDUtils#parseTimeValue(String)} to parse the time configuration value
     */
    private static class SegmentRollTimeEntry extends Entry<Long> {
        SegmentRollTimeEntry() {
            super("log.roll.time", null, TimeUnit.DAYS.toNanos(7));
        }

        @Override
        protected Long parseValue(String strValue) {
            TopicRetention retention = TopicRetention.parse(strValue);
            return TimeUnit.MILLISECONDS.toNanos(retention.getEffectiveLimit());
        }
    }

    public TopicsConfiguration(PropertiesType input) {
        super(input == null ? ImmutableMap.of()
                : input.getProperty().stream()
                        .collect(Collectors.toMap(PropertyType::getName, PropertyType::getValue)));
    }

    public <T> T getConfigValue(Entry<T> entry) {
        return super.getProperty(entry);
    }

    @Override
    protected Map<String, Entry<?>> getValidKeys() {
        return Entry.s_validConfigs;
    }
}
