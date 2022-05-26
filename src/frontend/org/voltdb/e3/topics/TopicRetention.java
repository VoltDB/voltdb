/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.catalog.Topic;

import com.google.common.collect.ImmutableMap;

/**
 * A class holding a topic retention.
 * <p>
 * Instances of this class can only created in a controlled way by parsing
 * a String or converting from Catalog. Both instantiation scenarios validate
 * the inputs and the internal state of the resulting instance is guaranteed valid.
 * <p>
 * The intent is to parse the retention specification only once and store the parsed
 * retention in the {@link org.voldb.catalog.Topic} catalog instance.
 */
public class TopicRetention {

    public static enum Policy {
        SIZE,
        TIME,
        COMPACT
    }

    private final Policy m_policy;
    private final int m_limit;
    private final String m_unit;

    private static final Pattern PAT_PARSE_RETENTION = Pattern.compile(
            "(?i)" +                                // (ignore case)
            "\\A"  +                                // start statement
            "\\s*(?<limit>\\d+)"  +                 // limit
            "\\s*(?<unit>[a-z]{2})"  +              // 2-char unit
            "\\s*\\z"                               // end
            );

    private static long s_minBytesLimitMb = 64;

    // A map of time configuration qualifiers to millisecond value
    private static final Map<String, Long> s_timeLimitConverter;
    static {
        ImmutableMap.Builder<String, Long>bldr = ImmutableMap.builder();
        bldr.put("ss", 1000L);
        bldr.put("mn", 60_000L);
        bldr.put("hr", 60L * 60_000L);
        bldr.put("dy", 24L * 60L * 60_000L);
        bldr.put("wk", 7L * 24L * 60L * 60_000L);
        bldr.put("mo", 30L * 24L * 60L * 60_000L);
        bldr.put("yr", 365L * 24L * 60L * 60_000L);
        s_timeLimitConverter = bldr.build();
    }

    private static final Map<String, String> s_timeDisplay;
    static {
        ImmutableMap.Builder<String, String>bldr = ImmutableMap.builder();
        bldr.put("ss", "second");
        bldr.put("mn", "minute");
        bldr.put("hr", "hour");
        bldr.put("dy", "day");
        bldr.put("wk", "week");
        bldr.put("mo", "month");
        bldr.put("yr", "year");
        s_timeDisplay = bldr.build();
    }

    // A map of byte configuration qualifiers to bytes value
    private static final Map<String, Long> s_byteLimitConverter;
    static {
        ImmutableMap.Builder<String, Long>bldr = ImmutableMap.builder();
        bldr.put("mb", 1024L * 1024L);
        bldr.put("gb", 1024L * 1024L * 1024L);
        s_byteLimitConverter = bldr.build();
    }

    private static final Map<String, String> s_byteDisplay;
    static {
        ImmutableMap.Builder<String, String>bldr = ImmutableMap.builder();
        bldr.put("mb", "megabyte");
        bldr.put("gb", "gigabyte");
        s_byteDisplay = bldr.build();
    }

    /**
     * Parse a retention string into a {@link TopicRetention} instance.
     * <p>
     * This is the only way to instantiate {@link TopicRetention}.
     *
     * @param retentionString
     * @return a {@link TopicRetention} instance
     * @throws IllegalArgumentException
     */
    public static TopicRetention parse(String retentionString) throws IllegalArgumentException {
        if (StringUtils.isBlank(retentionString)) {
            return new TopicRetention();
        }

        // Detect compact
        retentionString = retentionString.trim().toLowerCase();
        boolean compact = retentionString.startsWith("compact");
        if (compact) {
            retentionString = retentionString.replace("compact", "");
        }

        Matcher matcher = PAT_PARSE_RETENTION.matcher(retentionString);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "retention=\"%s\" is not a valid topic retention specification", retentionString));
        }

        int limit = Integer.parseInt(matcher.group("limit"));
        String unit = matcher.group("unit").toLowerCase();

        Policy policy = null;
        if (s_byteLimitConverter.get(unit) != null) {
            if (compact) {
                throw new IllegalArgumentException(String.format(
                        "\"%s\" is not a valid unit for compact topic retention specification: valid units are time units = %s",
                        unit, s_timeLimitConverter.keySet()));
            }
            policy = Policy.SIZE;
        }
        else if (s_timeLimitConverter.get(unit) != null) {
            policy = compact ? Policy.COMPACT : Policy.TIME;
        }
        else {
            throw new IllegalArgumentException(String.format(
                    "\"%s\" is not a valid unit for topic retention specification: valid units for time = %s, for size = %s",
                    unit, s_timeLimitConverter.keySet(), s_byteLimitConverter.keySet()));
        }
        return new TopicRetention(policy, limit, unit);
    }

    /**
     * Parse a retention from a {@link org.voldb.catalog.Topic} catalog instance
     * <p>
     * The catalog instance should be valid except if hacked.
     *
     * @param topic the {@link org.voldb.catalog.Topic} to initialize from
     */
    public static TopicRetention parse(Topic topic) throws IllegalArgumentException {
        try {
            // Policy from ordinal, ArrayOutOfBoundsException if hacked
            Policy policy = Policy.values()[topic.getRetentionpolicy()];
            return new TopicRetention(policy,
                    topic.getRetentionlimit(), topic.getRetentionunit());
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse retention from catalog topic " + topic.getTypeName());
        }
    }

    /**
     * Default constructor initializes to default retention of "time 7 dy"
     */
    public TopicRetention() {
        this(Policy.TIME, 7, "dy");
    }

    /**
     * Constructor for non-default retention, checking limit and unit specification
     * <p>
     * Note: package-private for JUnit tests
     *
     * @param policy
     * @param limit
     * @param unit
     */
    TopicRetention(Policy policy, int limit, String unit) throws IllegalArgumentException {
        m_policy = policy;
        m_unit = unit;
        m_limit = limit;

        // Check limit
        switch(policy) {
        case TIME:
        case COMPACT:
            Long timeFactor = s_timeLimitConverter.get(unit);
            if (timeFactor == null) {
                throw new IllegalArgumentException(String.format(
                        "\"%s\" is not a valid time unit for topic retention specification", unit));
            }
            break;
        case SIZE:
        default:
            Long sizeFactor = s_byteLimitConverter.get(unit);
            if (sizeFactor == null) {
                throw new IllegalArgumentException(String.format(
                        "\"%s\" is not a valid size unit for topic retention specification", unit));
            }
            long minLimit = s_minBytesLimitMb * s_byteLimitConverter.get("mb");
            if (getEffectiveLimit() < minLimit) {
                throw new IllegalArgumentException("Size-based topic retention limit must be >= " + s_minBytesLimitMb + " mb");
            }
            break;
        }
    }

    /**
     * @return the {@link Policy} of the retention
     */
    public Policy getPolicy() {
        return m_policy;
    }

    /**
     * @return the effective limit of the retention, i.e. bytes for size or milliseconds for time
     */
    public long getEffectiveLimit() {
        switch(m_policy) {
        case TIME:
        case COMPACT:
            return m_limit * s_timeLimitConverter.get(m_unit);
        case SIZE:
        default:
            return m_limit * s_byteLimitConverter.get(m_unit);
        }
    }

    /**
     * Set retention to a {@link org.voldb.catalog.Topic} catalog instance
     *
     * @param topic
     */
    public void toTopic(Topic topic) {
        topic.setRetentionpolicy(m_policy.ordinal());
        topic.setRetentionlimit(m_limit);
        topic.setRetentionunit(m_unit);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (m_policy == Policy.COMPACT) {
            sb.append(m_policy.name().toLowerCase()).append(" ");
        }
        sb.append(m_limit).append(" ");
        if (m_policy == Policy.SIZE) {
            sb.append(s_byteDisplay.get(m_unit));
        }
        else {
            sb.append(s_timeDisplay.get(m_unit));
        }
        if (m_limit != 1) {
            sb.append('s');
        }
        return sb.toString();
    }
}
