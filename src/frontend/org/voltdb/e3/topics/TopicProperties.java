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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Property;
import org.voltdb.common.Constants;
import org.voltdb.serdes.AvroSerde.Configuration.GeographyPointSerialization;
import org.voltdb.serdes.AvroSerde.Configuration.GeographySerialization;
import org.voltdb.serdes.AvroSerde.Configuration.TimestampPrecision;
import org.voltdb.serdes.EncodeFormat;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.Iterables;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

/**
 * Properties which can be configured on a topic in the topic DDL
 */
public class TopicProperties extends TypedPropertiesBase<TopicProperties.Key<?>> {
    public static ImmutableList<String> ALL_COLUMNS = ImmutableList.of("*");
    public static TopicProperties defaults() {
        return new TopicProperties(ImmutableMap.of());
    }

    public TopicProperties(CatalogMap<Property> properties) {
        super(StreamSupport.stream(properties.spliterator(), false)
                .collect(Collectors.toMap(Property::getTypeName, Property::getValue, (k, v) -> {
                    throw new IllegalStateException("Duplicate key " + k);
                }, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))));
    }

    public TopicProperties(Map<String, String> properties) {
        super(ImmutableSortedMap.<String, String>orderedBy(String.CASE_INSENSITIVE_ORDER).putAll(properties).build());
    }

    public <T> T get(Key<T> key) {
        return super.getProperty(key);
    }

    @Override
    protected Map<String, Key<?>> getValidKeys() {
        return Key.s_keys;
    }

    /**
     * @return the {@link EncodeFormat} to use for producer values or {@link EncodeFormat#UNDEFINED}
     */
    public EncodeFormat getProducerValueFormat() {
        return firstValidFormat(
                Key.PRODUCER_FORMAT_VALUE,
                Key.TOPIC_FORMAT);
    }

    /**
     * @return the {@link EncodeFormat} to use for consumer keys or {@link EncodeFormat#UNDEFINED}
     */
    public EncodeFormat getConsumerKeyFormat() {
        return firstValidFormat(
                Key.CONSUMER_FORMAT_KEY,
                Key.CONSUMER_FORMAT,
                Key.TOPIC_FORMAT);
    }

    /**
     * @return the {@link EncodeFormat} to use for consumer values or {@link EncodeFormat#UNDEFINED}
     */
    public EncodeFormat getConsumerValueFormat() {
        return firstValidFormat(
                Key.CONSUMER_FORMAT_VALUE,
                Key.CONSUMER_FORMAT,
                Key.TOPIC_FORMAT);
    }

    /**
     * Look through values associated with {@code keys} and return the first value which is not
     * {@link EncodeFormat#UNDEFINED} or return {@link EncodeFormat#UNDEFINED} if they are all that value.
     *
     * @param keys       ordered from most specific key to least
     * @return configured {@link EncodeFormat} or {@link EncodeFormat#UNDEFINED}
     */
    @SafeVarargs
    private final EncodeFormat firstValidFormat(TopicProperties.Key<EncodeFormat>... keys) {
        EncodeFormat format = EncodeFormat.UNDEFINED;
        for (TopicProperties.Key<EncodeFormat> key : keys) {
            format = get(key);
            if (format != EncodeFormat.UNDEFINED) {
                break;
            }
        }
        return format;
    }

    /**
     * Keys used in instances of {@link TopicProperties}
     *
     * @param <T> Type of value associated with the key
     */
    public static class Key<T> extends TypedPropertiesBase.KeyBase<T> {
        private static final Map<String, Key<?>> s_keys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        // Properties for specifying record encoding.
        // Note producer.format.value is not supported so related properties are not implemented

        // Historical: the following properties should not be documented. They are capturing
        // XML or DDL configuration clauses which were declared late in the project, with too
        // much existing code depending on them being properties and not catalog attributes.
        public static final Key<EncodeFormat> TOPIC_FORMAT = new FormatKey("topic.format");
        public static final Key<List<String>> CONSUMER_KEY = new ColumnsKey("consumer.key", ImmutableList.of());
        public static final Key<List<String>> CONSUMER_VALUE = new ColumnsKey("consumer.value", ALL_COLUMNS);
        // Historical end.

        // Formats which just apply to producer records
        public static final Key<EncodeFormat> PRODUCER_FORMAT_VALUE = new FormatKey("producer.format.value");

        // Formats which just apply to consumer records
        public static final Key<EncodeFormat> CONSUMER_FORMAT = new FormatKey("consumer.format");
        public static final Key<EncodeFormat> CONSUMER_FORMAT_KEY = new FormatKey("consumer.format.key");
        public static final Key<EncodeFormat> CONSUMER_FORMAT_VALUE = new FormatKey("consumer.format.value");

        public static final Key<Boolean> OPAQUE_PARTITIONED = new TopicProperties.BooleanKey(
                "opaque.partitioned", false);

        public static final Key<Boolean> PRODUCER_PARAMETERS_INCLUDE_KEY = new BooleanKey(
                "producer.parameters.includeKey", Boolean.FALSE);

        public static final Key<Boolean> CONSUMER_SKIP_INTERNALS = new BooleanKey("consumer.skip.internals", Boolean.TRUE);

        // Mutable property allowing skipping over consumer errors
        public static final Key<Boolean> CONSUMER_SKIP_ERRORS = new BooleanKey("consumer.skip.errors", Boolean.FALSE);

        // IMMUTABLE: If true the records are encoded to the desired format in the transaction path (a.k.a. inline encoding),
        // otherwise they are encoded and stored in the export format, and are encoded to the desired format when replying
        // to fetch requests from consumers
        public static final Key<Boolean> TOPIC_STORE_ENCODED = new BooleanKey("topic.store.encoded",
                false, Boolean.FALSE);

        public static final Key<Boolean> TOPIC_TRACE_SUBSCRIBER_SESSIONS = new BooleanKey("topic.trace.subscriber.sessions", Boolean.FALSE);

        // CSV properties
        public static final Key<Character> CONFIG_CSV_SEPARATOR = new CharKey(
                "config.csv.separator", CSVParser.DEFAULT_SEPARATOR);
        public static final Key<Character> CONFIG_CSV_QUOTE = new CharKey(
                "config.csv.quote", CSVParser.DEFAULT_QUOTE_CHARACTER);
        public static final Key<Character> CONFIG_CSV_ESCAPE = new CharKey(
                "config.csv.escape", CSVParser.DEFAULT_ESCAPE_CHARACTER);
        public static final Key<String> CONFIG_CSV_NULL = new TopicProperties.Key<String>(
                "config.csv.null", String.class, Constants.CSV_NULL, null);
        public static final Key<Boolean> CONFIG_CSV_STRICT_QUOTES = new TopicProperties.BooleanKey(
                "config.csv.strictQuotes", CSVParser.DEFAULT_STRICT_QUOTES);
        public static final Key<Boolean> CONFIG_CSV_IGNORE_LEADING_WHITESPACE = new TopicProperties.BooleanKey(
                "config.csv.ignoreLeadingWhitespace", CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE);
        public static final Key<Boolean> CONFIG_CSV_QUOTE_ALL = new TopicProperties.BooleanKey(
                "config.csv.quoteAll", false);

        // JSON properties
        public static final Key<JsonSchemaPolicy> CONFIG_JSON_SCHEMA = new TopicProperties.EnumKey<>(
                "config.json.schema", JsonSchemaPolicy.NONE);

        public static final Key<List<String>> CONFIG_JSON_PRODUCER_ATTRIBUTES = new JsonAttributesKey(
                "config.json.producer.attributes", ImmutableList.of());

        public static final Key<List<String>> CONFIG_JSON_CONSUMER_ATTRIBUTES = new JsonAttributesKey(
                "config.json.consumer.attributes", ImmutableList.of());

        // Avro properties
        public static final Key<TimestampPrecision> CONFIG_AVRO_TIMESTAMP = new TopicProperties.EnumKey<>(
                "config.avro.timestamp", TimestampPrecision.MICROSECONDS);
        public static final Key<GeographyPointSerialization> CONFIG_AVRO_GEOGRAPHY_POINT = new TopicProperties.EnumKey<>(
                "config.avro.geographyPoint", GeographyPointSerialization.FIXED_BINARY);
        public static final Key<GeographySerialization> CONFIG_AVRO_GEOGRAPHY = new TopicProperties.EnumKey<>(
                "config.avro.geography", GeographySerialization.BINARY);

        // Mutable properties governing fault injection - must be enabled by FAULT_INJECTION_ENABLED
        public static final Key<Boolean> FAULT_INJECTION_ENABLED = new BooleanKey("fault.injection.enabled", Boolean.FALSE);
        public static final Key<List<Long>> FAULT_INJECTION_ERROR_OFFSETS = new LongsKey("fault.injection.error.offsets", null);

        // Properties are mutable by default
        public Key(String name, Class<T> clazz, T defValue, Consumer<? super T> validator) {
            this(name, clazz, true, defValue, validator);
        }

        public Key(String name, Class<T> clazz, boolean mutable, T defValue, Consumer<? super T> validator) {
            super(name, clazz, mutable, defValue, validator);
            Key<?> prev = s_keys.put(name, this);
            assert prev == null : "Key already exists: " + name;
        }
    }

    public static class EnumKey<E extends Enum<E>> extends Key<E> {
        public EnumKey(String name, E defValue) {
            this(name, defValue, null);
        }

        public EnumKey(String name, E defValue, Consumer<? super E> validator) {
            super(name, defValue.getDeclaringClass(), defValue, validator);
        }

        @Override
        protected E parseValue(String strValue) {
            try {
                return Enum.valueOf(getValueClass(), strValue.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        strValue + ". Valid values: " + Joiner.on(", ").join(getValueClass().getEnumConstants()));
            }
        }
    }

    /**
     * Key for a {@link Character} property
     */
    static final class CharKey extends TopicProperties.Key<Character> {
        public CharKey(String name, Character defValue) {
            super(name, Character.class, defValue, null);
        }

        @Override
        protected Character parseValue(String strValue) {
            if (strValue.length() != 1) {
                throw new IllegalArgumentException("Value must be a single character: '" + strValue + "'");
            }

            char value = strValue.charAt(0);
            if (value != '\t' && (value < ' ' || value > '~')) {
                throw new IllegalArgumentException("Value must be a printable ascii character: '" + strValue + "'");
            }

            return value;
        }

    }
    /**
     * Key for a property with a {@link EncodeFormat} for a value
     */
    private static class FormatKey extends Key<EncodeFormat> {
        FormatKey(String name) {
            super(name, EncodeFormat.class, EncodeFormat.UNDEFINED, null);
        }

        @Override
        protected EncodeFormat parseValue(String strValue) {
            Set<EncodeFormat> validValues = EncodeFormat.complexFormats();
            EncodeFormat format;
            try {
                format = EncodeFormat.valueOf(strValue.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "'" + strValue + "' is not a valid format. Valid formats: " + validValues);
            }

            if (!validValues.contains(format)) {
                throw new IllegalArgumentException(
                        "'" + strValue + "' is not a valid format. Valid formats: " + validValues);
            }

            return format;
        }
    }

    /**
     * Key for a property with a {@link Boolean} for a value
     */
    public static class BooleanKey extends Key<Boolean> {
        public BooleanKey(String name, Boolean defValue) {
            this(name, true, defValue);
        }

        public BooleanKey(String name, boolean mutable, Boolean defValue) {
            super(name, Boolean.class, mutable, defValue, null);
        }

        @Override
        protected Boolean parseValue(String strValue) {
            return Boolean.valueOf(strValue);
        }
    }

    /**
     * Key for a property with a simple CSV of column names for a value: columns are converted to uppercase
     */
    private static class ColumnsKey extends Key<List<String>> {
        private static final Splitter s_commaSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

        ColumnsKey(String name, List<String> defValue) {
            super(name, null, defValue, null);
        }

        @Override
        protected List<String> parseValue(String strValue) {
            return ImmutableList.copyOf(Iterables.transform(s_commaSplitter.split(strValue), String::toUpperCase));
        }
    }

    /**
     * Key for a property a CSV list of JSON attributes
     */
    private static class JsonAttributesKey extends Key<List<String>> {
        private CSVParser m_parser = new CSVParser();

        JsonAttributesKey(String name, List<String> defValue) {
            super(name, null, defValue, null);
        }

        @Override
        protected List<String> parseValue(String strValue) {
            ImmutableList.Builder<String> bldr = ImmutableList.builder();
            try {
                List<Object> values = m_parser.parseLineList(strValue);
                if (values != null) {
                    for (Object value : values) {
                        if (value == null) {
                            throw new RuntimeException("null JSON key");
                        }
                        else if (value instanceof String) {
                            bldr.add((String) value);
                        }
                        else {
                            throw new RuntimeException("JSON key must be a string");
                        }
                    }
                }
            }
            catch (Exception ex) {
                throw new IllegalArgumentException(ex);
            }
            return bldr.build();
        }
    }

    /**
     * Key for a list of Long
     */
    private static class LongsKey extends Key<List<Long>> {
        LongsKey(String name, List<Long> defValue) {
            super(name, null, defValue, null);
        }

        @Override
        protected List<Long> parseValue(String strValue) {
            return ImmutableList.copyOf(Iterables.transform(CatalogUtil.splitOnCommas(strValue), Long::valueOf));
        }
    }
}
