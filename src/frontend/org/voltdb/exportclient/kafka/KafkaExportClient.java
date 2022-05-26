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

package org.voltdb.exportclient.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportClientLogger;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportDecoderBase.BinaryEncoding;
import org.voltdb.exportclient.ExportRow;
import org.voltdb.exportclient.decode.AvroDecoder;
import org.voltdb.exportclient.decode.CSVStringDecoder;
import org.voltdb.serdes.EncodeFormat;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

import io.confluent.kafka.serializers.KafkaAvroSerializer;


public class KafkaExportClient extends ExportClientBase {

    private final static String DEFAULT_CLIENT_ID = "voltdb";
    private final static String TIMEZONE_PN = "timezone";
    private final static String SKIP_INTERNALS_PN = "skipinternals";
    private final static String BINARY_ENCODING_PN = "binaryencoding";
    private final static String BATCH_MODE_PN = "batch.mode";
    private final static String TOPIC_KEY_PN = "topic.key";
    private final static String TOPIC_PREFIX_PN = "topic.prefix";
    private final static String PARTITION_KEY_PN = "partition.key";
    private final static String BROKER_LIST_PN = "metadata.broker.list";
    private final static String OLD_SERIALIZER = "serializer.class";
    private final static String OLD_PARTITIONER = "partitioner.class";
    private final static String ACKS_TIMEOUT = "acks.retry.timeout";
    private final static String LEGACY_ACKS = "request.required.acks";
    public final static String ENCODE_FORMAT = "type";

    static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    private final static String MAX_BLOCK_MS_DEFAULT = "60000";

    private final static Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
    private final static Splitter PERIOD_SPLITTER = Splitter.on(".").omitEmptyStrings().trimResults();

    private final static int SHUTDOWN_TIMEOUT_MS = 10_000;
    public final static String DEFAULT_EXPORT_PREFIX = "voltdbexport";

    private static final ExportClientLogger LOG = new ExportClientLogger();

    private EncodeFormat m_encodeFormat = EncodeFormat.CSV;

    Properties m_producerConfig;
    String m_topicPrefix = DEFAULT_EXPORT_PREFIX;
    Map<String, String> m_tableTopics;

    //Keep this default to false as people out there might depend on index in csv
    boolean m_skipInternals = false;
    TimeZone m_timeZone = VoltDB.REAL_DEFAULT_TIMEZONE;
    BinaryEncoding m_binaryEncoding = BinaryEncoding.HEX;
    Map<String, String> m_tablePartitionColumns;
    boolean m_pollFutures = false;
    int m_acksTimeout = 5_000;

    @Override
    public void configure(Properties config) throws Exception {
        String decodeType = config.getProperty(ENCODE_FORMAT, "").trim();
        if (decodeType.toLowerCase().equals("avro")) {
            m_encodeFormat = EncodeFormat.AVRO;
        } else {
            m_encodeFormat = EncodeFormat.CSV;
        }

        m_producerConfig = new Properties();
        m_producerConfig.putAll(config);

        m_producerConfig.remove(ExportDataProcessor.EXPORT_TO_TYPE);
        m_producerConfig.remove(BATCH_MODE_PN);
        m_producerConfig.remove(OLD_SERIALIZER);
        m_producerConfig.remove(OLD_PARTITIONER);
        m_producerConfig.remove(ENCODE_FORMAT);

        m_timeZone = VoltDB.GMT_TIMEZONE;
        String timeZoneID = config.getProperty(TIMEZONE_PN, "").trim();
        if (!timeZoneID.isEmpty()) {
            m_timeZone = TimeZone.getTimeZone(timeZoneID);
        }
        m_producerConfig.remove(TIMEZONE_PN);

        String skipVal = config.getProperty(SKIP_INTERNALS_PN, "").trim();
        if (!skipVal.isEmpty()) {
            m_skipInternals = Boolean.parseBoolean(skipVal);
        }
        m_producerConfig.remove(SKIP_INTERNALS_PN);

        String encodingVal = config.getProperty(BINARY_ENCODING_PN, "").trim().toUpperCase();
        if (!encodingVal.isEmpty()) {
            m_binaryEncoding = BinaryEncoding.valueOf(encodingVal);
        }
        m_producerConfig.remove(BINARY_ENCODING_PN);

        ImmutableMap.Builder<String, String> mbld = ImmutableMap.builder();
        String topicVal = config.getProperty(TOPIC_KEY_PN, "");
        for (String stanza: COMMA_SPLITTER.split(topicVal)) {
            List<String> pair = PERIOD_SPLITTER.splitToList(stanza);
            if (pair.size() != 2) {
                throw new IllegalArgumentException(
                        "Malformed value \"" + topicVal
                      + "\" for property " + TOPIC_KEY_PN
                        );
            }
            mbld.put(pair.get(0).toLowerCase(), pair.get(1));
        }
        try {
            m_tableTopics = mbld.build();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Repetitions found in \"" + topicVal
                  + "\" for property " + TOPIC_KEY_PN, e
                    );
        }
        m_producerConfig.remove(TOPIC_KEY_PN);

        String prefixVal = config.getProperty(TOPIC_PREFIX_PN);
        if (prefixVal != null) {
            m_topicPrefix = prefixVal.trim();
        }
        m_producerConfig.remove(TOPIC_PREFIX_PN);

        mbld = ImmutableMap.builder();
        String partitionKeyVal = config.getProperty(PARTITION_KEY_PN, "");
        for (String stanza: COMMA_SPLITTER.split(partitionKeyVal)) {
            List<String> pair = PERIOD_SPLITTER.splitToList(stanza);
            if (pair.size() != 2) {
                throw new IllegalArgumentException(
                        "Malformed value \"" + partitionKeyVal
                      + "\" for property " + PARTITION_KEY_PN
                        );
            }
            mbld.put(pair.get(0).toLowerCase(), pair.get(1));
        }
        try {
            m_tablePartitionColumns = mbld.build();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Repetitions found in \"" + partitionKeyVal
                  + "\" for property " + PARTITION_KEY_PN, e
                    );
        }
        m_producerConfig.remove(PARTITION_KEY_PN);

        String idVal = config.getProperty(ProducerConfig.CLIENT_ID_CONFIG, "").trim();
        if (idVal.isEmpty()) {
            // If no client id was provided, set it to a default which will be
            // replaced by a unique value in the decoder
            m_producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, DEFAULT_CLIENT_ID);
        }

        String acksVal = config.getProperty(ProducerConfig.ACKS_CONFIG, "").trim();
        if (acksVal.isEmpty()) {
            m_producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        }
        m_pollFutures = !"0".equals(m_producerConfig.get(ProducerConfig.ACKS_CONFIG));

        if (!m_pollFutures) {
            //If old producer property is seen use it to ack futures.
            String legacyAck = config.getProperty(LEGACY_ACKS, "false");
            m_pollFutures = Boolean.getBoolean(legacyAck);
        }
        String retries = "4";
        try {
            retries = config.getProperty(ProducerConfig.RETRIES_CONFIG, retries);
            if (Integer.parseInt(retries) < 0) {
                throw new IllegalArgumentException(
                        "\"" + ProducerConfig.RETRIES_CONFIG + "\" must be >= 0"
                        );
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "\"" + ProducerConfig.RETRIES_CONFIG + "\" must be an integer", e
                    );
        }
        m_producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, retries);

        String buffSize = "2097152";
        try {
            buffSize = config.getProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, buffSize);
            if (Long.parseLong(buffSize) <= 0) {
                throw new IllegalArgumentException(
                        "\"" + ProducerConfig.BUFFER_MEMORY_CONFIG + "\" must be > 0"
                        );
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "\"" + ProducerConfig.BUFFER_MEMORY_CONFIG + "\" must be a long", e
                    );
        }
        m_producerConfig.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, buffSize);

        String batchSize = "1024";
        try {
            batchSize = config.getProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            if (Integer.parseInt(batchSize) <= 0) {
                throw new IllegalArgumentException(
                        "\"" + ProducerConfig.BATCH_SIZE_CONFIG + "\" must be >= 0"
                        );
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "\"" + ProducerConfig.BATCH_SIZE_CONFIG + "\" must be an integer", e
                    );
        }
        m_producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);

        String kSerializer = config.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "").trim();
        if (kSerializer.isEmpty()) {
            m_producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } else try {
            Class.forName(kSerializer);
        } catch (UnknownError|ExceptionInInitializerError|ClassNotFoundException e) {
            throw new IllegalArgumentException("Unable to load serializer class " + kSerializer , e);
        }

        if (m_encodeFormat == EncodeFormat.AVRO) {
            m_producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

            String schemaRegistryUrl = config.getProperty(SCHEMA_REGISTRY_URL_CONFIG, "").trim();
            if (StringUtil.isEmpty(schemaRegistryUrl)) {
                throw new IllegalArgumentException("Property \"schema.registry.url\" cannot be empty.");
            }

            m_producerConfig.setProperty(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        } else {
            String vSerializer = config.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "").trim();
            if (vSerializer.isEmpty()) {
                m_producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            } else try {
                Class.forName(vSerializer);
            } catch (UnknownError|ExceptionInInitializerError|ClassNotFoundException e) {
                throw new IllegalArgumentException("Unable to load serializer class " + vSerializer , e);
            }
        }

        String bootstrapVal = config.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "").trim();
        if (bootstrapVal.isEmpty()) {
            String brokersVal = config.getProperty(BROKER_LIST_PN,"").trim();
            if (brokersVal.isEmpty()) {
                throw new IllegalArgumentException(
                        "Required property " + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG +
                        " is undefined"
                        );
            }
            bootstrapVal = brokersVal;
            m_producerConfig.remove(BROKER_LIST_PN);
            m_producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapVal);
        }

        m_producerConfig.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS_DEFAULT);

        LOG.info("Configuring Kafka export client: %s", m_producerConfig);
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new KafkaExportDecoder(source);
    }

    class KafkaExportDecoder extends ExportDecoderBase {

        String m_topic = null;
        Properties m_decoderProducerConfig;
        KafkaProducer<String, Object> m_producer;
        Map<String, AvroDecoder> m_tableAvroDecoderMap;
        CSVStringDecoder m_csvDecoder;
        final List<Future<RecordMetadata>> m_futures = new ArrayList<>();
        private final AtomicBoolean m_failure = new AtomicBoolean(false);
        final ListeningExecutorService m_es;
        private volatile boolean m_primed = false;
        private volatile boolean m_paused = false;;

        public KafkaExportDecoder(AdvertisedDataSource source) {
            super(source);

            if (m_encodeFormat == EncodeFormat.AVRO) {
                m_tableAvroDecoderMap = new ConcurrentHashMap<>();
            } else {
                CSVStringDecoder.Builder builder = CSVStringDecoder.builder();
                builder
                        .dateFormatter(Constants.ODBC_DATE_FORMAT_STRING)
                        .timeZone(m_timeZone)
                        .binaryEncoding(m_binaryEncoding)
                        .skipInternalFields(m_skipInternals)
                ;
                m_csvDecoder = builder.build();
            }

            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                m_es = CoreUtils.getListeningSingleThreadExecutor(
                        "Kafka Export decoder for partition " +
                                source.tableName + " - " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }

            // Ensure each decoder uses its own properties (ENG-17657)
            m_decoderProducerConfig = new Properties();
            m_decoderProducerConfig.putAll(m_producerConfig);
        }

        @Override
        public synchronized void pause() {
            m_paused = true;
            if (m_producer != null) {
                m_producer.close(Duration.ofMillis(0));
            }
            m_primed = false;
        }

        @Override
        public synchronized void resume() {
            m_paused = false;
        }

        final synchronized void checkOnFirstRow() throws RestartBlockException {
            if (m_paused) {
                throw new RestartBlockException("Exporter has been paused", false);
            }
            if (!m_primed) try {
                setClientId();
                m_producer = new KafkaProducer<>(m_decoderProducerConfig);
            }
            catch (ConfigException e) {
                LOG.error("Unable to instantiate a Kafka producer", e);
                throw new RestartBlockException("Unable to instantiate a Kafka producer", e, true);
            } catch (KafkaException e) {
                LOG.error("Unable to instantiate a Kafka producer", e);
                throw new RestartBlockException("Unable to instantiate a Kafka producer", e, true);
            }
            m_primed = true;
        }

        private void setClientId() {
            String clientId = m_decoderProducerConfig.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
            if (DEFAULT_CLIENT_ID.equals(clientId)) {
                // Generate a unique kafka client id
                clientId = "producer-" + m_source.tableName + "-" + m_source.partitionId;
                m_decoderProducerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            }
        }

        private void populateTopic(String tableName) {
            if (m_tableTopics != null && m_tableTopics.containsKey(tableName.toLowerCase())) {
                m_topic = m_tableTopics.get(tableName.toLowerCase()).intern();
            }
            else {
                m_topic = new StringBuilder(m_topicPrefix).append(tableName).toString().intern();
            }
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

        @Override
        public void onBlockCompletion(ExportRow row) throws RestartBlockException {
            try {
                if (m_pollFutures || m_failure.get()) {
                    m_producer.flush();
                    ImmutableList<Future<RecordMetadata>> pollFutures = ImmutableList.copyOf(m_futures);
                    for (Future<RecordMetadata> fut: pollFutures) {
                        fut.get(1, TimeUnit.MILLISECONDS); // flush call above ensures that the send calls completed.
                    }
                }
            } catch (TimeoutException e) {
                throw new RestartBlockException("Send operation timed out", e, true);
            } catch (ExecutionException e) {
                LOG.warn("Send operation failed to complete", e.getCause());
                throw new RestartBlockException("Send operation failed to complete", e.getCause(), true);
            } catch (InterruptedException e) {
                LOG.warn("Iterrupted send operation", e);
                throw new RestartBlockException("Iterrupted send operation", e, true);
            } finally {
                m_futures.clear();
                m_failure.set(false);
            }
        }

        @Override
        public void onBlockStart(ExportRow row) throws RestartBlockException {
            checkOnFirstRow();
            if (m_topic == null) populateTopic(row.tableName);
        }

        @Override
        public boolean processRow(ExportRow rd) throws RestartBlockException {
            checkOnFirstRow();
            //Use partition value by default if its null use partition id.
            //partition value will be null only if partition column is overridden table.column and is nullable
            String pval = (rd.partitionValue == null) ? String.valueOf(rd.partitionId) : rd.partitionValue.toString();
            ProducerRecord<String, Object> krec;
            if (m_encodeFormat == EncodeFormat.AVRO) {
                AvroDecoder decoder = m_tableAvroDecoderMap.computeIfAbsent(rd.tableName, k -> new AvroDecoder.Builder().build());
                GenericRecord avroRecord = decoder.decode(rd.generation, rd.tableName, rd.types, rd.names, null, rd.values);
                krec = new ProducerRecord<>(m_topic, pval, avroRecord);
            } else {
                String decoded = m_csvDecoder.decode(rd.generation, rd.tableName, rd.types, rd.names, null, rd.values);
                krec = new ProducerRecord<>(m_topic, pval, decoded);
            }

            try {
                m_futures.add(m_producer.send(krec, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e != null){
                            LOG.warn("Failed to send data. Verify if the kafka server matches bootstrap.servers %s", e,
                                    m_decoderProducerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
                            m_failure.compareAndSet(false, true);
                        }
                    }
                }));
            } catch (KafkaException e) {
                LOG.warn("Unable to send %s", e, krec);
                throw new RestartBlockException("Unable to send message", e, true);
            } catch(IllegalStateException e) { // thrown if a catalog update closes the producer through pause()
                throw new RestartBlockException("IllegalStateException, possibly because kafka producer was closed", e, false);
            }
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            if (m_producer != null) {
                try {
                    m_producer.close(Duration.ofMillis(0));
                } catch (Exception ignoreIt) {
                    LOG.debug("Unexpected error trying to close KafkaProducer", ignoreIt);
                }
            }
            if (m_es != null) {
                m_es.shutdown();
                try {
                    if (!m_es.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        // In case we were misconfigured to a non-existent Kafka broker,
                        // a decoder thread may be stuck on 'send' and we need to force it out.
                        // Note that the 'send' does not seem to heed the 'max.block.ms' timeout.
                        forceExecutorShutdown();
                    }
                } catch (InterruptedException e) {
                    // We are in the UAC path and don't want to throw exception for this condition;
                    // just force the shutdown.
                    LOG.warn("Interrupted while awaiting executor shutdown on source:" + m_source);
                    forceExecutorShutdown();
                }
            }
        }

        private void forceExecutorShutdown() {
            if (m_es == null) {
                return;
            }

            LOG.warn("Forcing executor shutdown on source: " + m_source);
            try {
                m_es.shutdownNow();
            } catch (Exception e) {
                LOG.error("Failed to force executor shutdown on source: " + m_source, e);
            }
        }
    }
}
