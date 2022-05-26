/* This file is part of VoltDB.
 * Copyright (C) 2020-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package topictest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.voltdb.CLIConfig;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;

import com.google_voltpatches.common.util.concurrent.RateLimiter;

public class TopicTest {
  static final String TEST_TOPIC = "TEST_TOPIC";
  static class TopicTestConfig extends CLIConfig {

      @Option(desc = "Topic (default " + TEST_TOPIC + ").")
      String topic = TEST_TOPIC;

      @Option(desc = "Comma separated list of servers to connect to (using default volt port).")
      String servers = "localhost";

      @Option(desc="Topic port on servers (default 9095)")
      int topicPort = 9095;

      @Option(desc = "Topic is using producer.parameters.includeKey (default false).")
      boolean includekey = false;

      @Option(desc="Key parameter position 0 or 2, see procedure definitions in DDL (default 2)")
      int keyposition = 2;

      @Option(desc = "Use avro (requires confluent schema registry, default false")
      boolean useavro = false;

      @Option(desc = "Schema registry URL (default http://localhost:8081). MUST match the value in deployment file.")
      String schemaregistry = "http://localhost:8081";

      @Option(desc = "How many produce invocations, default 10.")
      long count = 10;

      @Option(desc = "How records per second to insert (default = 1, if 0 = no rate).")
      int insertrate = 1;

      @Option(desc = "Log progress every X rows (default 0, no logging).")
      int logprogress = 0;

      @Override
      public void validate() {
          if (StringUtils.isBlank(topic)) exitWithMessageAndUsage("topic must not be empty or blank");
          if (count < 0) exitWithMessageAndUsage("count must be >= 0");
          if (insertrate < 0) exitWithMessageAndUsage("insertrate must be >= 0");
          if (topicPort <= 0) exitWithMessageAndUsage("topicPort must be > 0");
          if (logprogress < 0) exitWithMessageAndUsage("pollprogress must be >= 0");
          if (keyposition != 0 && keyposition != 2) exitWithMessageAndUsage("keyposition must be 0 or 2");
      }
  }

  final TopicTestConfig m_config;
  final ArrayList<String> m_brokers = new ArrayList<>();
  Schema m_schema;


  final AtomicLong m_rowId = new AtomicLong(0);
  final AtomicLong m_successfulInserts = new AtomicLong(0);
  final AtomicLong m_failedInserts = new AtomicLong(0);


  /**
   * Clean way of exiting from an exception
   * @param message   Message to accompany the exception
   * @param e         The exception thrown
   */
  void exitWithException(String message, Exception e) {
      exitWithException(message, e, false);
  }

  void exitWithException(String message, Exception e, boolean stackTrace) {
      System.out.println(message);
      System.out.println(String.format("Exit with exception: %s", e));
      if (stackTrace) {
          e.printStackTrace();
      }
      System.exit(1);
  }

  /**
   * Creates a new instance of the test to be run.
   * @param args The arguments passed to the program
   */
  TopicTest(TopicTestConfig config) {
      this.m_config = config;
      // Build broker string
      String[] serverArray = config.servers.split(",");
      for (String server : serverArray) {
          if (!StringUtils.isBlank(server)) {
              String[] split = server.split(":");
              m_brokers.add(split[0] + ":" + config.topicPort);
          }
      }
    }

    void runTest() throws InterruptedException, IOException {
      System.out.println("Starting test using brokers: " + m_brokers);

      doInserts();
      System.out.println(String.format("Finished test: %d inserted, %d failed", m_successfulInserts.get(), m_failedInserts.get()));
    }

    void doInserts() {
        RateLimiter rateLimiter = m_config.insertrate > 0 ? RateLimiter.create(m_config.insertrate) : null;
        KafkaProducer<Long, Object> producer = null;
        try {
            Properties props = new Properties();

            // General producer properties
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, m_brokers);
            props.put(ProducerConfig.ACKS_CONFIG, "all");

            // Keys must be serialized as per the expected VoltDB type of the partitioning parameter
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongSerializer.class);
            if (m_config.useavro) {
                // AVRO serialization
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
                props.put("schema.registry.url", m_config.schemaregistry);
            }
            else {
                // CSV serialization == String serialization
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            }

            // Configure the producer to use VoltDBKafkaPartitioner
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());
            props.put(VoltDBKafkaPartitioner.BOOTSTRAP_SERVERS_VOLTDB, m_config.servers);

            // Create producer
            producer = new KafkaProducer<>(props);

            // Create AVRO schema to use in test
            if (m_config.useavro) {
                m_schema = getAvroSchema();
            }

            // Produce desired count of records, wait for all completions
            long produced = 0;
            AtomicLong completed = new AtomicLong(0);

            while(produced++ < m_config.count) {
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }

                long nextRowId = m_rowId.incrementAndGet();
                ProducerRecord<Long, Object> record = null;
                if (m_config.useavro) {
                    record = getAvroRecord(nextRowId);
                }
                else {
                    record = getCsvRecord(nextRowId);
                }
                producer.send(record,
                        new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        completed.incrementAndGet();
                        if(e != null) {
                            if (m_failedInserts.get() == 0) {
                                // Print only the first error
                                e.printStackTrace();
                            }
                            m_failedInserts.incrementAndGet();
                        }
                        else {
                            m_successfulInserts.incrementAndGet();
                        }
                    }
                });

                if (m_config.logprogress > 0 && (produced % m_config.logprogress == 0)) {
                    System.out.println(String.format("Produced %d records", produced));
                }
            }

            while(true) {
                long nowCompleted = completed.get();
                if (nowCompleted >= m_config.count) break;
                System.out.println(String.format("Completed %d of %d, waiting...", nowCompleted, m_config.count));
                Thread.sleep(1000);
            }
        }
        catch (Exception e) {
            exitWithException("Producer failed inserting into topic", e, true);
        }
        finally {
            if (producer != null) {
                producer.close();
            }
        }
    }

    ProducerRecord<Long, Object> getCsvRecord(long key) {
        String value;
        if (m_config.includekey) {
            switch (m_config.keyposition) {
            case 2:
                // includeKey=true, we omit parameter 2 from value contents (see DDL for procedure01)
                value = String.format("%d,%d-bbb,%d-ddd", key + 100, key + 100, key + 100);
                break;
            case 0:
                // includeKey=true, we omit parameter 0 from value contents (see DDL for procedure02)
                value = String.format("%d-bbb,%d,%d-ddd", key + 100, key + 100, key + 100);
                break;
            default:
                // Should have been rejected b4
                throw new IllegalArgumentException("Unsupported key position: " + m_config.keyposition);
            }
        }
        else {
            // includeKey=false, repeat key in value as parameter 2
            switch (m_config.keyposition) {
            case 2:
                // includeKey=false, we set parameter 2 == key value (see DDL for procedure01)
                value = String.format("%d,%d-bbb,%d,%d-ddd", key + 100, key + 100, key, key + 100);
                break;
            case 0:
                // includeKey=false, we set parameter 0 == key value (see DDL for procedure01)
                value = String.format("%d,%d-bbb,%d, %d-ddd", key, key + 100, key + 100, key + 100);
                break;
            default:
                // Should have been rejected b4
                throw new IllegalArgumentException("Unsupported key position: " + m_config.keyposition);
            }
        }
        return new ProducerRecord<Long, Object>(m_config.topic, key, value);
    }

    /**
     * Create the AVRO schema used by this test run: with includekey == true, the parameter at key position
     * (the partitioning parameter of the procedure) must be omitted from the schema.
     *
     * @return Avro {@link Schema} to use
     */
    Schema getAvroSchema() {
        FieldAssembler<Schema> schemaFields = SchemaBuilder.record("TOPIC_TEST_PRODUCER").namespace("").fields();

        if (!m_config.includekey || m_config.keyposition != 0) {
            // Add parameter at position 0
            schemaFields.name("A").type().longType().noDefault();
        }
        schemaFields.name("B").type().stringType().noDefault();

        if (!m_config.includekey || m_config.keyposition != 2) {
            // Add parameter at position 2
            schemaFields.name("C").type().longType().noDefault();
        }
        schemaFields.name("D").type().stringType().noDefault();

        Schema schema = schemaFields.endRecord();
        System.out.println(String.format("Using AVRO schema: %s", schema));
        return schema;
    }

    ProducerRecord<Long, Object> getAvroRecord(long key) {
        assert m_schema != null : "getAvroSchema must be called and initialize m_schema";
        GenericRecord record = new GenericData.Record(m_schema);

        if (!m_config.includekey || m_config.keyposition != 0) {
            // Add parameter at position 0 or a value that is distinct from the key
            record.put("A", m_config.keyposition == 0 ? key : key + 100);
        }
        record.put("B", String.format("%s-bbb", key + 100));

        if (!m_config.includekey || m_config.keyposition != 2) {
            // Add parameter at position 2 or a value that is distinct from the key
            record.put("C", m_config.keyposition == 2 ? key : key + 100);
        }
        record.put("D", String.format("%s-ddd", key + 100));

        return new ProducerRecord<Long, Object>(m_config.topic, key, record);
    }

    /**
     * Main routine creates a test instance and kicks off the runTest method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) {
        TopicTestConfig config = new TopicTestConfig();
        config.parse(TopicTest.class.getName(), args);

        try {
            TopicTest test = new TopicTest(config);
            test.runTest();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
