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
package org.voltdb.importclient.kafka;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.importclient.kafka.util.HostAndPort;
import org.voltdb.importclient.kafka.util.KafkaUtils;
import org.voltdb.importclient.kafka.util.ProcedureInvocationCallback;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.ImporterLogger;
import org.voltdb.importer.formatter.FormatterBuilder;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.CSVTupleDataLoader;
import org.voltdb.utils.RowWithMetaData;

/**
 * Import Kafka data into the database, using a remote Volt client and manual offset management.
 *
 * @author jcrump
 * @since 7.5
 */
public class KafkaExternalLoader implements ImporterLifecycle, ImporterLogger {

    private static final VoltLogger m_log = new VoltLogger("KAFKA-EXTERNAL-LOADER");
    private static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;

    private final static AtomicLong m_failedCount = new AtomicLong(0);

    private final KafkaExternalLoaderCLIArguments m_args;
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private ExecutorService m_executorService = null;
    private ExecutorService m_callbackExecutor = null;

    public KafkaExternalLoader(KafkaExternalLoaderCLIArguments args) {
        m_args = args;
    }

    public void initialize() throws Exception {

        // Check for transitive runtime dependencies that Kafka will complain about if they're missing.
        try {
            KafkaExternalLoader.class.getClassLoader().loadClass("org.I0Itec.zkclient.IZkStateListener");
            KafkaExternalLoader.class.getClassLoader().loadClass("org.apache.zookeeper.Watcher");
        }
        catch (ClassNotFoundException cnfex) {
            throw new RuntimeException("Cannot find the Zookeeper client libraries, zkclient-0.3.jar and zookeeper-3.3.4.jar. Use the ZKLIB environment variable to specify the path to the Zookeeper jars files.");
        }

        // If we need to prompt the user for a VoltDB password, do so.
        m_args.password = CLIConfig.readPasswordIfNeeded(m_args.user, m_args.password, "Enter password: ");

        // Create connection
        final ClientConfig c_config = new ClientConfig(m_args.user, m_args.password, null);
        if (m_args.ssl != null && !m_args.ssl.trim().isEmpty()) {
            c_config.setTrustStoreConfigFromPropertyFile(m_args.ssl);
            c_config.enableSSL();
        }

        // Set procedure call timeout to forever:
        c_config.setProcedureCallTimeout(0);

        // Get the Volt host:port strings from the config, handling deprecated and default values.
        List<String> hostPorts = m_args.getVoltHosts();
        m_client = getVoltClient(c_config, hostPorts);

        KafkaBulkLoaderCallback kafkaBulkLoaderCallback = new KafkaBulkLoaderCallback();

        if (m_args.useSuppliedProcedure) {
            // Create an executor on which to run the success callback. For the direct-to-table bulk case, that loader already has an executor.
            String procName = (m_args.useSuppliedProcedure ? m_args.procedure : m_args.table) + "-callbackproc";
            m_callbackExecutor = CoreUtils.getSingleThreadExecutor(procName + "-" + Thread.currentThread().getName());
            m_loader = new CSVTupleDataLoader((ClientImpl) m_client, m_args.procedure, kafkaBulkLoaderCallback, m_callbackExecutor, kafkaBulkLoaderCallback);
        }
        else {
            m_loader = new CSVBulkDataLoader((ClientImpl) m_client, m_args.table, m_args.batch, m_args.update, kafkaBulkLoaderCallback, kafkaBulkLoaderCallback);
        }

        m_loader.setFlushInterval(m_args.flush, m_args.flush);
    }

    /*
     * Construct the infrastructure and start processing messages from Kafka
     */
    private void processKafkaMessages() throws Exception {

        try {
            m_executorService = createImporterExecutor(this, this);

            if (m_args.useSuppliedProcedure) {
                m_log.info("Kafka Consumer from topic: " + m_args.topic + " Started using procedure: " + m_args.procedure);
            }
            else {
                m_log.info("Kafka Consumer from topic: " + m_args.topic + " Started for table: " + m_args.table);
            }

            // Wait forever, per http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/package-summary.htm
            m_executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (Throwable terminate) {
            m_log.error("Error in Kafka Consumer", terminate);
        }
        finally {
            close();
        }
        System.exit(-1);
    }

    /*
     * Error callback from the CSV loader in case of trouble writing to Volt.
     */
    private class KafkaBulkLoaderCallback implements BulkLoaderErrorHandler, BulkLoaderSuccessCallback {

        @Override
        public void success(Object rowHandle, ClientResponse response) {
            RowWithMetaData metaData = (RowWithMetaData) rowHandle;
            try {
                metaData.procedureCallback.clientCallback(response);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean handleError(RowWithMetaData metaData, ClientResponse response, String error) {
            if (m_args.maxerrors <= 0) return false;
            if (response != null) {
                byte status = response.getStatus();
                if (status != ClientResponse.SUCCESS) {
                    m_log.error("Failed to Insert Row: error=" + error + " response=" + ((ClientResponseImpl)response).toJSONString() + " data=" + metaData.rawLine);
                    long failCount = m_failedCount.incrementAndGet();
                    // If we've reached our max-error threshold, quit. Use a different error message for the various cases for
                    // troubleshooting purposes.
                    String errMsg = null;
                    if (m_args.maxerrors > 0 && failCount > m_args.maxerrors) {
                        errMsg = "Max error count reached, exiting.";
                    }
                    else if (status != ClientResponse.USER_ABORT && status != ClientResponse.GRACEFUL_FAILURE) {
                        errMsg = "Error response received from database, exiting; status=" + Byte.toString(status);
                    }
                    if (errMsg != null) {
                        m_log.error(errMsg);
                        try {
                            closeExecutors();
                        }
                        catch (InterruptedException e) {
                            // Ignore
                        }
                        return true;
                    }
                }
                else {
                    if (metaData.procedureCallback != null) {
                        try {
                            metaData.procedureCallback.clientCallback(response);
                        }
                        catch (Exception e) {
                            // Failure in processing success callback, ignore.
                        }
                    }
                }
            }
            return false;
        }

        @Override
        public boolean hasReachedErrorLimit() {
            long fc = m_failedCount.get();
            return (m_args.maxerrors > 0 && fc > m_args.maxerrors);
        }
    }


    //
    // ImporterSupport implementation
    //

    private volatile boolean m_stopping = false;

    @Override
    public boolean shouldRun() {
        return !m_stopping;
    }

    @Override
    public void stop() {
        m_stopping = true;
    }

    @Override
    public void rateLimitedLog(Level level, Throwable cause, String format, Object... args) {
        m_log.rateLimitedLog(LOG_SUPPRESSION_INTERVAL_SECONDS, level, cause, format, args);
    }

    @Override
    public void info(Throwable t, String msgFormat, Object... args) {
        m_log.info(String.format(msgFormat, args), t);
    }

    @Override
    public void warn(Throwable t, String msgFormat, Object... args) {
        m_log.warn(String.format(msgFormat, args), t);
    }

    @Override
    public void error(Throwable t, String msgFormat, Object... args) {
        m_log.error(String.format(msgFormat, args), t);
    }

    @Override
    public void debug(Throwable t, String msgFormat, Object... args) {
        m_log.debug(String.format(msgFormat, args), t);
    }

    @Override
    public boolean isDebugEnabled() {
        return m_log.isDebugEnabled();
    }

    @Override
    public boolean hasTransaction() {
        return true;
    }

    /*
     * Create an executor with one importer per thread (per partition).
     */
    private ExecutorService createImporterExecutor(final ImporterLifecycle lifecycle, final ImporterLogger logger) throws Exception {

        Map<URI, KafkaStreamImporterConfig> configs = createKafkaImporterConfigFromProperties(m_args);
        ExecutorService executor = Executors.newFixedThreadPool(configs.size());
        m_log.info("Created " + configs.size() + " configurations for partitions:");

        for (URI uri : configs.keySet()) {
            m_log.info(" " + uri);
            KafkaStreamImporterConfig cfg = configs.get(uri);
            LoaderTopicPartitionImporter importer = new LoaderTopicPartitionImporter(cfg, lifecycle, logger);
            executor.submit(importer);
        }

        return executor;
    }

    /*
     * Create an importer config for each partition.
     */
    private Map<URI, KafkaStreamImporterConfig> createKafkaImporterConfigFromProperties(KafkaExternalLoaderCLIArguments properties) throws Exception {

        // If user supplied the 'zookeeper' argument, get the list of brokers from Zookeeper. Otherwise, use the list of brokers
        // that they supplied with the 'brokers' argument.  CLI argument validation ensures that one or the other, but not both, are supplied.
        List<HostAndPort> brokerList;
        String brokerListString;

        if (!properties.zookeeper.trim().isEmpty()) {
            brokerList = KafkaUtils.getBrokersFromZookeeper(properties.zookeeper, properties.zookeeperSessionTimeoutMillis);
            brokerListString = StringUtils.join(brokerList.stream().map(s -> s.getHost() + ":" + s.getPort()).collect(Collectors.toList()), ",");
        }
        else {
            brokerListString = properties.brokers.trim();
            brokerList = Arrays.stream(brokerListString.split(",")).map(s -> HostAndPort.fromString(s)).collect(Collectors.toList());
        }

        // Derive the key from the list of broker URIs:
        String brokerKey = KafkaUtils.getNormalizedKey(brokerListString);

        // Create the input formatter:
        FormatterBuilder builder = FormatterBuilder.createFormatterBuilder(properties.formatterProperties);

        return KafkaStreamImporterConfig.getConfigsForPartitions(brokerKey, brokerList, properties.topic, properties.groupid,
                                                properties.procedure, properties.timeout, properties.buffersize, properties.commitpolicy, builder);
    }

    /*
     * Create a Volt client from the supplied configuration and list of servers.
     */
    private static Client getVoltClient(ClientConfig config, List<String> hosts) throws Exception {
        final Client client = ClientFactory.createClient(config);
        for (String host : hosts) {
            client.createConnection(host);
        }
        return client;
    }

    /*
     * Shut down, close connections, and clean up.
     */
    private void closeExecutors() throws InterruptedException {
        stop();
        if (m_executorService != null) {
            m_executorService.shutdownNow();
            m_executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            m_executorService = null;
        }
        if (m_callbackExecutor != null) {
            m_callbackExecutor.shutdownNow();
            m_callbackExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            m_callbackExecutor = null;
        }
    }

    private void close() {
        try {
            closeExecutors();
            if (m_loader != null) {
                m_loader.close();
                m_loader = null;
            }
            if (m_client != null) {
                m_client.close();
                m_client = null;
            }
        }
        catch (Exception ex) {
            // Ignore
        }
    }

    /*
     * Extend the base partition importer to use a remote volt client (wrapped in a CSV loader) to write to the database
     */
    private class LoaderTopicPartitionImporter extends BaseKafkaTopicPartitionImporter implements Runnable {

        public LoaderTopicPartitionImporter(KafkaStreamImporterConfig config, ImporterLifecycle lifecycle, ImporterLogger logger) {
            super(config, lifecycle, logger);
        }

        @Override
        public boolean invoke(Object[] params, ProcedureInvocationCallback cb) {
           try {
               m_loader.insertRow(new RowWithMetaData(StringUtils.join(params, ","), cb.getOffset(), cb), params);
           }
           catch (Exception e) {
               m_log.error("Exception from loader while inserting row", e);
               return false;
           }
           return true;
        }

        @Override
        public void run() {
            accept();
        }
    }

    public static void main(String[] args) {

        System.out.println("Support for Kafka version 0.8 has been deprecated. The default kafkaloader is Kafka version 0.10.");
        final KafkaExternalLoaderCLIArguments cfg = new KafkaExternalLoaderCLIArguments();
        cfg.parse(KafkaExternalLoader.class.getName(), args);

        KafkaExternalLoader kloader = new KafkaExternalLoader(cfg);
        try {
            kloader.initialize();
            kloader.processKafkaMessages();
        }
        catch (Exception e) {
            kloader.close();
            m_log.error("Exception in KafkaExternalLoader", e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
