/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.Watcher.Event.KeeperState;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderSuccessCallback;
import org.voltdb.importclient.kafka.KafkaStreamImporterConfig.HostAndPort;
import org.voltdb.importer.ImporterLifecycle;
import org.voltdb.importer.ImporterLogger;
import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.importer.formatter.FormatterBuilder;
import org.voltdb.importer.formatter.builtin.VoltCSVFormatterFactory;
import org.voltdb.utils.BulkLoaderErrorHandler;
import org.voltdb.utils.CSVBulkDataLoader;
import org.voltdb.utils.CSVDataLoader;
import org.voltdb.utils.CSVTupleDataLoader;
import org.voltdb.utils.JsonUtils;
import org.voltdb.utils.RowWithMetaData;

import kafka.cluster.Broker;

/**
 * Import Kafka data into the database, using a remote Volt client and manual offset management.
 *
 * @author jcrump
 * @since 7.4
 */
public class KafkaExternalLoader implements ImporterLifecycle, ImporterLogger {

    private static final VoltLogger m_log = new VoltLogger("KAFKA-EXTERNAL-LOADER");
    private static final int LOG_SUPPRESSION_INTERVAL_SECONDS = 60;
    private static final int ZK_CONNECTION_TIMEOUT_SECONDS = 10;
    private final static AtomicLong m_failedCount = new AtomicLong(0);

    private final KafkaExternalLoaderCLIArguments m_config;
    private CSVDataLoader m_loader = null;
    private Client m_client = null;
    private ExecutorService m_executorService = null;
    private ExecutorService m_callbackExecutor = null;

    public KafkaExternalLoader(KafkaExternalLoaderCLIArguments config) {
        m_config = config;
    }

    public void initialize() throws Exception {

        // Try and load classes we need and not packaged.
        try {
            KafkaExternalLoader.class.getClassLoader().loadClass("org.I0Itec.zkclient.IZkStateListener");
            KafkaExternalLoader.class.getClassLoader().loadClass("org.apache.zookeeper.Watcher");
        }
        catch (ClassNotFoundException cnfex) {
            throw new RuntimeException("Cannot find the Zookeeper client libraries, zkclient-0.3.jar and zookeeper-3.3.4.jar. Use the ZKLIB environment variable to specify the path to the Zookeeper jars files.");
        }

        // If we need to prompt the user for a VoltDB password, do so.
        m_config.password = CLIConfig.readPasswordIfNeeded(m_config.user, m_config.password, "Enter password: ");

        // Create connection
        final ClientConfig c_config = new ClientConfig(m_config.user, m_config.password, null);
        if (m_config.ssl != null && !m_config.ssl.trim().isEmpty()) {
            c_config.setTrustStoreConfigFromPropertyFile(m_config.ssl);
            c_config.enableSSL();
        }

        // Set procedure call timeout to forever:
        c_config.setProcedureCallTimeout(0);

        // Get the Volt host:port strings from the config, handling deprecated and default values.
        List<String> hostPorts = m_config.getVoltHosts();
        m_client = getVoltClient(c_config, hostPorts);

        KafkaBulkLoaderCallback kafkaBulkLoaderCallback = new KafkaBulkLoaderCallback();

        if (m_config.useSuppliedProcedure) {
            // Create an executor on which to run the success callback. For the direct-to-table bulk case, that loader already has an executor.
            String procName = (m_config.useSuppliedProcedure ? m_config.procedure : m_config.table) + "-callbackproc";
            m_callbackExecutor = CoreUtils.getSingleThreadExecutor(procName + "-" + Thread.currentThread().getName());
            m_loader = new CSVTupleDataLoader((ClientImpl) m_client, m_config.procedure, kafkaBulkLoaderCallback, m_callbackExecutor, kafkaBulkLoaderCallback);
        } else {
            m_loader = new CSVBulkDataLoader((ClientImpl) m_client, m_config.table, m_config.batchsize, m_config.update, kafkaBulkLoaderCallback, kafkaBulkLoaderCallback);
        }

        m_loader.setFlushInterval(m_config.flush, m_config.flush);
    }

    /*
     * Construct the infrastructure and start processing messages from Kafka
     */
    private void processKafkaMessages() throws Exception {

        try {
            m_executorService = createImporterExecutor(m_loader, this, this);

            if (m_config.useSuppliedProcedure) {
                m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started using procedure: " + m_config.procedure);
            }
            else {
                m_log.info("Kafka Consumer from topic: " + m_config.topic + " Started for table: " + m_config.table);
            }

            // Wait forever, per http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/package-summary.htm
            m_executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
        catch (Throwable terminate) {
            m_log.error("Error in Kafka Consumer", terminate);
            System.exit(-1);
        }
        finally {
            close();
        }
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
            if (m_config.maxerrors <= 0) return false;
            if (response != null) {
                byte status = response.getStatus();
                if (status != ClientResponse.SUCCESS) {
                    m_log.error("Failed to Insert Row: " + metaData.rawLine);
                    long fc = m_failedCount.incrementAndGet();
                    // If we've reached our max-error threshold, quit. Use a different error message for the various cases for
                    // troubleshooting purposes.
                    String errMsg = null;
                    if (m_config.maxerrors > 0 && fc > m_config.maxerrors) {
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
            return (m_config.maxerrors > 0 && fc > m_config.maxerrors);
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
    private ExecutorService createImporterExecutor(CSVDataLoader loader, final ImporterLifecycle lifecycle, final ImporterLogger logger) throws Exception {

        Map<URI, KafkaStreamImporterConfig> configs = createKafkaImporterConfigFromProperties(m_config);
        // Log the configuration in a nice readable format:
        configs.entrySet().stream().forEach(c -> m_log.info(JsonUtils.toJson(c)));

        ExecutorService executor = Executors.newFixedThreadPool(configs.size());
        m_log.warn("Created " + configs.size() + " configurations for partitions:");

        for (URI uri : configs.keySet()) {
            m_log.warn(" " + uri);
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
            brokerList = getBrokersFromZookeeper(properties.zookeeper);
            brokerListString = StringUtils.join(brokerList.stream().map(s -> s.getHost() + ":" + s.getPort()).collect(Collectors.toList()), ",");
        }
        else {
            brokerListString = properties.brokers.trim();
            brokerList = Arrays.stream(brokerListString.split(",")).map(s -> HostAndPort.fromString(s)).collect(Collectors.toList());
        }

        // Derive the key from the list of broker URIs:
        String brokerKey = KafkaStreamImporterConfig.getBrokerKey(brokerListString);

        // Create the input formatter:
        FormatterBuilder fb = createFormatterBuilder(properties);

        return KafkaStreamImporterConfig.getConfigsForPartitions(brokerKey, brokerList, properties.topic, properties.groupid,
                                                properties.procedure, properties.timeout, properties.buffersize, properties.commitPolicy, fb);
    }

    /*
     * Create a FormatterBuilder from the supplied arguments. If no formatter is specified by configuration, return a default CSV formatter builder.
     */
    private FormatterBuilder createFormatterBuilder(KafkaExternalLoaderCLIArguments properties) throws Exception {

        FormatterBuilder fb;
        Properties formatterProperties = m_config.m_formatterProperties;
        AbstractFormatterFactory factory;

        if (formatterProperties.size() > 0) {
            String formatterClass = m_config.m_formatterProperties.getProperty("formatter");
            String format = m_config.m_formatterProperties.getProperty("format", "csv");
            Class<?> classz = Class.forName(formatterClass);
            Class<?>[] ctorParmTypes = new Class[]{ String.class, Properties.class };
            Constructor<?> ctor = classz.getDeclaredConstructor(ctorParmTypes);
            Object[] ctorParms = new Object[]{ format, m_config.m_formatterProperties };

            factory = new AbstractFormatterFactory() {
                @Override
                public Formatter create(String formatName, Properties props) {
                    try {
                        return (Formatter) ctor.newInstance(ctorParms);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }

                }
            };
            fb = new FormatterBuilder(format, formatterProperties);
        }
        else {
            factory = new VoltCSVFormatterFactory();
            Properties props = new Properties();
            factory.create("csv", props);
            fb = new FormatterBuilder("csv", props);
        }

        fb.setFormatterFactory(factory);
        return fb;

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
     * Fetch the list of brokers from Zookeeper, and return a list of their URIs.
     */
    static private class ZooKeeperConnection {

        private ZooKeeper zoo;
        private boolean connected = false;
        /*
         * Connect to Zookeeper. Throws an exception if we can't connect for any reason.
         */
        public ZooKeeper connect(String host) throws IOException, InterruptedException {

            final CountDownLatch latch = new CountDownLatch(1);
            Watcher w = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.SyncConnected) {
                        connected = true;
                        latch.countDown();
                     }
                }
            };

            try {
                zoo = new ZooKeeper(host, 10000, w, new HashSet<Long>());
                latch.await(ZK_CONNECTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
            }

            if (connected) {
                return zoo;
            }
            else {
                throw new RuntimeException("Could not connect to Zookeeper at host:" + host);
            }

        }

        public void close() throws InterruptedException {
            zoo.close();
        }

     }

    private static List<HostAndPort> getBrokersFromZookeeper(String zookeeperHost) throws Exception {

        ZooKeeperConnection zkConnection = new ZooKeeperConnection();

        try {
            ZooKeeper zk = zkConnection.connect(zookeeperHost);
            List<String> ids = zk.getChildren("/brokers/ids", false);
            ArrayList<HostAndPort> brokers = new ArrayList<HostAndPort>();

            for (String id : ids) {
                String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
                Broker broker = Broker.createBroker(Integer.valueOf(id), brokerInfo);
                if (broker != null) {
                    m_log.warn("Adding broker: " + broker.connectionString());
                    brokers.add(new HostAndPort(broker.host(), broker.port()));
                }
            }
            return brokers;
        }
        finally {
            zkConnection.close();
        }
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
            m_loader.close();
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
        public boolean executeVolt(Object[] params, TopicPartitionInvocationCallback cb) {
           try {
               m_loader.insertRow(new RowWithMetaData(StringUtils.join(params, ","), cb.getOffset(), cb), params);
           }
           catch (Exception e) {
               m_log.error(e);
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

        final KafkaExternalLoaderCLIArguments cfg = new KafkaExternalLoaderCLIArguments();
        cfg.parse(KafkaExternalLoader.class.getName(), args);

        try {
            KafkaExternalLoader kloader = new KafkaExternalLoader(cfg);
            kloader.initialize();
            kloader.processKafkaMessages();
        }
        catch (Exception e) {
            m_log.error("Exception in KafkaExternalLoader", e);
            System.exit(-1);
        }

        System.exit(0);
    }


}
