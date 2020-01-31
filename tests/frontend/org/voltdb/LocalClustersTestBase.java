/* This file is part of VoltDB.
 * Copyright (C) 2018-2019 VoltDB Inc.
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

package org.voltdb;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.voltcore.utils.Pair;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

public class LocalClustersTestBase extends JUnit4LocalClusterTest {
    static final String JAR_NAME = "lcsmoke.jar";

    static final String INSERT_PREFIX = "Insert_";
    static final String INSERT_PREFIX_P = "Insert_P";
    static final String SELECT_ALL_PREFIX = "SelectAll_";
    static final String REPLICATED_TAG = "rep_";
    static final String STREAM_TAG = "stream_";
    static final String TOPIC_TAG = "topic_";
    static final MessageFormat REPLICATED_TABLE_FMT = new MessageFormat("create table {0}" + REPLICATED_TAG
            + "{1} (key bigint not null, value bigint not null, PRIMARY KEY(key));" + "create procedure "
            + INSERT_PREFIX + "{0}" + REPLICATED_TAG + "{1} as insert into {0}" + REPLICATED_TAG + "{1} values (?, ?);"
            + "create procedure " + SELECT_ALL_PREFIX + "{0}" + REPLICATED_TAG + "{1} as select key, value from {0}"
            + REPLICATED_TAG + "{1} order by key;" + "dr table {0}" + REPLICATED_TAG + "{1};");

    static final MessageFormat PARTITIONED_TABLE_FMT = new MessageFormat(
            "create table {0}{1} (key bigint not null, value bigint not null, PRIMARY KEY(key));"
                    + "partition table {0}{1} on column key;"
                    + "create procedure " + INSERT_PREFIX + "{0}{1} "
                    + "as insert into {0}{1} (key, value) values (?, ?);"
                    + "create procedure " + INSERT_PREFIX_P + "{0}{1} "
                    + "partition on table {0}{1} column key "
                    + "as insert into {0}{1} (key, value) values (?, ?);"
                    + "create procedure " + SELECT_ALL_PREFIX
                    + "{0}{1} as select key, value from {0}{1} order by key;"
                    + "dr table {0}{1};");

    public static final MessageFormat STREAM_FMT = new MessageFormat(
            "create stream {0}" + STREAM_TAG + "{1} export to target {2} (key bigint not null, value bigint not null);"
                    + "partition table {0}" + STREAM_TAG + "{1} on column key;"
                    + "create procedure " + INSERT_PREFIX + "{0}" + STREAM_TAG + "{1} as insert into {0}" + STREAM_TAG + "{1} values (?, ?);");

    public static final MessageFormat TOPIC_FMT = new MessageFormat(
            "create stream {0}" + TOPIC_TAG + "{1} partition on column key as topic (key bigint not null, value bigint not null);"
                    + "create procedure " + INSERT_PREFIX + "{0}" + TOPIC_TAG + "{1} as insert into {0}" + TOPIC_TAG + "{1} values (?, ?);");

    // Track the current running clusters so they can be reused between tests if the configuration doesn't change
    private static final List<ClusterConfiguration> CLUSTER_CONFIGURATIONS = new ArrayList<>();

    private static final List<Pair<LocalCluster, Client>> CLUSTERS_AND_CLIENTS = new ArrayList<>();
    protected static final List<Pair<LocalCluster, Client>> CLUSTERS_AND_CLIENTS_RO = Collections
            .unmodifiableList(CLUSTERS_AND_CLIENTS);

    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    @Rule
    public final TestName m_name = new TestName();

    @Rule
    public final TemporaryFolder m_temporaryFolder = new TemporaryFolder();

    @Rule
    public final TestWatcher m_cleanUpOnError = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            shutdownAllClustersAndClients();
        };
    };

    private String m_methodName;
    private boolean m_cleanupAfterTest = false;

    private final Set<Long> m_generatedKeys = new HashSet<>();

    @AfterClass
    public static void cleanUp() {
        shutdownAllClustersAndClients();
    }

    protected static void shutdown(Pair<LocalCluster, Client> pair) {
        shutdown(pair.getFirst(), pair.getSecond());
    }

    protected static void shutdown(LocalCluster localCluster, Client client) {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {}
        }
        if (localCluster != null) {
            try {
                localCluster.shutDown();
            } catch (Exception e) {}
        }
    }

    protected static void shutdownAllClustersAndClients() {
        System.out.println("Shutting everything down.");
        CLUSTER_CONFIGURATIONS.clear();
        CLUSTERS_AND_CLIENTS.forEach(LocalClustersTestBase::shutdown);
        CLUSTERS_AND_CLIENTS.clear();
    }

    @Before
    public void setUp() throws Exception {
        // Parameterized tests add ]
        m_methodName = m_name.getMethodName().replaceAll("[\\[\\]]", "_");
        System.out.println("Running " + m_methodName);
        VoltFile.resetSubrootForThisProcess();
    }

    @After
    public void optionalCleanUp() {
        if (m_cleanupAfterTest) {
            shutdownAllClustersAndClients();
        }
    }

    public String getMethodName() {
        return m_methodName;
    }

    protected void cleanupAfterTest() {
        m_cleanupAfterTest = true;
    }

    /**
     * Configure a clusters and corresponding client. A cluster will be created with the results of the constructed
     * cluster and client are stored in {@link #CLUSTERS_AND_CLIENTS}
     * <p>
     * If cluster with {@code config} is already running it will be left running and just the schema will be added to
     * the running clusters. The creation of clusters can be forced by calling {@link #shutdownAllClustersAndClients()}
     * prior to calling this method.
     *
     * @param config                Cluster configuration
     * @param partitionedTableCount number of partitioned tables to create
     * @param replicatedTableCount  number of replicated tables to create
     * @throws Exception if an error occurs
     */
    protected void configureClusterAndClient(ClusterConfiguration config, int partitionedTableCount,
            int replicatedTableCount) throws Exception {
        configureClustersAndClients(ImmutableList.of(config), partitionedTableCount, replicatedTableCount);
    }

    /**
     * Configure a number of clusters and corresponding clients. A cluster will be created for each
     * {@link ClusterConfiguration} within {@code configs}. The results of the constructed clusters and clients are
     * stored in {@link #CLUSTERS_AND_CLIENTS}
     * <p>
     * If clusters with {@code configs} are already running they will be left running and just the schema will be added
     * to the running clusters. The creation of clusters can be forced by calling
     * {@link #shutdownAllClustersAndClients()} prior to calling this method.
     *
     * @param configs               List cluster configurations
     * @param partitionedTableCount number of partitioned tables to create
     * @param replicatedTableCount  number of replicated tables to create
     * @throws Exception if an error occurs
     */
    protected void configureClustersAndClients(List<ClusterConfiguration> configs,
                                               int partitionedTableCount,
                                               int replicatedTableCount) throws Exception {
        configureClustersAndClients(configs, partitionedTableCount, replicatedTableCount,
                ArrayUtils.EMPTY_STRING_ARRAY);
    }

    protected void configureClustersAndClients(List<ClusterConfiguration> configs,
                                               int partitionedTableCount,
                                               int replicatedTableCount,
                                               String[] streamTargets) throws Exception {
        configureClustersAndClients(configs, partitionedTableCount, replicatedTableCount,
                streamTargets, ArrayUtils.EMPTY_STRING_ARRAY, "", "");
    }

    protected void configureClustersAndClients(List<ClusterConfiguration> configs,
            int partitionedTableCount,
            int replicatedTableCount,
            String[] streamTargets,
            String[] topics) throws Exception {
        configureClustersAndClients(configs, partitionedTableCount, replicatedTableCount,
                streamTargets, topics, "", "");
    }

    protected void configureClustersAndClients(List<ClusterConfiguration> configs,
                                               int partitionedTableCount,
                                               int replicatedTableCount,
                                               String[] streamTargets,
                                               String[] topics,
                                               String username,
                                               String password) throws Exception {
        if (configs.size() > getMaxClusters()) {
            throw new IllegalArgumentException("Maximum supported clusters is " + getMaxClusters());
        }

        if (Objects.equals(CLUSTER_CONFIGURATIONS, configs)) {
            addSchema(partitionedTableCount, replicatedTableCount, streamTargets, topics);
        } else {
            createClustersAndClientsWithCredentials(configs, partitionedTableCount, replicatedTableCount,
                    streamTargets, topics, username, password);
        }
    }

    /**
     * @param clusterId ID of cluster. 0 based
     * @return the {@link LocalCluster} for the cluster with id {@code clusterId}
     */
    protected LocalCluster getCluster(int clusterId) {
        return CLUSTERS_AND_CLIENTS.get(clusterId).getFirst();
    }

    /**
     * @param clusterId ID of cluster. 0 based
     * @return the {@link Client} for the cluster with id {@code clusterId}
     */
    public Client getClient(int clusterId) {
        return CLUSTERS_AND_CLIENTS.get(clusterId).getSecond();
    }

    protected int getClusterCount() {
        return CLUSTERS_AND_CLIENTS.size();
    }

    protected int getMaxClusters() {
        return 1;
    }

    protected ClientConfig createClientConfig() {
        return createClientConfig("", "");
    }

    protected ClientConfig createClientConfig(String username, String password) {
        ClientConfig cc = new ClientConfig(username, password);
        cc.setProcedureCallTimeout(10 * 60 * 1000); // 10 min
        return cc;
    }

    protected void shutdownCluster(int clusterId) {
        System.out.println("Shutting down cluster: " + clusterId);
        shutdown(CLUSTERS_AND_CLIENTS.get(clusterId));
    }

    protected void killHostFromCluster(int clusterId, int hostId) throws InterruptedException {
        System.out.println("Shutting down host: " + hostId + " of cluster: " + clusterId);
        CLUSTERS_AND_CLIENTS.get(clusterId).getFirst().killSingleHost(hostId);
    }

    protected Pair<LocalCluster, Client> recoverCluster(int clusterId) throws IOException {
        System.out.println("Recovering cluster: " + clusterId);
        Pair<LocalCluster, Client> pair = CLUSTERS_AND_CLIENTS.get(clusterId);

        LocalCluster lc = pair.getFirst();
        lc.overrideStartCommandVerb("recover");
        lc.startUp(false);

        Client client = lc.createAdminClient(createClientConfig());

        pair = new Pair<>(lc, client);
        CLUSTERS_AND_CLIENTS.set(clusterId, pair);
        return pair;
    }

    protected Pair<LocalCluster, Client> startCluster(int clusterId, boolean clearLocalDataDirectories,
            boolean skipInit) throws IOException {
        Pair<LocalCluster, Client> pair = CLUSTERS_AND_CLIENTS.get(clusterId);

        LocalCluster lc = pair.getFirst();
        lc.startUp(clearLocalDataDirectories, skipInit);

        pair = Pair.of(lc, lc.createAdminClient(createClientConfig()));
        CLUSTERS_AND_CLIENTS.set(clusterId, pair);
        return pair;
    }

    protected void addSchema(int partitionedTableCount, int replicatedTableCount, String[] streamTargets)
            throws Exception {
        addSchema(partitionedTableCount, replicatedTableCount, streamTargets, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    protected void addSchema(int partitionedTableCount, int replicatedTableCount,
            String[] streamTargets, String[] topics)
            throws Exception {
        String schemaDDL = createSchemaDDL(partitionedTableCount, replicatedTableCount, streamTargets, topics);

        for (Pair<LocalCluster, Client> clusterAndClient : CLUSTERS_AND_CLIENTS) {
            Client client = clusterAndClient.getSecond();
            assertEquals(ClientResponse.SUCCESS, callDMLProcedure(client, "@AdHoc", schemaDDL).getStatus());;
        }
    }

    /**
     * Create a new set of clusters using {@code configs} to configure the clusters
     *
     * @param configs               {@link List} of {@link ClusterConfiguration}s. One for each cluster to be created
     * @param partitionedTableCount number of partitioned tables to create
     * @param replicatedTableCount  number of replicated tables to create
     * @param username              username
     * @param password              password
     * @throws Exception
     */
    private void createClustersAndClientsWithCredentials(List<ClusterConfiguration> configs,
                                                         int partitionedTableCount,
                                                         int replicatedTableCount,
                                                         String[] streamTargets,
                                                         String[] topics,
                                                         String username,
                                                         String password) throws Exception {
        System.out.println("Creating clusters and clients. method: " + m_methodName + " configurations: " + configs
                + ", partitionedTableCount: " + partitionedTableCount + ", replicatedTableCount: "
                + replicatedTableCount);

        shutdownAllClustersAndClients();

        ClientConfig cc = createClientConfig(username, password);

        int clusterNumber = 0;
        for (ClusterConfiguration config : configs) {
            LocalCluster lc = null;
            Client c = null;

            DrRoleType drRoleType = config.getDrRole(configs.size());
            try {
                System.out.println("Creating cluster " + clusterNumber);
                String schemaDDL = createSchemaDDL(partitionedTableCount, replicatedTableCount, streamTargets, topics);
                lc = LocalCluster.createLocalCluster(schemaDDL, config.siteCount, config.hostCount, config.kfactor,
                        clusterNumber, 11000 + (clusterNumber * 100), clusterNumber == 0 ? 11100 : 11000,
                        m_temporaryFolder.newFolder().getAbsolutePath(), JAR_NAME, drRoleType,
                        false, config.builder, getClass().getSimpleName(), m_methodName, false, ImmutableMap.of());

                System.out.println("Creating client for cluster " + clusterNumber);
                c = lc.createAdminClient(cc);
            } catch (Throwable t) {
                shutdown(lc, c);
                throw t;
            }

            CLUSTERS_AND_CLIENTS.add(Pair.of(lc, c));

            if (clusterNumber > 0) {
                handleMultipleClusterStartup(clusterNumber, c, drRoleType);
            }

            ++clusterNumber;
        }

        CLUSTER_CONFIGURATIONS.addAll(configs);
    }

    private String createSchemaDDL(int partitionedTableCount, int replicatedTableCount, String[] streamTargets) {
        return createSchemaDDL(partitionedTableCount, replicatedTableCount, streamTargets, ArrayUtils.EMPTY_STRING_ARRAY);
    }

    private String createSchemaDDL(int partitionedTableCount, int replicatedTableCount,
            String[] streamTargets, String[] topics) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < partitionedTableCount; ++i) {
            generateTableDDL(i, TableType.PARTITIONED, sb);
        }
        for (int i = 0; i < replicatedTableCount; ++i) {
            generateTableDDL(i, TableType.REPLICATED, sb);
        }
        for (int i = 0; i < streamTargets.length; ++i) {
            generateStreamDDL(streamTargets[i], i, sb);
        }
        for (int i = 0; i < topics.length; ++i) {
            generateTopicDDL(i, sb);
        }

        return sb.toString();
    }

    protected void generateTableDDL(int tableNum, TableType type, StringBuffer sb) {
        type.generateTableDDL(sb, m_methodName, tableNum);
    }

    protected void generateStreamDDL(String target, int streamNum, StringBuffer sb) {
        TableType.STREAM.generateTableDDL(sb, m_methodName, streamNum, target);
    }

    protected void generateTopicDDL(int topicNum, StringBuffer sb) {
        TableType.TOPIC.generateTableDDL(sb, m_methodName, topicNum);
    }

    /**
     * Wrapper around calling DML procedure.
     *
     * @param client {@link Client} which will be used to call the procedure
     * @param name   of the procedure to call
     * @param args   arguments to the procedure
     */
    protected void callDMLProcedure(Client client, ProcedureCallback callback, String name, Object... args)
            throws NoConnectionsException, IOException {
        client.callProcedure(callback, name, args);
    }

    /**
     * Wrapper around calling DML procedure.
     *
     * @param client {@link Client} which will be used to call the procedure
     * @param name   of the procedure to call
     * @param args   arguments to the procedure
     * @return {@link ClientResponse} returned by {@code client} when procedure was called
     */
    protected ClientResponse callDMLProcedure(Client client, String name, Object... args)
            throws NoConnectionsException, IOException, ProcCallException {
        return client.callProcedure(name, args);
    }

    /**
     * Wrapper around calling DML run everywhere procedure.
     *
     * @param client {@link Client} which will be used to call the procedure
     * @param name   of the procedure to call
     * @param args   arguments to the procedure
     * @return {@link ClientResponse} returned by {@code client} when procedure was called
     */
    protected ClientResponseWithPartitionKey[] callDMLProcedureEverywhere(Client client, String name, Object... args)
            throws NoConnectionsException, IOException, ProcCallException {
        return client.callAllPartitionProcedure(name, args);
    }

    /**
     * Use this to insert random rows using a partitioned procedure.
     * {@link #insertRandomRows(Client, int, TableType, int)} inserts using a non-partitioned procedure.
     */
    protected void insertRandomRowsPartitioned(Client client, int tableNumber, int rowCount)
            throws NoConnectionsException, IOException, InterruptedException {
        insertRandomRows(client, INSERT_PREFIX_P, tableNumber, TableType.PARTITIONED, rowCount, r -> {});
    }

    /**
     * Calls {@link #insertRandomRows(Client, int, TableType, int, LongConsumer)} with a consumer that does nothing.
     *
     * @see #insertRandomRows(Client, int, boolean, int, LongConsumer)
     */
    public void insertRandomRows(Client client, int tableNumber, TableType tableType, int rowCount)
            throws NoConnectionsException, IOException, InterruptedException {
        insertRandomRows(client, tableNumber, tableType, rowCount, r -> {});
    }

    /**
     * Insert a row with random contents into a table using {@code client}
     *
     * @param client      {@link Client} which will be used to perform insert
     * @param tableNumber 0 based table number
     * @param tableType   TableType indicating the type of table
     * @param rowCount    Number of rows to insert into the table
     * @param consumer    Optional {@link LongConsumer} to consume all of the keys for the inserted rows
     * @throws InterruptedException
     */
    public void insertRandomRows(Client client, int tableNumber, TableType tableType, int rowCount,
            LongConsumer consumer) throws NoConnectionsException, IOException, InterruptedException {
        insertRandomRows(client, INSERT_PREFIX, tableNumber, tableType, rowCount, consumer);
    }

    private void insertRandomRows(Client client, String insertPrefix, int tableNumber, TableType tableType,
            int rowCount, LongConsumer consumer) throws NoConnectionsException, IOException, InterruptedException {
        String procedureName = getDbResourceName(insertPrefix, tableNumber, tableType);
        AtomicInteger errorCount = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(rowCount);
        for (int i = 0; i < rowCount; ++i) {
            long key = uniqueRandomKey();
            callDMLProcedure(client, cr -> {
                latch.countDown();
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    errorCount.getAndIncrement();
                }
            }, procedureName, key, m_random.nextLong());
            if (consumer != null) {
                consumer.accept(key);
            }
        }
        latch.await();
        assertEquals(0, errorCount.get());
    }

    protected void insertOneRow(Client client,
                              int tableNumber,
                              TableType tableType,
                              long key) throws NoConnectionsException, IOException, ProcCallException, InterruptedException {
        String procedureName = getDbResourceName(INSERT_PREFIX, tableNumber, tableType);
        callDMLProcedure(client, procedureName, key, key);
    }

    /**
     * Call an ordered select of all rows from a table
     *
     * @param client      {@link Client} which will be used to perform select
     * @param tableNumber 0 based table number
     * @param tableType the type of the table
     * @return {@link ClientResponse} returned by {@code client} when selected was performed
     */
    public ClientResponse selectAll(Client client, int tableNumber, TableType tableType)
            throws NoConnectionsException, IOException, ProcCallException {
        return client.callProcedure(getDbResourceName(SELECT_ALL_PREFIX, tableNumber, tableType));
    }

    /**
     * Drops a table and its associated procedures.
     *
     * @param client      Client to the cluster from which table must be dropped
     * @param tableNumber table number
     * @param type        {@link TableType} of the table
     *
     * @return ClientResponse from executing the drop statement
     */
    protected ClientResponse dropTable(Client client, int tableNumber, TableType type)
            throws ProcCallException, IOException {
        String stmt = "DROP PROCEDURE " + getDbResourceName(INSERT_PREFIX, tableNumber, type) + ";" + "DROP PROCEDURE "
                + getDbResourceName(SELECT_ALL_PREFIX, tableNumber, type) + ";" + "DROP TABLE "
                + getTableName(tableNumber, type) + ";";
        return callDMLProcedure(client, "@AdHoc", stmt);
    }

    /**
     *
     * @param baseName    of resource
     * @param tableNumber 0 based table number
     * @param tableType the type of table
     * @return name of database resource
     */
    protected String getDbResourceName(String baseName, int tableNumber, TableType tableType) {
        return baseName + m_methodName + tableType.m_tableTag + tableNumber;
    }

    /**
     * @param tableNumber 0 based table number
     * @param tableType the type of the table
     * @return the name of the corresponding table
     */
    protected String getTableName(int tableNumber, TableType tableType) {
        return getDbResourceName("", tableNumber, tableType);
    }

    /**
     * Generate a truncate table sql statement and append it to {@code sqlStatement}
     *
     * @param sqlStatement {@link StringBuilder} to which the statement will be appended
     * @param tableName    Name of table to be truncated
     */
    protected void generateTruncate(StringBuilder sqlStatement, String tableName) {
        sqlStatement.append("truncate table ").append(tableName).append(';');
    }

    /**
     * Generate a sql statement to insert a row with random contents and append it to {@code sqlStatement}
     *
     * @param sqlStatement {@link StringBuilder} to which the statement will be appended
     * @param tableName    Name of table in which to insert a row
     * @return key of row which was inserted
     */
    protected long generateRandomInsert(StringBuilder sqlStatement, String tableName) {
        long key = uniqueRandomKey();
        sqlStatement.append("insert into ").append(tableName).append(" values (").append(key).append(", ")
                .append(m_random.nextLong()).append(");");
        return key;
    }

    /**
     * Generate a sql statement to update a row with random contents and append it to {@code sqlStatement}
     *
     * @param sqlStatement {@link StringBuilder} to which the statement will be appended
     * @param tableName    Name of table in which to update a row
     * @param key          of row to update
     */
    protected void generateRandomUpdate(StringBuilder sqlStatement, String tableName, Long key) {
        sqlStatement.append("update ").append(tableName).append(" set value = ").append(m_random.nextLong())
                .append(" where key = ").append(key).append(';');
    }

    protected void generateDelete(StringBuilder sqlStatement, String tableName, Long key) {
        sqlStatement.append("delete from ").append(tableName).append(" where key = ").append(key).append(';');
    }

    private long uniqueRandomKey() {
        do {
            long key = m_random.nextLong();
            if (m_generatedKeys.add(key)) {
                return key;
            }
        } while (true);
    }

    protected void waitForDRToDrain(int clusterId) throws Exception {}

    protected void handleMultipleClusterStartup(int clusterNumber, Client client, DrRoleType drRoleType)
            throws Exception {}

    public enum TableType {
        PARTITIONED("", PARTITIONED_TABLE_FMT),
        REPLICATED(REPLICATED_TAG, REPLICATED_TABLE_FMT),
        STREAM(STREAM_TAG, STREAM_FMT),
        TOPIC(TOPIC_TAG, TOPIC_FMT);

        private final String m_tableTag;
        private final MessageFormat m_tableFormat;

        TableType(String tableTag, MessageFormat tableFormat) {
            m_tableTag = tableTag;
            m_tableFormat = tableFormat;
        }

        void generateTableDDL(StringBuffer sb, Object... args) {
            m_tableFormat.format(args, sb, null);
        }
    }

    public static class ClusterConfiguration {
        final int siteCount;
        final int hostCount;
        final int kfactor;
        final DrRoleType drRoleType;
        final VoltProjectBuilder builder;

        public ClusterConfiguration(int siteCount) {
            this(siteCount, 1, 0);
        }

        public ClusterConfiguration(int siteCount, int hostCount, int kfactor) {
            this(siteCount, hostCount, kfactor, null, null);
        }

        public ClusterConfiguration(int siteCount, int hostCount, int kfactor, VoltProjectBuilder builder) {
            this(siteCount, hostCount, kfactor, null, builder);
        }

        public ClusterConfiguration(int siteCount, int hostCount, int kfactor, DrRoleType drRoleType,
                VoltProjectBuilder builder) {
            super();
            this.siteCount = siteCount;
            this.hostCount = hostCount;
            this.kfactor = kfactor;
            this.drRoleType = drRoleType;
            this.builder = builder;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + hostCount;
            result = prime * result + kfactor;
            result = prime * result + siteCount;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            ClusterConfiguration other = (ClusterConfiguration) obj;
            return siteCount == other.siteCount && hostCount == other.hostCount && kfactor == other.kfactor;
        }

        public DrRoleType getDrRole(int clusterCount) {
            return drRoleType == null ? clusterCount > 1 ? DrRoleType.XDCR : DrRoleType.NONE : drRoleType;
        }

        @Override
        public String toString() {
            return "ClusterConfiguration [siteCount=" + siteCount + ", hostCount=" + hostCount + ", kfactor=" + kfactor
                    + "]";
        }
    }
}
