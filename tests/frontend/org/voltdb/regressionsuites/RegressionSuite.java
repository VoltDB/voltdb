/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Pattern;

import javax.net.ssl.SSLEngine;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.AdHocQueryTester;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.client.Client;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientConfigForTest;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.net.HostAndPort;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import junit.framework.TestCase;

/**
 * Base class for a set of JUnit tests that perform regression tests
 * on a running VoltDB server. It is assumed that all tests will access
 * a particular VoltDB server to do their work and check the output of
 * any procedures called. The main feature of this class is that the
 * backend instance of VoltDB is very flexible and the tests can be run
 * on multiple instances of VoltDB to test different VoltDB topologies.
 *
 */
public class RegressionSuite extends TestCase {
    final static double GEOGRAPHY_EPSILON = 1.0e-13;

    static int m_verboseDiagnosticRowCap = 40;
    protected VoltServerConfig m_config;
    protected String m_username = "default";
    protected String m_password = "password";
    protected final ArrayList<Client> m_clients = new ArrayList<>();
    private final ArrayList<SocketChannel> m_clientChannels = new ArrayList<>();
    final String m_methodName;
    // If the current RegressionSuite instance is the last one in the current VoltServerConfig,
    // shutdown the cluster completely after finishing the test.
    boolean m_completeShutdown;
    boolean m_fatalFailure;
    /**
     * We sometimes need to take different actions, i.e. skip a test, change error message, etc. depending on
     * the actual parser/planner being in use. Eventually when we fully integrate with Calcite and replace our
     * legacy planner, we don't need to take branches on those test logic.
     */
    final static boolean USING_CALCITE =
            Boolean.parseBoolean(System.getProperty("plan_with_calcite", "false"));

    /**
     * Trivial constructor that passes parameter on to superclass.
     * @param name The name of the method to run as a test. (JUnit magic)
     */
    public RegressionSuite(final String name) {
        super(name);
        m_methodName = name;

        VoltServerConfig.setInstanceSet(new HashSet<>());
        m_completeShutdown = false;
        m_fatalFailure = false;
    }

    /**
     * JUnit special method called to setup the test. This instance will start
     * the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void setUp() throws Exception {
        //New tests means a new server thread that hasn't done a restore
        m_config.setCallingMethodName(m_methodName);
        m_config.startUp(true);
    }

    private static Catalog getCurrentCatalog() {
        CatalogContext context = VoltDB.instance().getCatalogContext();
        if (context == null) {
            return null;
        }
        InMemoryJarfile currentCatalogJar = context.getCatalogJar();
        String serializedCatalogString = CatalogUtil.getSerializedCatalogStringFromJar(currentCatalogJar);
        assertNotNull(serializedCatalogString);
        Catalog c = new Catalog();
        c.execute(serializedCatalogString);
        return c;
    }

    /**
     * JUnit special method called to shutdown the test. This instance will
     * stop the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void tearDown() throws Exception {
        if (m_fatalFailure) {
            System.exit(0);
        } else if (m_completeShutdown) {
            m_config.shutDown();
        } else {
            Catalog currentCataog = getCurrentCatalog();
            if (currentCataog != null) {
                CatalogDiffEngine diff = new CatalogDiffEngine(m_config.getInitialCatalog(), currentCataog);
                // All catalog changes will have a changed "set /clusters#cluster/databases#database schema" command.
                // If the diff command only has this line, it means something is changed first but restored later.
                // We will ignore this case.
                if (diff.commands().split("\n").length > 1) {
                    fail("Catalog changed in test " + getName() +
                            " while the regression suite optimization is on: \n" +
                            diff.getDescriptionOfChanges(false) + "\n<" + diff.commands() + ">");
                }
            }

            Client client = getClient();
            VoltTable tableList = client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
            ArrayList<String> tableNames = new ArrayList<>(tableList.getRowCount());
            int tableNameColIdx = tableList.getColumnIndex("TABLE_NAME");
            int tableTypeColIdx = tableList.getColumnIndex("TABLE_TYPE");
            while (tableList.advanceRow()) {
                String tableType = tableList.getString(tableTypeColIdx);
                if (! tableType.equalsIgnoreCase("EXPORT")) {
                    tableNames.add(tableList.getString(tableNameColIdx));
                }
            }
            for (String tableName : tableNames) {
                try {
                    client.callProcedure("@AdHoc", "DELETE FROM " + tableName);
                } catch (ProcCallException pce) {
                    if (! pce.getMessage().contains("Illegal to modify a materialized view.")) {
                        fail("Hit an exception when cleaning up tables between tests: " + pce.getMessage());
                    }
                }
            }
            client.drain();
        }
        for (final Client c : m_clients) {
            c.close();
        }
        synchronized (m_clientChannels) {
            for (final SocketChannel sc : m_clientChannels) {
                try {
                    ConnectionUtil.closeConnection(sc);
                } catch (final IOException e) {
                    e.printStackTrace();
                }
            }
            m_clientChannels.clear();
        }
        m_clients.clear();
    }

    /**
     * @return Is the underlying instance of VoltDB running HSQL?
     */
    public boolean isHSQL() {
        return m_config.isHSQL();
    }

    /**
     * @return The number of logical partitions in this configuration
     */
    int getLogicalPartitionCount() {
        return m_config.getLogicalPartitionCount();
    }

    /**
     * @return Is the underlying instance of VoltDB running Valgrind with the IPC client?
     */
    public boolean isValgrind() {
        return m_config.isValgrind();
    }

    boolean isLocalCluster() {
        return m_config instanceof LocalCluster;
    }

    public boolean isDebug() {
        return m_config.isDebug();
    }

    /**
     * @return a reference to the associated VoltServerConfig
     */
    public VoltServerConfig getServerConfig() {
        return m_config;
    }

    public Client getAdminClient() throws IOException {
        return getClient(1000 * 60 * 10, ClientAuthScheme.HASH_SHA256, true); // 10 minute default
    }

    public Client getClient() throws IOException {
        return getClient(1000 * 60 * 10, ClientAuthScheme.HASH_SHA256); // 10 minute default
    }

    public Client getClient(ClientAuthScheme scheme) throws IOException {
        return getClient(1000 * 60 * 10, scheme); // 10 minute default
    }

    public Client getClientToHostId(int hostId) throws IOException {
        return getClientToHostId(hostId, 1000 * 60 * 10); // 10 minute default
    }

    public Client getClientToSubsetHosts(int[] hostIds) throws IOException {
        return getClientToSubsetHosts(hostIds, 1000 * 60 * 10); // 10 minute default
    }

    public Client getFullyConnectedClient() throws IOException {
        return getFullyConnectedClient(1000 * 60 * 10); // 10 minute default
    }

    /**
     * Get a VoltClient instance connected to the server driven by the
     * VoltServerConfig instance. Just pick from the list of listeners
     * randomly.
     *
     * Only uses the time
     *
     * @return A VoltClient instance connected to the server driven by the
     * VoltServerConfig instance.
     */
    public Client getClient(long timeout) throws IOException {
        return getClient(timeout, ClientAuthScheme.HASH_SHA256);
    }

    /**
     * Get a VoltClient instance connected to the server driven by the
     * VoltServerConfig instance. Just pick from the list of listeners
     * randomly.
     *
     * Only uses the time
     *
     * @return A VoltClient instance connected to the server driven by the
     * VoltServerConfig instance.
     */
    public Client getClient(long timeout, ClientAuthScheme scheme) throws IOException {
        return getClient(timeout, scheme, false);
    }

    public Client getClient(long timeout, ClientAuthScheme scheme, boolean useAdmin) throws IOException {
        final Random r = new Random();
        String listener;
        if (useAdmin) {
            listener = m_config.getAdminAddress(r.nextInt(m_config.getListenerCount()));
        } else {
            listener = m_config.getListenerAddress(r.nextInt(m_config.getListenerCount()));
        }
        ClientConfig config = new ClientConfigForTest(m_username, m_password, scheme);
        config.setConnectionResponseTimeout(timeout);
        config.setProcedureCallTimeout(timeout);
        final Client client = ClientFactory.createClient(config);
        // Use the port generated by LocalCluster if applicable
        try {
            client.createConnection(listener);
        } catch (ConnectException e) { // retry once
            String newListener;
            do {
                if (useAdmin) {
                    newListener = m_config.getAdminAddress(r.nextInt(m_config.getListenerCount()));
                } else {
                    newListener = m_config.getListenerAddress(r.nextInt(m_config.getListenerCount()));
                }
            } while (newListener.equals(listener) && m_config.getListenerCount() > 1);
            client.createConnection(newListener);
        }
        m_clients.add(client);
        return client;
    }

    /**
     * Get a VoltClient instance connected to the server driven by the
     * VoltServerConfig instance. Just pick from the list of listeners
     * randomly.
     *
     * Only uses the time
     *
     * @return A VoltClient instance connected to the server driven by the
     * VoltServerConfig instance.
     */
    public Client getClientSha1(long timeout) throws IOException {
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        String listener = listeners.get(r.nextInt(listeners.size()));
        ClientConfig config = new ClientConfigForTest(m_username, m_password, ClientAuthScheme.HASH_SHA1);
        config.setConnectionResponseTimeout(timeout);
        config.setProcedureCallTimeout(timeout);
        final Client client = ClientFactory.createClient(config);
        // Use the port generated by LocalCluster if applicable
        try {
            client.createConnection(listener);
        } catch (ConnectException e) { // retry once
            listener = listeners.get(r.nextInt(listeners.size()));
            client.createConnection(listener);
        }
        m_clients.add(client);
        return client;
    }

    /**
     * Get a VoltClient instance connected to a specific server driven by the
     * VoltServerConfig instance. Find the server by the config's HostId.
     *
     * @return A VoltClient instance connected to the server driven by the
     * VoltServerConfig instance.
     */
    Client getClientToHostId(int hostId, long timeout) throws IOException {
        final String listener = m_config.getListenerAddress(hostId);
        ClientConfig config = new ClientConfigForTest(m_username, m_password);
        config.setConnectionResponseTimeout(timeout);
        config.setProcedureCallTimeout(timeout);
        final Client client = ClientFactory.createClient(config);
        try {
            client.createConnection(listener);
        } catch (ConnectException e) { // retry once
            client.createConnection(listener);
        }
        m_clients.add(client);
        return client;
    }

    Client getClientToSubsetHosts(int[] hostIds, long timeout) throws IOException {
        List<String> listeners = new ArrayList<>();
        for (int hostId : hostIds) {
            listeners.add(m_config.getListenerAddress(hostId));
        }
        ClientConfig config = new ClientConfigForTest(m_username, m_password);
        config.setConnectionResponseTimeout(timeout);
        config.setProcedureCallTimeout(timeout);
        final Client client = ClientFactory.createClient(config);
        for (String listener : listeners) {
            try {
                client.createConnection(listener);
            } catch (ConnectException e) { // retry once
                client.createConnection(listener);
            }
        }
        m_clients.add(client);
        return client;
    }

    public Client getFullyConnectedClient(long timeout) throws IOException {
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        ClientConfig config = new ClientConfigForTest(m_username, m_password);
        config.setConnectionResponseTimeout(timeout);
        config.setProcedureCallTimeout(timeout);
        final Client client = ClientFactory.createClient(config);
        for (String listener : listeners) {
            // Use the port generated by LocalCluster if applicable
            try {
                client.createConnection(listener);
            } catch (ConnectException e) { // retry once
                listener = listeners.get(r.nextInt(listeners.size()));
                client.createConnection(listener);
            }
        }
        m_clients.add(client);
        return client;
    }

    /**
     * Release a client instance and any resources associated with it
     */
    public void releaseClient(Client c) throws InterruptedException {
        boolean removed = m_clients.remove(c);
        assert(removed);
        c.close();
    }

    /**
     * Get a SocketChannel that is an authenticated connection to a server driven by the
     * VoltServerConfig instance. Just pick from the list of listeners
     * randomly.
     *
     * @return A SocketChannel that is already authenticated with the server
     */

    SocketChannel getClientChannel() throws IOException {
        return getClientChannel(false);
    }

    SocketChannel getClientChannel(final boolean noTearDown) throws IOException {
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        final String listener = listeners.get(r.nextInt(listeners.size()));
        byte[] hashedPassword = ConnectionUtil.getHashedPassword(m_password);
        HostAndPort hNp = HostAndPort.fromString(listener);
        int port = Constants.DEFAULT_PORT;
        if (hNp.hasPort()) {
            port = hNp.getPort();
        }

        SSLEngine sslEngine = null;
        boolean sslEnabled = Boolean.valueOf(System.getenv("ENABLE_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_SSL")) : System.getenv("ENABLE_SSL"));
        if (sslEnabled) {
            SslContext sslContext = SSLConfiguration.createClientSslContext(new SSLConfiguration.SslConfig());
            sslEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT, hNp.getHost(), port);
            sslEngine.setUseClientMode(true);
        }

        final SocketChannel channel = (SocketChannel)
        ConnectionUtil.getAuthenticatedConnection(hNp.getHost(), m_username, hashedPassword, port, null,
                ClientAuthScheme.getByUnencodedLength(hashedPassword.length), sslEngine, 0)[0];
        channel.configureBlocking(true);
        if (!noTearDown) {
            synchronized (m_clientChannels) {
                m_clientChannels.add(channel);
            }
        }
        return channel;
    }
    /**
     * Protected method used by MultiConfigSuiteBuilder to set the VoltServerConfig
     * instance a particular test will run with.
     *
     * @param config An instance of VoltServerConfig to run tests with.
     */
    void setConfig(final VoltServerConfig config) {
        m_config = config;
    }


    @Override
    public String getName() {
        // munge the test name with the VoltServerConfig instance name
        return super.getName() + "-" + m_config.getName();
    }

    /**
     * Return appropriate port for hostId. Deal with LocalCluster providing non-default ports.
     * @param hostId zero-based host id
     * @return port number
     */
    public int port(int hostId) {
        return isLocalCluster() ? ((LocalCluster)m_config).port(hostId) : VoltDB.DEFAULT_PORT+hostId;
    }

    /**
     * Return appropriate admin port for hostId. Deal with LocalCluster providing non-default ports.
     * @param hostId zero-based host id
     * @return admin port number
     */
    public int adminPort(int hostId) {
        return isLocalCluster() ? ((LocalCluster)m_config).adminPort(hostId) : VoltDB.DEFAULT_ADMIN_PORT+hostId;
    }

    /**
     * Return appropriate internal port for hostId. Deal with LocalCluster providing non-default ports.
     * @param hostId zero-based host id
     * @return internal port number
     */
    public int internalPort(int hostId) {
        return isLocalCluster() ? ((LocalCluster)m_config).internalPort(hostId) : org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT+hostId;
    }

    static protected void validateDMLTupleCount(Client c, String sql, long modifiedTupleCount)
            throws IOException, ProcCallException {
        VoltTable vt = c.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(sql, vt, new long[][] {{modifiedTupleCount}});
    }

    static protected void validateTableOfLongs(Client c, String sql, long[][] expected)
            throws IOException, ProcCallException {
        VoltTable vt = c.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(sql, vt, expected);
    }

    static public void validateTableOfScalarLongs(VoltTable vt, long[] expected) {
        assertNotNull(expected);
        assertEquals("Different number of rows! ", expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            validateRowOfLongs(vt, new long[] {expected[i]});
        }
    }

    static protected void validateTableOfScalarLongs(Client client, String sql, long[] expected)
            throws IOException, ProcCallException {
        assertNotNull(expected);
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfScalarLongs(vt, expected);
    }

    static protected void validateTableOfScalarDecimals(Client client, String sql, BigDecimal[] expected)
            throws IOException, ProcCallException {
        assertNotNull(expected);
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals("Different number of rows! ", expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            assertTrue(vt.advanceRow());
            String message = "at column 0,";

            BigDecimal actual = new BigDecimal(-10000000);
            try {
                actual = vt.getDecimalAsBigDecimal(i);
            } catch (IllegalArgumentException ex) {
                ex.printStackTrace();
                fail(message);
            }
            assertEquals(message, expected[i], actual);
        }
    }

    private static void dumpExpectedLongs(long[][] expected) {
        System.out.println("row count:" + expected.length);
        for (long[] row : expected) {
            String prefix = "{ ";
            for (long value : row) {
                System.out.print(prefix + value);
                prefix = ", ";
            }
            System.out.println(" }");
        }
    }

    private static void validateTableOfLongs(String messagePrefix, VoltTable vt, long[][] expected) {
        assertNotNull(expected);
        if (expected.length != vt.getRowCount() && vt.getRowCount() < m_verboseDiagnosticRowCap) {
            System.out.println("Diagnostic dump of unexpected result for " + messagePrefix + " : " + vt);
            System.out.println("VS. expected : ");
            dumpExpectedLongs(expected);
        }
        assertEquals(messagePrefix + " returned wrong number of rows.  ",
                        expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            validateRowOfLongs(messagePrefix + " at row " + (i+1) + ", ", vt, expected[i]);
        }
    }

    protected void validateRowCount(Client client, String query, int expected)
            throws IOException, ProcCallException {
        VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
        int actual = result.getRowCount();
        assertEquals("Wrong row count from query '" + query + "'", expected, actual);
    }

    public static void validateTableOfLongs(VoltTable vt, long[][] expected) {
        validateTableOfLongs("", vt, expected);
    }

    static protected void validateRowOfLongs(String messagePrefix, VoltTable vt, long [] expected) {
        int len = expected.length;
        assertTrue(vt.advanceRow());
        for (int i=0; i < len; i++) {
            String message = messagePrefix + "at column " + (i+1) + ", ";

            long actual = -10000000;
            // ENG-4295: hsql bug: HSQLBackend sometimes returns wrong column type.
            try {
                actual = vt.getLong(i);
            } catch (IllegalArgumentException ex) {
                try {
                    actual = (long) vt.getDouble(i);
                } catch (IllegalArgumentException newEx) {
                    try {
                        actual = vt.getTimestampAsLong(i);
                    } catch (IllegalArgumentException exTm) {
                        try {
                            actual = vt.getDecimalAsBigDecimal(i).longValueExact();
                        } catch (IllegalArgumentException newerEx) {
                            newerEx.printStackTrace();
                            fail(message);
                        }
                    } catch (ArithmeticException newestEx) {
                        newestEx.printStackTrace();
                        fail(message);
                    }
                }
            }

            // Long.MIN_VALUE is like a NULL
            if (expected[i] != Long.MIN_VALUE) {
                assertEquals(message, expected[i], actual);
            }
            else {
                VoltType type = vt.getColumnType(i);
                assertEquals(message + "expected null: ", Long.parseLong(type.getNullValue().toString()), actual);
            }
        }
    }

    static protected void validateRowOfLongs(VoltTable vt, long [] expected) {
        validateRowOfLongs("", vt, expected);
    }

    /**
     * Given a two dimensional array, randomly permute the rows, but
     * leave the columns alone.  This is used to generate test cases for kinds
     * of sorts.
     *
     * @param input
     */
    static<T> T[][] shuffleArray(T [][] input) {
        T[][] output = input.clone();
        Integer [] indices = new Integer[input.length];
        for (int idx = 0; idx < indices.length; idx += 1) {
            indices[idx] = Integer.valueOf(idx);
        }
        List<Integer> permutation = Arrays.asList(indices);
        Collections.shuffle(permutation);
        for (int idx = 0; idx < input.length; idx += 1) {
            output[idx] = input[permutation.get(idx)];
        }
        return output;
    }

    static void validateTableColumnOfScalarLong(VoltTable vt, int col, long[] expected) {
        assertNotNull(expected);
        assertEquals(expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            assertTrue(vt.advanceRow());
            long actual = vt.getLong(col);

            if (expected[i] == Long.MIN_VALUE) {
                assertTrue(vt.wasNull());
                assertEquals(null, actual);
            } else {
                assertEquals(expected[i], actual);
            }
        }
    }

    static protected void validateTableColumnOfScalarVarchar(Client client, String sql, String[] expected)
            throws IOException, ProcCallException {
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarVarchar(vt, 0, expected);
    }

    static protected void validateTableColumnOfScalarVarchar(VoltTable vt, String[] expected) {
        validateTableColumnOfScalarVarchar(vt, 0, expected);
    }

    static protected void validateTableColumnOfScalarVarchar(VoltTable vt, int col, String[] expected) {
        assertNotNull(expected);
        assertEquals(expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            assertTrue(vt.advanceRow());
            if (expected[i] == null) {
                String actual = vt.getString(col);
                assertTrue(vt.wasNull());
                assertNull(actual);
            } else {
                assertEquals(expected[i], vt.getString(col));
            }
        }
    }


    static protected void validateTableColumnOfScalarVarbinary(Client client, String sql, String[] expected)
            throws IOException, ProcCallException {
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarVarbinary(vt, 0, expected);
    }

    static private void validateTableColumnOfScalarVarbinary(VoltTable vt, int col, String[] expected) {
          assertNotNull(expected);
          assertEquals(expected.length, vt.getRowCount());
          int len = expected.length;
          for (int i=0; i < len; i++) {
              assertTrue(vt.advanceRow());
              byte[] actual = vt.getVarbinary(col);

              if (expected[i] == null) {
                  assertTrue(vt.wasNull());
                  assertNull(actual);
              } else {
                  assertEquals(expected[i], Encoder.hexEncode(actual));
              }
          }
    }

    static protected void validateTableColumnOfScalarFloat(Client client, String sql, double[] expected)
            throws IOException, ProcCallException {
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarFloat(vt, 0, expected);
    }

    static void validateTableColumnOfScalarFloat(VoltTable vt, int col, double[] expected) {
          assertNotNull(expected);
          assertEquals(expected.length, vt.getRowCount());
          int len = expected.length;
          for (int i=0; i < len; i++) {
              assertTrue(vt.advanceRow());
              double actual = vt.getDouble(col);

              if (expected[i] <= VoltType.NULL_FLOAT) {
                  assertTrue(vt.wasNull());
              } else {
                  assertEquals(expected[i], actual, 0.00001);
              }
          }
    }

    private void validateRowOfDecimal(VoltTable vt, BigDecimal [] expected) {
        int len = expected.length;
        assertTrue(vt.advanceRow());
        for (int i=0; i < len; i++) {
            BigDecimal actual = null;
            try {
                actual = vt.getDecimalAsBigDecimal(i);
            } catch (IllegalArgumentException ex) {
                ex.printStackTrace();
                fail();
            }
            if (expected[i] != null) {
                assertNotSame(null, actual);
                assertEquals(expected[i], actual);
            } else if (isHSQL()) {
                // We don't actually use this with
                // HSQL.  So, just assert failure here.
                fail("HSQL is not used to test the Volt DECIMAL type.");
            } else {
                assertTrue(vt.wasNull());
            }
        }
    }

    private void validateTableOfDecimal(VoltTable vt, BigDecimal[][] expected) {
        assertNotNull(expected);
        assertEquals(expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            validateRowOfDecimal(vt, expected[i]);
        }
    }

    static int m_defaultScale = 12;
    static String m_roundingEnabledProperty = "BIGDECIMAL_ROUND";
    static String m_roundingModeProperty = "BIGDECIMAL_ROUND_POLICY";
    static String m_defaultRoundingEnablement = "true";
    static String m_defaultRoundingMode = "HALF_UP";

    static String getRoundingString(String label) {
        return String.format("%sRounding %senabled, mode is %s",
                label == null ? (label + ": ") : "",
                VoltDecimalHelper.isRoundingEnabled() ? "is " : "is *NOT* ",
                VoltDecimalHelper.getRoundingMode().toString());
    }

    /*
     * This little helper function converts a string to
     * a decimal, and, maybe, rounds it to the Volt default scale
     * using the given mode.  If roundingEnabled is false, no
     * rounding is done.
     */
    static BigDecimal roundDecimalValue(
            String decimalValueString, boolean roundingEnabled, RoundingMode mode) {
        BigDecimal bd = new BigDecimal(decimalValueString);
        if (!roundingEnabled) {
            return bd;
        }
        int precision = bd.precision();
        int scale = bd.scale();
        int lostScale = scale - m_defaultScale ;
        if (lostScale <= 0) {
            return bd;
        }
        int newPrecision = precision - lostScale;
        MathContext mc = new MathContext(newPrecision, mode);
        BigDecimal nbd = bd.round(mc);
        assertTrue(nbd.scale() <= m_defaultScale);
        if (nbd.scale() != m_defaultScale) {
            nbd = nbd.setScale(m_defaultScale);
        }
        assertEquals(getRoundingString("Decimal Scale setting failure"), m_defaultScale, nbd.scale());
        return nbd;
    }

    void validateTableOfDecimal(Client c, String sql, BigDecimal[][] expected) throws Exception {
        assertNotNull(expected);
        VoltTable vt = c.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfDecimal(vt, expected);
    }

    static void validateTableColumnOfScalarDecimal(Client client, String sql, BigDecimal[] expected)
            throws IOException, ProcCallException {
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableColumnOfScalarDecimal(vt, 0, expected);
    }

    static void validateTableColumnOfScalarDecimal(VoltTable vt, int col, BigDecimal[] expected) {
        assertNotNull(expected);
        assertEquals(expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            assertTrue(vt.advanceRow());
            BigDecimal actual = vt.getDecimalAsBigDecimal(col);

            if (expected[i] == null) {
                assertTrue(vt.wasNull());
                assertNull(actual);
            } else {
                BigDecimal rounded = expected[i].setScale(m_defaultScale, RoundingMode.valueOf(m_defaultRoundingMode));
                assertEquals(rounded, actual);
            }
        }
  }

    void assertTablesAreEqual(String prefix, VoltTable expectedRows, VoltTable actualRows) {
        assertTablesAreEqual(prefix, expectedRows, actualRows, null);
    }

    private static final long TOO_MUCH_INFO = 100;
    void assertTablesAreEqual(String prefix, VoltTable expectedRows, VoltTable actualRows, Double epsilon) {
        assertEquals(prefix + "column count mismatch.  Expected: " + expectedRows.getColumnCount() + " actual: " + actualRows.getColumnCount(),
                expectedRows.getColumnCount(), actualRows.getColumnCount());
        if (expectedRows.getRowCount() != actualRows.getRowCount()) {
            long expRowCount = expectedRows.getRowCount();
            long actRowCount = actualRows.getRowCount();
            if (expRowCount + actRowCount < TOO_MUCH_INFO) {
                System.out.println("Expected: " + expectedRows);
                System.out.println("Actual:   " + actualRows);
            } else {
                System.out.println("Expected: " + expRowCount + " rows");
                System.out.println("Actual:   " + actRowCount + " rows");
            }
            fail(prefix + "row count mismatch.  Expected: " + expectedRows.getRowCount() + " actual: " + actualRows.getRowCount());
        }
        int rowNum = 1;
        while (expectedRows.advanceRow()) {
            if (! actualRows.advanceRow()) {
                fail(prefix + "too few actual rows; expected more than " + rowNum);
            }
            for (int j = 0; j < actualRows.getColumnCount(); j++) {
                String columnName = actualRows.getColumnName(j);
                String colPrefix = prefix + "row " + rowNum + ": column: " + columnName + ": ";
                VoltType actualType = actualRows.getColumnType(j);
                VoltType expectedType = expectedRows.getColumnType(j);
                assertEquals(colPrefix + "type mismatch", expectedType, actualType);

                Object expectedObj = expectedRows.get(j, expectedType);
                Object actualObj = actualRows.get(j, actualType);
                if (expectedRows.wasNull()) {
                    if (actualRows.wasNull()) {
                        continue;
                    }
                    fail(colPrefix + "expected null, got non null value: " + actualObj);
                } else {
                    assertFalse(colPrefix + "expected the value " + expectedObj +
                            ", got a null value.",
                            actualRows.wasNull());
                    String message = colPrefix + "values not equal: ";
                    if (expectedType == VoltType.FLOAT) {
                        if (epsilon != null) {
                            assertEquals(message, (Double)expectedObj, (Double)actualObj, epsilon);
                            continue;
                        }
                        // With no epsilon provided, fall through to take
                        // a chance on an exact value match, but helpfully
                        // annotate any false positive that results.
                        message += ". NOTE: You may want to pass a" +
                                " non-null epsilon value >= " +
                                Math.abs((Double)expectedObj - (Double)actualObj) +
                                " to the table comparison test " +
                                " if nearly equal FLOAT values are " +
                                " causing a false mismatch.";
                    }
                    assertEquals(message, expectedObj, actualObj);
                }
            }
            rowNum++;
        }
    }

    public static void assertEquals(String msg, GeographyPointValue expected, GeographyPointValue actual) {
        assertApproximatelyEquals(msg, expected, actual, GEOGRAPHY_EPSILON);
    }

    /**
     * Assert that two points are approximately equal.  By this we mean the latitude and
     * longitude differ by at most epsilon.  If epsilon is zero or negative this means
     * equality.
     *
     * @param msg
     * @param expected
     * @param actual
     */
    public static void assertApproximatelyEquals(String msg, GeographyPointValue expected, GeographyPointValue actual, double epsilon) {
        if (epsilon > 0) {
            assertEquals(msg + " latitude: ", expected.getLatitude(), actual.getLatitude(), epsilon);
            assertEquals(msg + " longitude: ", expected.getLongitude(), actual.getLongitude(), epsilon);
        } else {
            assertEquals(msg + " latitude: ", expected.getLatitude(), actual.getLatitude());
            assertEquals(msg + " longitude: ", expected.getLongitude(), actual.getLongitude());
        }
    }

    public static void assertEquals(GeographyPointValue expected, GeographyPointValue actual) {
        assertEquals("Points not equal: ", expected, actual);
    }

    /**
     * Assert that two geography values are equal.  This delegates to
     * assertApproximatelyEquals with epsilon equal to zero.
     *
     * @param msg
     * @param expected
     * @param actual
     */
    public static void assertEquals(String msg, GeographyValue expected, GeographyValue actual) {
        assertApproximatelyEquals(msg, expected, actual, GEOGRAPHY_EPSILON);
    }

    /**
     * Assert that two geography values are approximately equal.  By approximately
     * equal we mean that the vertices of the expected and actual values differ
     * by at most epsilon.  If epsilon is not positive this means the values must
     * be exactly equal.
     *
     * @param msg
     * @param expected
     * @param actual
     * @param epsilon
     */
    static void assertApproximatelyEquals(String msg, GeographyValue expected, GeographyValue actual, double epsilon) {
        if (expected == actual) {
            return;
        }

        // caller checks for null in the expected value
        assert (expected != null);

        if (actual == null) {
            fail(msg + " found null value when non-null expected");
        }

        List<List<GeographyPointValue>> expectedLoops = expected.getRings();
        List<List<GeographyPointValue>> actualLoops = actual.getRings();

        assertEquals(msg + "wrong number of loops, expected " + expectedLoops.size() + ", "
                + "got " + actualLoops.size(),
                expectedLoops.size(), actualLoops.size());

        int loopCtr = 0;
        Iterator<List<GeographyPointValue>> expectedLoopIt = expectedLoops.iterator();
        for (List<GeographyPointValue> actualLoop : actualLoops) {
            List<GeographyPointValue> expectedLoop = expectedLoopIt.next();
            assertEquals(msg + "loop " + loopCtr + " should have " + expectedLoop.size()
                    + " vertices, but has " + actualLoop.size() + ";",
                    expectedLoop.size(), actualLoop.size());

            int vertexCtr = 0;
            Iterator<GeographyPointValue> expectedVertexIt = expectedLoop.iterator();
            for (GeographyPointValue actualPt : actualLoop) {
                GeographyPointValue expectedPt = expectedVertexIt.next();
                String prefix = msg + "at loop " + loopCtr + ", vertex " + vertexCtr;
                assertApproximatelyEquals(prefix, expectedPt, actualPt, epsilon);
                ++vertexCtr;
            }

            ++loopCtr;
        }
    }

    public static void assertEquals(GeographyValue expected, GeographyValue actual) {
        assertEquals("Geographies not equal: ", expected, actual);
    }

    private static void assertApproximateContentOfRow(int row, Object[] expectedRow, VoltTable actualRow, double epsilon) {
        assertEquals("Actual row has wrong number of columns",
                expectedRow.length, actualRow.getColumnCount());
        for (int i = 0; i < expectedRow.length; ++i) {
            String msg = "Row " + row + ", col " + i + ": ";
            Object expectedObj = expectedRow[i];
            if (expectedObj == null) {
                VoltType vt = actualRow.getColumnType(i);
                Object actualValue = actualRow.get(i, vt);
                String fullMsg = msg + "expected null, but got: "+actualValue;
                if (actualValue instanceof byte[]) {
                    fullMsg = msg+"expected null, but got VARBINARY with array of byte values: "
                            + Arrays.toString((byte[])actualValue);
                } else if (actualValue instanceof Byte[]) {
                    fullMsg = msg+"expected null, but got VARBINARY with array of Byte values: "
                            + Arrays.toString((Byte[])actualValue);
                }
                assertTrue(fullMsg, actualRow.wasNull());
            } else if (expectedObj instanceof GeographyPointValue) {
                assertApproximatelyEquals(msg, (GeographyPointValue) expectedObj, actualRow.getGeographyPointValue(i), epsilon);
            } else if (expectedObj instanceof GeographyValue) {
                assertApproximatelyEquals(msg, (GeographyValue) expectedObj, actualRow.getGeographyValue(i), epsilon);
            } else if (expectedObj instanceof Long) {
                long val = (Long)expectedObj;
                assertEquals(msg, val, actualRow.getLong(i));
            } else if (expectedObj instanceof Integer) {
                long val = ((Integer)expectedObj).longValue();
                assertEquals(msg, val, actualRow.getLong(i));
            } else if (expectedObj instanceof Short) {
                long val = ((Short)expectedObj).longValue();
                assertEquals(msg, val, actualRow.getLong(i));
            } else if (expectedObj instanceof Byte) {
                long val = ((Byte) expectedObj).longValue();
                assertEquals(msg, val, actualRow.getLong(i));
            } else if (expectedObj instanceof Double) {
                Double expectedValue = (Double)expectedObj;
                final Double actualValue = actualRow.getDouble(i);
                // Either both are null or neither is null
                assertEquals(msg+"expected "+expectedValue+" but got "+actualValue+": checking for null FLOAT: ",
                        expectedValue == null, actualRow.wasNull());
                if (epsilon <= 0 || !Double.isFinite(expectedValue)) {
                    String fullMsg = msg + String.format("Expected value %f != actual value %f", expectedValue, actualValue);
                    assertEquals(fullMsg, expectedValue, actualValue);
                } else {
                    String fullMsg = msg + String.format("abs(Expected Value - Actual Value) = %e >= %e: expected %f, got %f",
                            Math.abs(expectedValue - actualValue), epsilon, expectedValue, actualValue);
                    assertTrue(fullMsg, Math.abs(expectedValue - actualValue) < epsilon);
                }
            } else if (expectedObj instanceof BigDecimal) {
                BigDecimal exp = (BigDecimal)expectedObj;
                BigDecimal got = actualRow.getDecimalAsBigDecimal(i);
                // Either both are null or neither is null
                assertEquals(msg+"expected "+exp+" but got "+got+": checking for null DECIMAL: ",
                        exp == null, got == null);
                assertEquals(msg, exp.doubleValue(), got.doubleValue(), epsilon);
            } else if (expectedObj instanceof byte[]) {
                byte[] expectedVarbinary = (byte[]) expectedObj;
                byte[] actualVarbinary = actualRow.getVarbinary(i);
                assertEquals(msg+"length of VARBINARY: ", expectedVarbinary.length, actualVarbinary.length);
                for (int k = 0; k < expectedVarbinary.length; k++) {
                    assertEquals(msg+"index "+k+" of VARBINARY value: ", expectedVarbinary[k], actualVarbinary[k]);
                }
            } else if (expectedObj instanceof Byte[]) {
                Byte[] expectedVarbinary = (Byte[]) expectedObj;
                Byte[] actualVarbinary = SerializationHelper.boxUpByteArray(actualRow.getVarbinary(i));
                assertEquals(msg+"length of VARBINARY: ", expectedVarbinary.length, actualVarbinary.length);
                for (int k = 0; k < expectedVarbinary.length; k++) {
                    assertEquals(msg+"index "+k+" of VARBINARY value: ", expectedVarbinary[k], actualVarbinary[k]);
                }
            } else if (expectedObj instanceof String) {
                String val = (String)expectedObj;
                assertEquals(msg, val, actualRow.getString(i));
            } else if (expectedObj instanceof TimestampType) {
                TimestampType val = (TimestampType)expectedObj;
                assertEquals(msg, val, actualRow.getTimestampAsTimestamp(i));
            } else {
                fail("Unexpected type in expected row: " + expectedObj.getClass().getSimpleName());
            }
        }
    }

    /**
     * Accept expected table contents as 2-dimensional array of objects, to make it easy to write tests.
     * Currently only handles some data types.  Feel free to add more as needed.
     */
    public static void assertContentOfTable(Object[][] expectedTable, VoltTable actualTable) {
        assertApproximateContentOfTable(expectedTable, actualTable, 0.0d);
    }

    /**
     * Assert that the expected and actual valus are approximately equal. By
     * approximately equal we mean that non-floating point values are identical,
     * and floating point values differ by at most epsilon. If epsilon is zero or negative,
     * we require equality.
     *
     * @param expectedTable
     * @param actualTable
     * @param epsilon
     */
    static void assertApproximateContentOfTable(Object[][] expectedTable, VoltTable actualTable, double epsilon) {
        for (int i = 0; i < expectedTable.length; ++i) {
            assertTrue("Fewer rows than expected: "
                    + "expected: " + expectedTable.length + ", "
                    + "actual: " + i,
                    actualTable.advanceRow());
            assertApproximateContentOfRow(i, expectedTable[i], actualTable, epsilon);
        }
        assertFalse("More rows than expected: "
                + "expected " + expectedTable.length + ", "
                + "actual: " + actualTable.getRowCount(),
                actualTable.advanceRow());
    }

    static protected void assertSuccessfulDML(Client client, String stmt) throws NoConnectionsException, IOException, ProcCallException {
        assertSuccessfulDML(client, stmt, 1L);
    }

    static protected void assertSuccessfulDML(Client client, String stmt, long returnValue) throws NoConnectionsException, IOException, ProcCallException {
        VoltTable[] results = null;
        results = client.callProcedure("@AdHoc", stmt).getResults();
        assertEquals(1, results.length);
        assertEquals(returnValue, results[0].asScalarLong());
    }

    static protected void verifyStmtFails(Client client, String stmt, String expectedPattern) throws IOException {
        verifyProcFails(client, expectedPattern, "@AdHoc", stmt);
    }

    static void verifyAdHocFails(Client client, String expectedPattern, Object... args) throws IOException {
        verifyProcFails(client, expectedPattern, "@AdHoc", args);
    }

    static public void verifyProcFails(Client client, String expectedPattern, String storedProc, Object... args) throws IOException {

        String what;
        if (storedProc.compareTo("@AdHoc") == 0) {
            what = "the statement \"" + args[0] + "\"";
        } else {
            what = "the stored procedure \"" + storedProc + "\"";
        }

        try {
            client.callProcedure(storedProc, args);
        } catch (ProcCallException pce) {
            String msg = pce.getMessage();
            String diagnostic = "Expected " + what + " to throw an exception matching the pattern \"" +
                    expectedPattern + "\", but instead it threw an exception containing \"" + msg + "\".";
            Pattern pattern = Pattern.compile(expectedPattern, Pattern.MULTILINE);
            assertTrue(diagnostic, pattern.matcher(msg).find());
            return;
        }

        String diagnostic = "Expected " + what + " to throw an exception matching the pattern \"" +
                expectedPattern + "\", but instead it threw nothing.";
        fail(diagnostic);
    }


    // ALL OF THE VALIDATION SCHEMAS IN THIS TEST ARE BASED OFF OF
    // THE VOLTDB DOCS, RATHER THAN REUSING THE CODE THAT GENERATES THEM.
    // IN SOME MAGICAL FUTURE MAYBE THEY ALL CAN BE GENERATED FROM THE
    // SAME METADATA.
    static protected void validateSchema(VoltTable result, VoltTable expected) {
        assertEquals(expected.getColumnCount(), result.getColumnCount());
        for (int i = 0; i < result.getColumnCount(); i++) {
            assertEquals("Failed name column: " + i, expected.getColumnName(i), result.getColumnName(i));
            assertEquals("Failed type column: " + i, expected.getColumnType(i), result.getColumnType(i));
        }
    }

    static protected void checkDeploymentPropertyValue(Client client, String key, String value)
            throws IOException, ProcCallException, InterruptedException {
        boolean found = false;

        VoltTable result = client.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
        while (result.advanceRow()) {
            if (result.getString("PROPERTY").equalsIgnoreCase(key)) {
                found = true;
                assertEquals(value, result.getString("VALUE"));
                break;
            }
        }
        assertTrue(found);
    }

    static protected void checkQueryPlan(Client client, String query, String...patterns)
            throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt;
        assert(patterns.length >= 1);

        vt = client.callProcedure("@Explain", query).getResults()[0];
        String vtStr = vt.toString();

        for (String pattern : patterns) {
            if (! vtStr.contains(pattern)) {
                fail("The explain plan \n" + vtStr + "\n is expected to contain pattern: " + pattern);
            }
        }
    }

    /**
     * Utility function to run queries and dump results to stdout.
     * @param client
     * @param queries one or more query strings to send in a batch
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    protected static void dumpQueryResults(Client client, String... queries)
            throws IOException, NoConnectionsException, ProcCallException {
        VoltTable vts[] = client.callProcedure("@AdHoc", StringUtils.join(queries, '\n')).getResults();
        int ii = 0;
        for (VoltTable vtn : vts) {
            System.err.println("DEBUG: result for " + queries[ii] + "\n" + vtn + "\n");
            ++ii;
        }
    }

    /**
     * Utility function to explain queries and dump results to stdout.
     * @param client
     * @param queries one or more query strings to send in a batch to @Explain.
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    protected static void dumpQueryPlans(Client client, String... queries)
            throws IOException, NoConnectionsException, ProcCallException {
        for (String query : Arrays.asList(queries)) {
            System.out.println("DEBUG: plan for " + query + "\n" + getQueryPlan('\n', client, query));
        }
    }

    protected static String getQueryPlan(char splitter, Client client, String... queries) throws IOException, ProcCallException{
        StringBuilder sb = new StringBuilder();
        for (VoltTable vtb : Arrays.asList(client.callProcedure("@Explain", StringUtils.join(queries, '\n')).getResults())) {
            sb.append(vtb.toString()).append(splitter);
        }
        return sb.toString();
    }

    protected static void truncateTables(Client client, String... tables)
            throws IOException, ProcCallException {
        for (String tb : tables) {
            truncateTable(client, tb);
        }
    }

    protected static void truncateTable(Client client, String tb)
            throws IOException, ProcCallException {
        client.callProcedure("@AdHoc", "Truncate table " + tb);
        validateTableOfScalarLongs(client, "select count(*) from " + tb, new long[]{0});
    }

    protected static void truncateAllTables(Client client) throws Exception {
        ClientResponse cr;
        cr = client.callProcedure("@SystemCatalog", "TABLES");
        assertEquals(cr.getStatus(), ClientResponse.SUCCESS);
        VoltTable vt = cr.getResults()[0];
        String allTables[] = new String[vt.getRowCount()];
        int idx = 0;
        while (vt.advanceRow()) {
            allTables[idx++] = vt.getString("TABLE_NAME");
        }
        truncateTables(client, allTables);
    }


    /**
     * Drop everything of the given kind from the database at the
     * other end if the client.
     *
     * @param client
     * @param kind
     * @throws Exception
     */
    private void dropAllTheThings(Client client, String kind) throws Exception {
        String sysCatalogKind = kind;
        if ("view".equals(kind)) {
            sysCatalogKind = "table";
        }
        ClientResponse cr = client.callProcedure("@SystemCatalog", sysCatalogKind + "s");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable funcs = cr.getResults()[0];
        while (funcs.advanceRow()) {
            String artifactName = funcs.getString(sysCatalogKind.toUpperCase() + "_NAME");
            if ( "view".equals(kind) ? kind.equalsIgnoreCase(funcs.getString("TABLE_TYPE")) : true ) {
                if ( ! artifactName.contains(".")) {
                    cr = client.callProcedure("@AdHoc", String.format("drop %s %s;", kind, artifactName));
                }
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            }
        }
    }

    /**
     * Drop all user defined functions from the database at the other end of the client.
     *
     * @param client
     * @throws Exception
     */
    private void dropAllFunctions(Client client) throws Exception {
        dropAllTheThings(client, "function");
    }

    /**
     * Drop all views from the database at the other end of the client.
     *
     * @param client
     * @throws Exception
     */
    private void dropAllViews(Client client) throws Exception {
        dropAllTheThings(client, "view");
    }

    /**
     * Drop all tables from the database at the other end of the client.  This
     * will not drop views.
     *
     * @param client
     * @throws Exception
     */
    private void dropAllTables(Client client) throws Exception {
        dropAllTheThings(client, "table");
    }

    /**
     * Drop all procedures from the database at the other end of the client.
     * This will not drop tables on which these procedures depend.
     *
     * @param client
     * @throws Exception
     */
    private void dropAllProcedures(Client client) throws Exception {
        dropAllTheThings(client, "procedure");
    }

    /**
     * Drop everything from the database at the other end of the client.  This
     * will drop things in the right order, so that no dependences will be
     * violated.
     *
     * @param client
     * @throws Exception
     */
    void dropEverything(Client client) throws Exception {
        dropAllProcedures(client);
        dropAllViews(client);
        dropAllTables(client);
        dropAllFunctions(client);
    }

    /**
     * A convenience method to build a Properties object initialized with an
     * arbitrary number of property/value pairs.
     * This one method with its scalable argument list replaces the
     * calls to the constructor and to the ugly and nonscalable
     * putAll(ImmutableMap.<String, String> of(...) and/or
     * a verbose list of calls to setProperty.
     * @param alternatingKeysAndValues property-name-1, string-value-1,
     *        property-name-2, string-value-2, ...
     * @return the new Properties object
     **/
    protected static Properties buildProperties(String... alternatingKeysAndValues) {
        Properties properties = new Properties();
        int nStrings = alternatingKeysAndValues.length;
        // Each key should have a value, so the length should be even.
        assert nStrings % 2 == 0;
        for (int ii = 0; ii < nStrings; ii += 2) {
            // Initialize each key value pair from adjacent strings.
            properties.setProperty(
                    alternatingKeysAndValues[ii],
                    alternatingKeysAndValues[ii+1]);
        }
        return properties;
    }

    /**
     * Test environment with configured schema and server.
     */
    public static class RegresssionEnv {
        final VoltProjectBuilder m_builder;
        LocalCluster m_cluster;
        public Client m_client = null;

        public RegresssionEnv(String ddlText, String pathToCatalog, String pathToDeployment,
                int siteCount, int hostCount, int kFactor, boolean debug) {

            m_builder = new VoltProjectBuilder();
            //Increase query timeout as long literal queries taking long time.
            m_builder.setQueryTimeout(60000);
            try {
                m_builder.addLiteralSchema(ddlText);

                // add more partitioned and replicated tables, PARTED[1-3] and REPED[1-2]
                AdHocQueryTester.setUpSchema(m_builder, pathToCatalog, pathToDeployment);
            } catch (Exception e) {
                e.printStackTrace();
                fail("Failed to set up schema");
            }

            m_cluster = new LocalCluster(pathToCatalog, siteCount, hostCount, kFactor,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    debug);
            m_cluster.setHasLocalServer(true);
            boolean success = m_cluster.compile(m_builder);
            assert(success);

            try {
                MiscUtils.copyFile(m_builder.getPathToDeployment(), pathToDeployment);
            } catch (Exception e) {
                fail(String.format("Failed to copy \"%s\" to \"%s\"", m_builder.getPathToDeployment(), pathToDeployment));
            }
        }

        public void setUp() {
            m_cluster.startUp();

            try {
                // do the test
                m_client = ClientFactory.createClient();
                m_client.createConnection("localhost", m_cluster.port(0));
            } catch (IOException e) {
                e.printStackTrace();
                fail(String.format("Failed to connect to localhost:%d", m_cluster.port(0)));
            }
        }

        public void tearDown() {
            if (m_client != null) {
                try {
                    m_client.close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Failed to close client");
                }
            }
            m_client = null;

            if (m_cluster != null) {
                try {
                    m_cluster.shutDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    fail("Failed to shut down cluster");
                }
            }
            m_cluster = null;

            // no clue how helpful this is
            System.gc();
        }

        public boolean isValgrind() {
            if (m_cluster != null) {
                return m_cluster.isValgrind();
            }
            return true;
        }

        public boolean isMemcheckDefined() {
            return (m_cluster != null) ? m_cluster.isMemcheckDefined() : true;
        }
    }
}
