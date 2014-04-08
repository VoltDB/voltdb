/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientConfigForTest;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.client.ProcCallException;
import org.voltdb.common.Constants;

import com.google_voltpatches.common.net.HostAndPort;

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

    protected VoltServerConfig m_config;
    protected String m_username = "default";
    protected String m_password = "password";
    private final ArrayList<Client> m_clients = new ArrayList<Client>();
    private final ArrayList<SocketChannel> m_clientChannels = new ArrayList<SocketChannel>();
    protected final String m_methodName;

    /**
     * Trivial constructor that passes parameter on to superclass.
     * @param name The name of the method to run as a test. (JUnit magic)
     */
    public RegressionSuite(final String name) {
        super(name);
        m_methodName = name;
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

    /**
     * JUnit special method called to shutdown the test. This instance will
     * stop the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void tearDown() throws Exception {
        m_config.shutDown();
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
     * @return Is the underlying instance of VoltDB running Valgrind with the IPC client?
     */
    public boolean isValgrind() {
        return m_config.isValgrind();
    }

    public boolean isLocalCluster() {
        return m_config instanceof LocalCluster;
    }

    /**
     * @return a reference to the associated VoltServerConfig
     */
    public final VoltServerConfig getServerConfig() {
        return m_config;
    }

    public Client getClient() throws IOException {
        return getClient(1000 * 60 * 10); // 10 minute default
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
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        String listener = listeners.get(r.nextInt(listeners.size()));
        ClientConfig config = new ClientConfigForTest(m_username, m_password);
        config.setConnectionResponseTimeout(timeout);
        config.setProcedureCallTimeout(timeout);
        final Client client = ClientFactory.createClient(config);
        // Use the port generated by LocalCluster if applicable
        try {
            client.createConnection(listener);
        }
        // retry once
        catch (ConnectException e) {
            listener = listeners.get(r.nextInt(listeners.size()));
            client.createConnection(listener);
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
            }
            // retry once
            catch (ConnectException e) {
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
    public void releaseClient(Client c) throws IOException, InterruptedException {
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
    public SocketChannel getClientChannel() throws IOException {
        return getClientChannel(false);
    }
    public SocketChannel getClientChannel(final boolean noTearDown) throws IOException {
        final List<String> listeners = m_config.getListenerAddresses();
        final Random r = new Random();
        final String listener = listeners.get(r.nextInt(listeners.size()));
        byte[] hashedPassword = ConnectionUtil.getHashedPassword(m_password);
        HostAndPort hNp = HostAndPort.fromString(listener);
        int port = Constants.DEFAULT_PORT;
        if (hNp.hasPort()) {
            port = hNp.getPort();
        }
        final SocketChannel channel = (SocketChannel)
            ConnectionUtil.getAuthenticatedConnection(
                    hNp.getHostText(), m_username, hashedPassword, port, null)[0];
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
        return isLocalCluster() ? ((LocalCluster)m_config).internalPort(hostId) : VoltDB.DEFAULT_INTERNAL_PORT+hostId;
    }

    static public void validateTableOfLongs(Client c, String sql, long[][] expected)
            throws Exception, IOException, ProcCallException {
        assertNotNull(expected);
        VoltTable vt = c.callProcedure("@AdHoc", sql).getResults()[0];
        validateTableOfLongs(vt, expected);
    }

    static public void validateTableOfScalarLongs(VoltTable vt, long[] expected) {
        assertNotNull(expected);
        assertEquals(expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            validateRowOfLongs(vt, new long[] {expected[i]});
        }
    }

    static public void validateTableOfLongs(VoltTable vt, long[][] expected) {
        assertNotNull(expected);
        assertEquals(expected.length, vt.getRowCount());
        int len = expected.length;
        for (int i=0; i < len; i++) {
            validateRowOfLongs(vt, expected[i]);
        }
    }

    static public void validateRowOfLongs(VoltTable vt, long [] expected) {
        int len = expected.length;
        assertTrue(vt.advanceRow());
        for (int i=0; i < len; i++) {
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
                            fail();
                        }
                    } catch (ArithmeticException newestEx) {
                        newestEx.printStackTrace();
                        fail();
                    }
                }
            }
            if (expected[i] != Long.MIN_VALUE) {
                assertEquals(expected[i], actual);
            } else {
                VoltType type = vt.getColumnType(i);
                assertEquals(Long.parseLong(type.getNullValue().toString()), actual);
            }
        }
    }

    // ALL OF THE VALIDATION SCHEMAS IN THIS TEST ARE BASED OFF OF
    // THE VOLTDB DOCS, RATHER THAN REUSING THE CODE THAT GENERATES THEM.
    // IN SOME MAGICAL FUTURE MAYBE THEY ALL CAN BE GENERATED FROM THE
    // SAME METADATA.
    static public void validateSchema(VoltTable result, VoltTable expected)
    {
        assertEquals(expected.getColumnCount(), result.getColumnCount());
        for (int i = 0; i < result.getColumnCount(); i++) {
            assertEquals("Failed name column: " + i, expected.getColumnName(i), result.getColumnName(i));
            assertEquals("Failed type column: " + i, expected.getColumnType(i), result.getColumnType(i));
        }
    }

    static public void validStatisticsForTableLimit(Client client, String tableName, long limit) throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 10000) fail("Took too long");

            VoltTable[] results = client.callProcedure("@Statistics", "TABLE", 0).getResults();
            for (VoltTable t: results) { System.out.println(t.toString()); }
            if (results[0].getRowCount() == 0) continue;

            for (VoltTable vt: results) {
                while(vt.advanceRow()) {
                    String name = vt.getString("TABLE_NAME");
                    if (tableName.equals(name)) {
                        assertEquals(limit, vt.getLong("TUPLE_LIMIT"));
                        return;
                    }
                }
            }
        }
    }

    static public void validStatisticsForTableLimitAndPercentage(Client client, String tableName, long limit, long percentage) throws Exception {
        long start = System.currentTimeMillis();
        while (true) {
            Thread.sleep(1000);
            if (System.currentTimeMillis() - start > 10000) fail("Took too long");

            VoltTable[] results = client.callProcedure("@Statistics", "TABLE", 0).getResults();
            for (VoltTable t: results) { System.out.println(t.toString()); }
            if (results[0].getRowCount() == 0) continue;

            for (VoltTable vt: results) {
                while(vt.advanceRow()) {
                    String name = vt.getString("TABLE_NAME");
                    if (tableName.equals(name)) {
                        assertEquals(limit, vt.getLong("TUPLE_LIMIT"));
                        assertEquals(percentage, vt.getLong("PERCENT_FULL"));
                        return;
                    }
                }
            }
        }
    }
}
