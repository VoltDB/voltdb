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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.client.ClientAuthScheme;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestClientPortChannel extends JUnit4LocalClusterTest {

    int m_clientPort;
    int m_adminPort;
    LocalCluster m_config;

    public TestClientPortChannel() {
    }

    /**
     * JUnit special method called to setup the test. This instance will start
     * the VoltDB server using the VoltServerConfig instance provided.
     */
    @Before
    public void setUp() throws Exception {
        m_clientPort = SecureRandom.getInstance("SHA1PRNG").nextInt(2000) + 22000;
        m_adminPort = m_clientPort + 1;
        System.out.println("Random Client port is: " + m_clientPort);
        try {
            //Build the catalog
            VoltProjectBuilder builder = new VoltProjectBuilder();
            String mySchema
                    = "create table A ("
                    + "s varchar(20) default null, "
                    + "); "
                    + "create table B ("
                    + "clm_integer integer default 0 not null, "
                    + "clm_tinyint tinyint default 0, "
                    + "clm_smallint smallint default 0, "
                    + "clm_bigint bigint default 0, "
                    + "clm_string varchar(20) default null, "
                    + "clm_decimal decimal default null, "
                    + "clm_float float default null, "
                    + "clm_timestamp timestamp default null, "
                    + "clm_varinary varbinary(20) default null"
                    + "); ";
            builder.addLiteralSchema(mySchema);
            String catalogJar = "dummy.jar";

            m_config = new LocalCluster(catalogJar, 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
            m_config.setHasLocalServer(true);
            m_config.portGenerator.enablePortProvider();
            m_config.portGenerator.pprovider.setNextClient(m_clientPort);
            m_config.portGenerator.pprovider.setAdmin(m_adminPort);
            m_config.setHasLocalServer(false);
            boolean success = m_config.compile(builder);
            assertTrue(success);

            m_config.startUp();
        } catch (IOException ex) {
            fail(ex.getMessage());
        } finally {
        }
    }

    /**
     * JUnit special method called to shutdown the test. This instance will
     * stop the VoltDB server using the VoltServerConfig instance provided.
     */
    @After
    public void tearDown() throws Exception {
        if (m_config != null) {
            m_config.shutDown();
        }
    }

    // Just do a login
    public void login(PortConnector conn) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(41);
        buf.putInt(37);
        buf.put((byte) 0);
        buf.putInt(8);
        buf.put("database".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(ClientAuthScheme.HASH_SHA1, ""));
        buf.flip();
        conn.write(buf);

        ByteBuffer resp = ByteBuffer.allocate(4);
        conn.read(resp, 4);
        resp.flip();
        int length = resp.getInt();
        resp = ByteBuffer.allocate(length);
        conn.read(resp, length);
        resp.flip();

        byte code = resp.get();
        assertEquals(code, 0);
        byte rcode = resp.get();
        assertEquals(rcode, 0);
        resp.getInt();
        resp.getLong();
        resp.getLong();
        resp.getInt();
        int buildStringLength = resp.getInt();
        byte buildStringBytes[] = new byte[buildStringLength];
        resp.get(buildStringBytes);

        System.out.println("Authenticated to server: " + new String(buildStringBytes, "UTF-8"));
    }

    // Just do a login
    public void loginSha2(PortConnector conn) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(54);
        buf.putInt(50);
        buf.put((byte) 1);
        buf.put((byte )ClientAuthScheme.HASH_SHA256.getValue()); // Add scheme
        buf.putInt(8);
        buf.put("database".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(ClientAuthScheme.HASH_SHA256, ""));
        buf.flip();
        conn.write(buf);

        ByteBuffer resp = ByteBuffer.allocate(4);
        conn.read(resp, 4);
        resp.flip();
        int length = resp.getInt();
        resp = ByteBuffer.allocate(length);
        conn.read(resp, length);
        resp.flip();

        byte code = resp.get();
        assertEquals(code, 0);
        byte rcode = resp.get();
        assertEquals(rcode, 0);
        resp.getInt();
        resp.getLong();
        resp.getLong();
        resp.getInt();
        int buildStringLength = resp.getInt();
        byte buildStringBytes[] = new byte[buildStringLength];
        resp.get(buildStringBytes);

        System.out.println("Authenticated to server: " + new String(buildStringBytes, "UTF-8"));
    }

    //Login with new connection and close
    public void doLoginAndClose(int port) throws Exception {
        System.out.println("Testing valid login message");
        PortConnector channel = new PortConnector("localhost", port);
        channel.connect();
        login(channel);
        channel.close();
    }

    //Login with new connection and close
    public void doLoginSha2AndClose(int port) throws Exception {
        System.out.println("Testing valid login message");
        PortConnector channel = new PortConnector("localhost", port);
        channel.connect();
        loginSha2(channel);
        channel.close();
    }

    @Test
    public void testLoginMessagesClientPort() throws Exception {
        runBadLoginMessages(m_clientPort);
    }

    @Test
    public void testLoginMessagesAdminPort() throws Exception {
        runBadLoginMessages(m_adminPort);
    }

    public void runBadLoginMessages(int port) throws Exception {
        //Just connect and disconnect
        PortConnector channel = new PortConnector("localhost", port);
        System.out.println("Testing Connect and Close");
        int scnt = 0;
        int fcnt = 0;
        for (int i = 0; i < 100; i++) {
            try {
                channel.connect();
                channel.close();
                scnt++;
            } catch (Exception ex) {
                ex.printStackTrace();
                fcnt++;
            }
            System.out.println("Success: " + scnt + " Failed: " + fcnt);
        }

        //Bad +ve length
        System.out.println("Testing bad login message");
        channel.connect();
        ByteBuffer buf = ByteBuffer.allocate(Integer.SIZE);
        buf.putInt(10);
        buf.position(0);
        channel.write(buf);
        channel.close();

        //Bad -ve length.
        System.out.println("Testing negative length of message");
        channel.connect();
        buf = ByteBuffer.allocate(Integer.SIZE);
        buf.putInt(-1);
        buf.position(0);
        channel.write(buf);
        channel.close();

        //Bad 0 length.
        System.out.println("Testing zero length of message");
        channel.connect();
        buf = ByteBuffer.allocate(Integer.SIZE);
        buf.putInt(0);
        buf.position(0);
        channel.write(buf);
        channel.close();

        //too big length
        System.out.println("Testing too big length of message");
        channel.connect();
        buf = ByteBuffer.allocate(Integer.SIZE);
        buf.putInt(Integer.MAX_VALUE);
        buf.position(0);
        channel.write(buf);
        channel.close();

        //login message with bad version
        System.out.println("Testing bad service name");
        channel.connect();
        buf = ByteBuffer.allocate(42);
        buf.putInt(38);
        buf.put((byte) '0');
        buf.put((byte) ClientAuthScheme.HASH_SHA1.getValue());
        buf.putInt(8);
        buf.put("dataCase".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(ClientAuthScheme.HASH_SHA1, ""));
        buf.flip();
        channel.write(buf);
        //Now this will fail because bad version will be read.
        try {
            ByteBuffer resp = ByteBuffer.allocate(6);
            channel.read(resp, 6);
            resp.flip();
            resp.getInt();
            resp.get();
            byte code = resp.get();
            assertEquals(5, code);
        } catch (Exception ioex) {
            //Should not get called; we get a legit response.
            fail();
        }

        //login message with bad version
        System.out.println("Testing bad scheme name");
        channel.connect();
        buf = ByteBuffer.allocate(42);
        buf.putInt(38);
        buf.put((byte) 1);
        buf.put((byte) 3);
        buf.putInt(8);
        buf.put("database".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(ClientAuthScheme.HASH_SHA1, ""));
        buf.flip();
        channel.write(buf);
        //Now this will fail because bad version will be read.
        try {
            ByteBuffer resp = ByteBuffer.allocate(6);
            channel.read(resp, 6);
            resp.flip();
            resp.getInt();
            resp.get();
            byte code = resp.get();
            assertEquals(3, code);
        } catch (Exception ioex) {
            //Should not get called; we get a legit response.
            fail();
        }

        //login message with bad service name length.
        System.out.println("Testing service name with invalid length");
        channel.connect();
        buf = ByteBuffer.allocate(42);
        buf.putInt(38);
        buf.put((byte) '0');
        buf.put((byte) ClientAuthScheme.HASH_SHA1.getValue());
        buf.putInt(Integer.MAX_VALUE);
        buf.put("database".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(ClientAuthScheme.HASH_SHA1, ""));
        buf.flip();
        channel.write(buf);

        boolean mustfail = false;
        try {
            ByteBuffer resp = ByteBuffer.allocate(6);
            channel.read(resp, 6);
            resp.flip();
        } catch (Exception ioex) {
            //Should not get called; we get a legit response.
            mustfail = true;
        }
        assertTrue(mustfail);

        channel.close();

        //Make sure server is up and we can login/connect after all the beating it took.
        doLoginAndClose(port);
        doLoginSha2AndClose(port);

    }

    @Test
    public void testInvocationClientPort() throws Exception {
        runInvocationMessageTest(ClientAuthScheme.HASH_SHA1, m_clientPort);
        runInvocationMessageTest(ClientAuthScheme.HASH_SHA256, m_clientPort);
    }

    @Test
    public void testInvocationAdminPort() throws Exception {
        runInvocationMessageTest(ClientAuthScheme.HASH_SHA1, m_adminPort);
        runInvocationMessageTest(ClientAuthScheme.HASH_SHA256, m_adminPort);
    }

    final int iVERSION = 0;
    final int iLENGH = 1;
    final byte VAR1[] = {
        0,                       // Version (1 byte)
        0, 0, 0, 5,              // procedure name string length (4 byte int)
        '@', 'P', 'i', 'n', 'g', // procedure name
        0, 0, 0, 0, 0, 0, 0, 0,  // Client Data (8 byte long)
        0,                       // Fields byte
        0                        // Status byte
    };

    private void updateLength(byte[] arr, int length) {
        byte iarr[] = ByteBuffer.allocate(4).putInt(length).array();
        for (int i = 0; i < 4; i++) {
            arr[i + iLENGH] = iarr[i];
        }
    }

    public void runInvocationMessageTest(ClientAuthScheme scheme, int port) throws Exception {
        PortConnector channel = new PortConnector("localhost", port);
        channel.connect();

        //Now start testing combinations of invocation messages.
        //Start with a good Ping procedure invocation.
        System.out.println("Testing good Ping invocation before login");

        byte pingr[] = VAR1.clone();
        try {
            verifyInvocation(pingr, channel, (byte) 1);
            fail("Expect exception");
        } catch (Exception ioe) {
            System.out.println("Good that we could not execute a proc.");
        }

        //reconnect as we should have bombed.
        channel.connect();
        //Send login message before invocation.
        if (scheme == ClientAuthScheme.HASH_SHA1)
            login(channel);
        else
            loginSha2(channel);

        //Now start testing combinations of invocation messages.
        //Start with a good Ping procedure invocation.
        System.out.println("Testing good Ping invocation");
        verifyInvocation(pingr, channel, (byte) 1);

        final byte ERROR_CODE = -3;

        //With bad message length of various shapes and sizes
        //Procedure name length mismatch
        System.out.println("Testing Ping invocation with bad procname length");
        byte bad_length[] = VAR1.clone();
        updateLength(bad_length, 6);
        verifyInvocation(bad_length, channel, ERROR_CODE);

        //Procedure name length -ve long
        System.out.println("Testing Ping invocation with -1 procname length.");
        byte neg1_length[] = VAR1.clone();
        updateLength(neg1_length, -1);
        verifyInvocation(neg1_length, channel, ERROR_CODE);

        System.out.println("Testing Ping invocation with -200 procname length.");
        byte neg2_length[] = VAR1.clone();
        updateLength(neg2_length, -200);
        verifyInvocation(neg2_length, channel, ERROR_CODE);

        //Procedure name length too long
        System.out.println("Testing Ping invocation with looooong procname length.");
        byte too_long_length[] = VAR1.clone();
        updateLength(too_long_length, Integer.MAX_VALUE);
        verifyInvocation(too_long_length, channel, ERROR_CODE);

        //Bad protocol version
        System.out.println("Testing good Ping invocation with bad protocol version.");
        byte bad_proto[] = VAR1.clone();
        bad_proto[iVERSION] = (byte) (StoredProcedureInvocation.CURRENT_MOST_RECENT_VERSION + 1);
        verifyInvocation(bad_proto, channel, ERROR_CODE);

        //Client Data - Bad Data meaning invalid number of bytes.
        System.out.println("Testing good Ping invocation with incomplete client data");
        byte bad_cl_data[] = Arrays.copyOfRange(VAR1, 0, 12);
        verifyInvocation(bad_cl_data, channel, ERROR_CODE);

        System.out.println("Testing good Ping invocation Again");
        verifyInvocation(pingr, channel, (byte) 1);
        channel.close();
    }

    @Test
    public void testInvocationParamsClientPort() throws Exception {
        runInvocationParams(ClientAuthScheme.HASH_SHA1, m_clientPort);
        runInvocationParams(ClientAuthScheme.HASH_SHA256, m_clientPort);
    }

    @Test
    public void testInvocationParamsAdminPort() throws Exception {
        runInvocationParams(ClientAuthScheme.HASH_SHA1, m_adminPort);
        runInvocationParams(ClientAuthScheme.HASH_SHA256, m_adminPort);
    }

    final byte VAR2[] = {
        0,                       // Version (1 byte)
        0, 0, 0, 8,              // procedure name string length (4 byte int)
        'B', '.', 'i', 'n', 's', 'e', 'r', 't', // procedure name
        0, 0, 0, 0, 0, 0, 0, 0,  // Client Data (8 byte long)
        0,                       // Fields byte
        0                        // Status byte
    };
    final int PIDX = VAR2.length - 2;

    private void updateShortBytes(byte[] arr, short num) {
        byte[] iarr = ByteBuffer.allocate(2).putShort(num).array();
        arr[PIDX] = iarr[0];
        arr[PIDX + 1] = iarr[1];
    }

    private void updateIntBytes(byte[] arr, int num, int ith) {
        byte[] iarr = ByteBuffer.allocate(4).putInt(num).array();
        for (int i = 0; i < 4; i++) {
            arr[ith + i] = iarr[i];
        }
    }

    public void runInvocationParams(ClientAuthScheme scheme, int port) throws Exception {
        PortConnector channel = new PortConnector("localhost", port);
        channel.connect();

        //Send login message before invocation.
        if (scheme == ClientAuthScheme.HASH_SHA1)
            login(channel);
        else
            loginSha2(channel);

        //Now start testing combinations of invocation messages with invocation params.
        final byte ERROR_CODE = (byte) -2;
        //
        //no param
        //
        byte i1[] = VAR2.clone();
        verifyInvocation(i1, channel, ERROR_CODE);

        byte i2[] = VAR2.clone();
        updateShortBytes(i2, Short.MAX_VALUE);
        verifyInvocation(i2, channel, ERROR_CODE);

        updateShortBytes(i2, (short) -1);
        verifyInvocation(i2, channel, ERROR_CODE);

        //Lie about param count.
        updateShortBytes(i2, (short) 4);
        verifyInvocation(i2, channel, ERROR_CODE);

        //Put correct param count but no values
        updateShortBytes(i2, (short) 9);
        verifyInvocation(i2, channel, ERROR_CODE);

        //Lie length of param string.
        byte i3[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 9, //9 is string type
        };
        updateShortBytes(i3, (short) 1);
        verifyInvocation(i3, channel, ERROR_CODE);

        //Pass string length of 8 but dont pass string.
        byte i4[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 9, //9 is string type
            0, 0, 0, 0 //String length
        };
        updateShortBytes(i4, (short) 1);
        updateIntBytes(i4, 8, 24);
        verifyInvocation(i4, channel, ERROR_CODE);

        //Pass string length of 6 and pass 6 byte string this should succeed.
        byte i5[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 9, //9 is string type
            0, 0, 0, 0, //String length
            'v', 'o', 'l', 't', 'd', 'b'
        };
        updateShortBytes(i5, (short) 1);
        updateIntBytes(i5, 6, 24);
        verifyInvocation(i5, channel, (byte) 1);

        //Lie length of param  array.
        byte i6[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99 //-99 is array
        };
        updateShortBytes(i6, (short) 1);
        verifyInvocation(i6, channel, ERROR_CODE);

        //Lie length of param  array.
        byte i61[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            70, //bad element type
        };
        updateShortBytes(i61, (short) 1);
        verifyInvocation(i61, channel, ERROR_CODE);

        //Array of Array not supported should not crash server.
        byte i62[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            -99, //Array of Array
        };
        updateShortBytes(i62, (short) 1);
        verifyInvocation(i62, channel, ERROR_CODE);

        //Array of string but no data.
        byte i63[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            9, //String with no data
        };
        updateShortBytes(i63, (short) 1);
        verifyInvocation(i63, channel, ERROR_CODE);

        //Array of string with bad length
        byte i631[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            9, 0, 0, 0, 0//String with no data
        };
        updateShortBytes(i631, (short) 1);
        updateIntBytes(i631, Integer.MAX_VALUE, i631.length - 4);
        verifyInvocation(i631, channel, ERROR_CODE);

        //Array of long but no data.
        byte i64[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            6, //Long with no data
        };
        updateShortBytes(i64, (short) 1);
        verifyInvocation(i64, channel, ERROR_CODE);

        //Array of long with non parsable long
        byte i65[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            6, //Long
            'A'
        };
        updateShortBytes(i65, (short) 1);
        verifyInvocation(i65, channel, ERROR_CODE);

        //Lie data type invaid data type.
        byte i7[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 70, //98 bad
        };
        updateShortBytes(i7, (short) 1);
        verifyInvocation(i7, channel, ERROR_CODE);

        //Lie data type invaid data type.
        byte i71[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 26, //98 bad
        };
        updateShortBytes(i71, (short) 1);
        verifyInvocation(i71, channel, ERROR_CODE);

        //Test Good Ping at end to ensure server is up.
        System.out.println("Testing good Ping invocation Again");
        byte pingr[] = VAR1.clone();
        verifyInvocation(pingr, channel, (byte) 1);

        //Test insert again
        verifyInvocation(i5, channel, (byte) 1);
        channel.close();
    }

    private ByteBuffer verifyInvocation(byte[] in, PortConnector channel, byte expected_status) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(in.length + 4);
        buf.putInt(in.length);
        buf.put(in);
        buf.flip();
        channel.write(buf);

        ByteBuffer lenbuf = ByteBuffer.allocate(4);
        channel.read(lenbuf, 4);
        lenbuf.flip();
        int len = lenbuf.getInt();
        System.out.println("Response length is: " + len);

        ByteBuffer respbuf = ByteBuffer.allocate(len);
        channel.read(respbuf, len);
        respbuf.flip();
        System.out.println("Version is: " + respbuf.get());

        //client handle data
        long handle = respbuf.getLong();
        System.out.println("Client Data is: " + handle);
        //fields present and status code.

        byte fp = respbuf.get();
        System.out.println("Fields Present is: " + fp);
        byte status = respbuf.get();
        System.out.println("Status is: " + status);
        assertEquals(expected_status, status);

        len = respbuf.getInt();
        System.out.println("Status length is: " + len);
        if (len > 0) {
            byte statusb[] = new byte[len];
            respbuf.get(statusb);
            System.out.println("Status is: " + new String(statusb, "UTF-8"));
        }

        return respbuf;
    }
}
