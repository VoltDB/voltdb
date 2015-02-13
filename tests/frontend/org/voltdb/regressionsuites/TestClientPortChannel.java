/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.nio.ByteBuffer;
import java.security.SecureRandom;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestClientPortChannel extends TestCase {

    int m_clientPort;
    int m_adminPort;
    LocalCluster m_config;

    public TestClientPortChannel(String name) {
        super(name);
    }

    /**
     * JUnit special method called to setup the test. This instance will start
     * the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
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

            m_config.portGenerator.enablePortProvider();
            m_config.portGenerator.pprovider.setNextClient(m_clientPort);
            m_config.portGenerator.pprovider.setAdmin(m_adminPort);
            m_config.setHasLocalServer(false);
            boolean success = m_config.compile(builder);
            assertTrue(success);

            m_config.startUp();

            Thread.currentThread().sleep(5000);
        } catch (IOException ex) {
            fail(ex.getMessage());
        } finally {
        }
    }

    /**
     * JUnit special method called to shutdown the test. This instance will
     * stop the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void tearDown() throws Exception {
        if (m_config != null) {
            m_config.shutDown();
        }
    }

    // Just do a login
    public void login(PortConnector conn) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(41);
        buf.putInt(37);
        buf.put((byte) '0');
        buf.putInt(8);
        buf.put("database".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(""));
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

    public void testLoginMessagesClientPort() throws Exception {
        runBadLoginMessages(m_clientPort);
    }
    public void testLoginMessagesAdminPort() throws Exception {
        runBadLoginMessages(m_adminPort);
    }

    public void runBadLoginMessages(int port) throws Exception {
        //Just connect and disconnect
        PortConnector channel = new PortConnector("localhost", port);
        System.out.println("Testing Connect and Close");
        for (int i = 0; i < 100; i++) {
            channel.connect();
            channel.close();
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

        //login message with bad service
        System.out.println("Testing bad service name");
        channel.connect();
        buf = ByteBuffer.allocate(41);
        buf.putInt(37);
        buf.put((byte) '0');
        buf.putInt(8);
        buf.put("dataCase".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(""));
        buf.flip();
        channel.write(buf);

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

        //login message with bad service name length.
        System.out.println("Testing service name with invalid length");
        channel.connect();
        buf = ByteBuffer.allocate(41);
        buf.putInt(37);
        buf.put((byte) '0');
        buf.putInt(Integer.MAX_VALUE);
        buf.put("database".getBytes("UTF-8"));
        buf.putInt(0);
        buf.put("".getBytes("UTF-8"));
        buf.put(ConnectionUtil.getHashedPassword(""));
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

    }

    public void testInvocationClientPort() throws Exception {
        runInvocationMessageTest(m_clientPort);
    }
    public void testInvocationAdminPort() throws Exception {
        runInvocationMessageTest(m_adminPort);
    }

    public void runInvocationMessageTest(int port) throws Exception {
        PortConnector channel = new PortConnector("localhost", port);
        channel.connect();

        //Now start testing combinations of invocation messages.
        //Start with a good Ping procedure invocation.
        System.out.println("Testing good Ping invocation before login");
        byte pingr[] = {0, //Version
            0, 0, 0, 5,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        boolean failed = false;
        try {
            verifyInvocation(pingr, channel, (byte) 1);
        } catch (Exception ioe) {
            System.out.println("Good that we could not execute a proc.");
            failed = true;
        }
        assertTrue(failed);

        //reconnect as we should have bombed.
        channel.connect();
        //Send login message before invocation.
        login(channel);

        //Now start testing combinations of invocation messages.
        //Start with a good Ping procedure invocation.
        System.out.println("Testing good Ping invocation");
        verifyInvocation(pingr, channel, (byte) 1);

        //With bad message length of various shapes and sizes
        //Procedure name length mismatch
        System.out.println("Testing Ping invocation with bad procname length");
        byte bad_length[] = {0, //Version
            0, 0, 0, 6,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        verifyInvocation(bad_length, channel, (byte) -3);
        //Procedure name length -ve long
        System.out.println("Testing Ping invocation with -1 procname length.");
        byte neg1_length[] = {0, //Version
            0, 0, 0, 0,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        byte iarr[] = ByteBuffer.allocate(4).putInt(-1).array();
        for (int i = 0; i < 4; i++) {
            neg1_length[i + 1] = iarr[i];
        }
        verifyInvocation(neg1_length, channel, (byte) -3);

        System.out.println("Testing Ping invocation with -200 procname length.");
        byte neg2_length[] = {0, //Version
            0, 0, 0, 0,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        iarr = ByteBuffer.allocate(4).putInt(-200).array();
        for (int i = 0; i < 4; i++) {
            neg2_length[i + 1] = iarr[i];
        }
        verifyInvocation(neg2_length, channel, (byte) -3);

        //Procedure name length too long
        System.out.println("Testing Ping invocation with looooong procname length.");
        byte too_long_length[] = {0, //Version
            0, 0, 0, 0,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        iarr = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();
        for (int i = 0; i < 4; i++) {
            too_long_length[i + 1] = iarr[i];
        }
        verifyInvocation(too_long_length, channel, (byte) -3);

        //Bad protocol version
        System.out.println("Testing good Ping invocation with bad protocol version.");
        byte bad_proto[] = {1, //Version
            0, 0, 0, 5,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        verifyInvocation(bad_proto, channel, (byte) 1);

        //Client Data - Bad Data meaning invalid number of bytes.
        System.out.println("Testing good Ping invocation with incomplete client data");
        byte bad_cl_data[] = {0, //Version
            0, 0, 0, 5,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0 //Client Data
        };
        verifyInvocation(bad_cl_data, channel, (byte) -3);
        System.out.println("Testing good Ping invocation Again");
        verifyInvocation(pingr, channel, (byte) 1);
        channel.close();
    }

    public void testInvocationParamsClientPort() throws Exception {
        runInvocationParams(m_clientPort);
    }
    public void testInvocationParamsAdminPort() throws Exception {
        runInvocationParams(m_adminPort);
    }

    public void runInvocationParams(int port) throws Exception {
        PortConnector channel = new PortConnector("localhost", port);
        channel.connect();

        //Send login message before invocation.
        login(channel);

        //Now start testing combinations of invocation messages with invocation params.
        //no param
        byte i1[] = {0, //Version
            0, 0, 0, 8,
            'B', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        int pidx = i1.length - 2;
        verifyInvocation(i1, channel, (byte) -2);
        byte i2[] = {0, //Version
            0, 0, 0, 8,
            'B', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
        byte iarr[] = ByteBuffer.allocate(2).putShort(Short.MAX_VALUE).array();
        i2[pidx] = iarr[0];
        i2[pidx + 1] = iarr[1];
        verifyInvocation(i2, channel, (byte) -2);
        iarr = ByteBuffer.allocate(2).putShort((short) -1).array();
        i2[pidx] = iarr[0];
        i2[pidx + 1] = iarr[1];
        verifyInvocation(i2, channel, (byte) -2);
        //Lie about param count.
        iarr = ByteBuffer.allocate(2).putShort((short) 4).array();
        i2[pidx] = iarr[0];
        i2[pidx + 1] = iarr[1];
        verifyInvocation(i2, channel, (byte) -2);
        //Put correct param count but no values
        iarr = ByteBuffer.allocate(2).putShort((short) 9).array();
        i2[pidx] = iarr[0];
        i2[pidx + 1] = iarr[1];
        verifyInvocation(i2, channel, (byte) -2);
        //Lie length of param string.
        byte i3[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 9, //9 is string type
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i3[21] = iarr[0];
        i3[21 + 1] = iarr[1];
        verifyInvocation(i3, channel, (byte) -2);

        //Pass string length of 8 but dont pass string.
        byte i4[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 9, //9 is string type
            0, 0, 0, 0 //String length
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i4[21] = iarr[0];
        i4[21 + 1] = iarr[1];
        iarr = ByteBuffer.allocate(4).putInt(8).array();
        i4[24] = iarr[0];
        i4[24 + 1] = iarr[1];
        i4[24 + 2] = iarr[2];
        i4[24 + 3] = iarr[3];
        verifyInvocation(i4, channel, (byte) -2);

        //Pass string length of 6 and pass 6 byte string this should succeed.
        byte i5[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 9, //9 is string type
            0, 0, 0, 0, //String length
            'v', 'o', 'l', 't', 'd', 'b'
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i5[21] = iarr[0];
        i5[21 + 1] = iarr[1];
        iarr = ByteBuffer.allocate(4).putInt(6).array();
        i5[24] = iarr[0];
        i5[24 + 1] = iarr[1];
        i5[24 + 2] = iarr[2];
        i5[24 + 3] = iarr[3];
        verifyInvocation(i5, channel, (byte) 1);

        //Lie length of param  array.
       byte i6[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99 //-99 is array
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i6[21] = iarr[0];
        i6[21 + 1] = iarr[1];
        verifyInvocation(i6, channel, (byte) -2);
        //Lie length of param  array.
        byte i61[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            70, //bad element type
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i61[21] = iarr[0];
        i61[21 + 1] = iarr[1];
        verifyInvocation(i61, channel, (byte) -2);

        //Array of Array not supported should not crash server.
        byte i62[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            -99, //Array of Array
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i62[21] = iarr[0];
        i62[21 + 1] = iarr[1];
        verifyInvocation(i62, channel, (byte) -2);

        //Array of string but no data.
        byte i63[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            9, //String with no data
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i63[21] = iarr[0];
        i63[21 + 1] = iarr[1];
        verifyInvocation(i63, channel, (byte) -2);

        //Array of string with bad length
        byte i631[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            9, 0, 0, 0, 0//String with no data
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i631[21] = iarr[0];
        i631[21 + 1] = iarr[1];
        iarr = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();
        i631[i631.length - 4] = iarr[0];
        i631[i631.length - 3] = iarr[1];
        i631[i631.length - 2] = iarr[2];
        i631[i631.length - 1] = iarr[3];
        verifyInvocation(i631, channel, (byte) -2);

        //Array of long but no data.
        byte i64[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            6, //Long with no data
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i64[21] = iarr[0];
        i64[21 + 1] = iarr[1];
        verifyInvocation(i64, channel, (byte) -2);

        //Array of long with non parsable long
        byte i65[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, -99, //-99 is array
            6, //Long
            'A'
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i64[21] = iarr[0];
        i64[21 + 1] = iarr[1];
        verifyInvocation(i65, channel, (byte) -2);

        //Lie data type invaid data type.
        byte i7[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 70, //98 bad
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i7[21] = iarr[0];
        i7[21 + 1] = iarr[1];
        verifyInvocation(i7, channel, (byte) -2);

        //Lie data type invaid data type.
        byte i71[] = {0, //Version
            0, 0, 0, 8,
            'A', '.', 'i', 'n', 's', 'e', 'r', 't', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0, 26, //98 bad
        };
        iarr = ByteBuffer.allocate(2).putShort((short) 1).array();
        i71[21] = iarr[0];
        i71[21 + 1] = iarr[1];
        verifyInvocation(i71, channel, (byte) -2);

        //Test Good Ping at end to ensure server is up.
        System.out.println("Testing good Ping invocation Again");
        byte pingr[] = {0, //Version
            0, 0, 0, 5,
            '@', 'P', 'i', 'n', 'g', //proc string length and name
            0, 0, 0, 0, 0, 0, 0, 0, //Client Data
            0, 0
        };
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
        channel.read(respbuf, len - 4);
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
