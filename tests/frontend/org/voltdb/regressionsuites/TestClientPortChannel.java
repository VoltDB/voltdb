/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static junit.framework.Assert.assertTrue;
import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestClientPortChannel extends TestCase {

    PortConnector channel;
    int rport;
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
        rport = SecureRandom.getInstance("SHA1PRNG").nextInt(2000) + 22000;
        System.out.println("Random Client port is: " + rport);
        channel = new PortConnector("localhost", rport);
        try {
            //Build the catalog
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema("");
            String catalogJar = "dummy.jar";

            m_config = new LocalCluster(catalogJar, 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

            m_config.portGenerator.enablePortProvider();
            m_config.portGenerator.pprovider.setNextClient(rport);
            m_config.setHasLocalServer(true);

            boolean success = m_config.compile(builder);
            assertTrue(success);

            m_config.startUp();

            Thread.currentThread().sleep(10000);
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

        if (channel != null) {
            channel.close();
        }
        m_config.shutDown();
    }

    public void testClientPortChannel() throws Exception {
    }

    public void testClientPortChannelBadLoginMessage() throws Exception {
        //Just connect and disconnect
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

        System.out.println("Testing valid login message");
        channel.connect();
        buf = ByteBuffer.allocate(43);
        buf.putInt(36);
        buf.put((byte) '0');
        buf.putInt(8);
        buf.put("database".getBytes());
        buf.putInt(0);
        buf.put("".getBytes());
        buf.put(ConnectionUtil.getHashedPassword(""));

        buf.position(0);
        channel.write(buf);

        ByteBuffer resp = ByteBuffer.allocate(4);
        channel.read(buf, 4);
        int length = resp.getInt();
        resp = ByteBuffer.allocate(length);
        channel.read(buf, length);
        resp.flip();

        byte code = resp.get();
        assertEquals(code, 0);
        resp.getInt();
        resp.getLong();
        resp.getLong();
        resp.getInt();
        int buildStringLength = resp.getInt();
        byte buildStringBytes[] = new byte[buildStringLength];
        resp.get(buildStringBytes);

        System.out.println("Authenticated to server: " + buildStringBytes);
        channel.close();


    }

}
