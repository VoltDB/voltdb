/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 */

package org.voltdb;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.utils.MiscUtils;

/*
 * Harass a bunch of VoltDB ports with garbage data for a several seconds, see if they stop accepting connections
 */
public class TestPortPirate {

    volatile boolean shouldContinue = true;
    volatile Throwable pillageError = null;
    volatile Long errorSeed;
    volatile String errorPillager;
    AtomicLong pillages = new AtomicLong();
    List<PortPillager> pillagers = new ArrayList<PortPillager>();
    ServerThread m_localServer = null;
    VoltDB.Configuration m_config = null;


    static AtomicLong pillagerCounter = new AtomicLong();
    public class PortPillager extends Thread {
        final int port;
        public PortPillager(int port) {
            super("Port Pillager - " + pillagerCounter.getAndIncrement());
            this.port = port;
        }

        @Override
        public void run() {
            long seed = System.nanoTime();
            try {

                pillagePort(port, seed);
            } catch (Throwable t) {
                pillageError = t;
                errorSeed = seed;
                errorPillager = getName();
            }
        }
    }

    /*
     * Connect and send a garbage message with a variable size prefix that is filled with a specific random byte
     * followed by a payload of random bytes
     */
    private void pillagePort(int port, long seed) throws Throwable {
        final Random r = new Random(seed);

        while (shouldContinue) {
            SocketChannel sc;
            try {
                sc = SocketChannel.open(new InetSocketAddress(InetAddress.getByName("localhost"), port));
            } catch (Throwable t) {
                throw new RuntimeException("Failed to connect to port " + port, t);
            }
            final int leadingFill = r.nextInt(16);
            final byte fillByte[] = new byte[1];
            r.nextBytes(fillByte);
            try {
                ByteBuffer buf = ByteBuffer.allocate(64);
                for (int ii = 0; ii < leadingFill; ii++) {
                    buf.put(fillByte);
                }

                byte nextBytes[] = new byte[buf.remaining()];
                r.nextBytes(nextBytes);
                buf.put(nextBytes);

                buf.flip();

                while (buf.hasRemaining()) {
                    sc.write(buf);
                }
            } finally {
                sc.close();
            }
            pillages.incrementAndGet();
        }
    }

    @Before
    public void setUp() throws Exception
    {
        pillageError = null;
        m_config = AdHocQueryTester.setUpSPDB();
        m_config.m_httpPort = 8080;
        m_localServer = new ServerThread(m_config);
        m_localServer.start();
        m_localServer.waitForInitialization();
    }

    @After
    public void tearDown() throws Exception {
        shouldContinue = false;
        for (PortPillager pp : pillagers) {
            pp.join();
        }
        pillagers.clear();
        shouldContinue = true;
        checkPillageError();
        m_localServer.shutdown();
    }

    void startPillaging() {
        for (PortPillager pp : pillagers) {
            pp.start();
        }
    }

    private void checkPillageError() {
        if (pillageError != null) {
            throw new RuntimeException("Seed for error " + errorSeed + " pillager " + errorPillager, pillageError);
        }
    }

    @Test
    public void testPillage() throws Exception {
        pillagers.add(new PortPillager(MiscUtils.getPortFromHostnameColonPort(m_config.m_zkInterface, 2181)));
        pillagers.add(new PortPillager(m_config.m_port));
        pillagers.add(new PortPillager(9090));
        pillagers.add(new PortPillager(m_config.m_internalPort));
        pillagers.add(new PortPillager(8080));
        startPillaging();



        for (int ii = 0; ii < 10; ii++) {
            Thread.sleep(1000);
            checkPillageError();

        }
        checkPillageError();
        System.out.println("Pillaged " + pillages.get() + " times");
    }
}
