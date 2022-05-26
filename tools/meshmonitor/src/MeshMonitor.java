/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.Histogram;
import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.net.HostAndPort;

public class MeshMonitor {

    public static class Monitor {
        private final Object receiveLock = new Object();
        private Histogram m_receiveHistogram = new Histogram(24 * 60 * 60 * 1000, 3);
        private final Object sendLock = new Object();
        private Histogram m_sendHistogram = new Histogram(24 * 60 * 60 * 1000, 3);
        private final Object deltaLock = new Object();
        private Histogram m_deltaHistogram = new Histogram(24 * 60 * 60 * 1000, 3);
        private final SocketChannel m_sc;
        private int keptmin = new Date().getMinutes();
        private int keptsec = new Date().getSeconds();

        private boolean firstrunconnect = true;

        public Monitor(SocketChannel sc) {
            m_sc = sc;
            new Thread(sc.socket().getRemoteSocketAddress() + " send thread") {
                @Override
                public void run() {
                    long lastRuntime = System.currentTimeMillis();
                    while (true) {
                        try {
                            Thread.sleep(5);
                            long now = System.currentTimeMillis();
                            long valueToRecord = now - lastRuntime;
                            if (valueToRecord > m_sendHistogram.getHighestTrackableValue()
                                    || valueToRecord < 0) {
                                System.err.println(new Date() + " ERROR: Delta betweens sends was " + valueToRecord);
                            } else {
                                synchronized (sendLock) {
                                    now = System.currentTimeMillis();
                                    m_sendHistogram.recordValue(valueToRecord, 5);
                                }
                            }
                            lastRuntime = now;
                            ByteBuffer sendBuf = ByteBuffer.allocate(8);
                            sendBuf.putLong(now);
                            sendBuf.flip();
                            while (sendBuf.hasRemaining()) {
                                m_sc.write(sendBuf);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }
            }.start();
            new Thread(sc.socket().getRemoteSocketAddress() + " receive thread") {
                @Override
                public void run() {
                    try {
                        long lastRecvTime = System.currentTimeMillis();
                        while (true) {
                            ByteBuffer recvBuf = ByteBuffer.allocate(8);
                            while (recvBuf.hasRemaining()) {
                                m_sc.read(recvBuf);
                            }
                            recvBuf.flip();
                            long sentTime = recvBuf.getLong();
                            long now = System.currentTimeMillis();
                            long valueToRecord = now - lastRecvTime;
                            if (valueToRecord > m_receiveHistogram.getHighestTrackableValue()
                                    || valueToRecord < 0) {
                                System.err.println(new Date() + " ERROR: Delta between receives was " + valueToRecord);
                            } else {
                                synchronized (receiveLock) {
                                    m_receiveHistogram.recordValue(valueToRecord, 5);
                                }
                            }
                            lastRecvTime = now;
                            //Abs because clocks can be slightly out of sync...
                            valueToRecord = Math.abs(now - sentTime);
                            if (valueToRecord > m_deltaHistogram.getHighestTrackableValue()) {
                                System.err.println(new Date() + " ERROR: Delta between remote send time and recorded receive time was " + valueToRecord);
                            } else {
                                synchronized (deltaLock) {
                                    m_deltaHistogram.recordValue(valueToRecord, 5);
                                }
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }.start();
        }

        public void printResults(int minHiccupSize) throws Exception {
        	SimpleDateFormat formatUTC = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    		formatUTC.setTimeZone(TimeZone.getTimeZone("UTC"));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);
            if ( ( keptmin == new Date().getMinutes() && keptsec == new Date().getSeconds() ) || firstrunconnect ) {
		        System.out.printf( formatUTC.format( new Date()) + " %-22s %-33s "
	        		+ " connected to remote endpoint " + m_sc.socket().getRemoteSocketAddress() + "\n"
	        		, m_sc.socket().getLocalSocketAddress(), m_sc.socket().getRemoteSocketAddress()
	        	);
		        if (firstrunconnect) {
		        	firstrunconnect = false;
		        }
            }

            boolean haveOutliers = false;

            synchronized (receiveLock) {
                if (m_receiveHistogram.getMaxValue() > minHiccupSize) {
                    haveOutliers = true;
                    ps.printf( formatUTC.format(new Date()) + " %-22s %-33s  delta receive      - MaxLat: %4d Avg: %6.2f 99th-Pct: %3d %3d %3d %3d \n"
                    	, m_sc.socket().getLocalSocketAddress(), m_sc.socket().getRemoteSocketAddress()
                    	, m_receiveHistogram.getMaxValue(), m_receiveHistogram.getMean()
                    	, m_receiveHistogram.getValueAtPercentile(99.0), m_receiveHistogram.getValueAtPercentile(99.9)
                    	, m_receiveHistogram.getValueAtPercentile(99.99), m_receiveHistogram.getValueAtPercentile(99.999)
                    );
                }
                m_receiveHistogram = new Histogram(24 * 60 * 60 * 1000, 3);
            }
            synchronized (deltaLock) {
                if (m_deltaHistogram.getMaxValue() > minHiccupSize) {
                    haveOutliers = true;
                    ps.printf( formatUTC.format(new Date()) + " %-22s %-33s  delta timestamp    - MaxLat: %4d Avg: %6.2f 99th-Pct: %3d %3d %3d %3d \n"
                    	, m_sc.socket().getLocalSocketAddress(), m_sc.socket().getRemoteSocketAddress()
                    	, m_deltaHistogram.getMaxValue(), m_deltaHistogram.getMean()
                    	, m_deltaHistogram.getValueAtPercentile(99.0), m_deltaHistogram.getValueAtPercentile(99.9)
                    	, m_deltaHistogram.getValueAtPercentile(99.99), m_deltaHistogram.getValueAtPercentile(99.999)
                    );
                }
                m_deltaHistogram = new Histogram(24 * 60 * 60 * 1000, 3);
            }
            synchronized(sendLock) {
                if (m_sendHistogram.getMaxValue() > minHiccupSize) {
                    haveOutliers = true;
                    ps.printf( formatUTC.format(new Date()) + " %-22s %-33s  delta send         - MaxLat: %4d Avg: %6.2f 99th-Pct: %3d %3d %3d %3d \n"
                    	, m_sc.socket().getLocalSocketAddress(), m_sc.socket().getRemoteSocketAddress()
                    	, m_sendHistogram.getMaxValue(), m_sendHistogram.getMean()
                    	, m_sendHistogram.getValueAtPercentile(99.0), m_sendHistogram.getValueAtPercentile(99.9)
                    	, m_sendHistogram.getValueAtPercentile(99.99), m_sendHistogram.getValueAtPercentile(99.999)
                    );
                }
                m_sendHistogram = new Histogram(24 * 60 * 60 * 1000, 3);
            }
            ps.flush();
            if (haveOutliers) {
                System.out.print(new String(baos.toByteArray(), Charsets.UTF_8));
            } else {
            	System.out.printf( formatUTC.format(new Date()) + " %-22s %-33s "
            			+ " Threshold not reached: " + minHiccupSize + "ms; nothing to report \n"
            			, m_sc.socket().getLocalSocketAddress(), m_sc.socket().getRemoteSocketAddress()
            	);
            }
        }
    }

	public static void printUsageAndExit() {
		int minutes = new Date().getMinutes();
        System.out.println("args: minHiccupSize reportInterval [interface]:port host1 host2... hostN ");
        System.exit(-1);
    }

    private static final ScheduledExecutorService m_ses = Executors.newSingleThreadScheduledExecutor();

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ServerSocketChannel ssc = ServerSocketChannel.open();
        if (args.length == 0) {
            printUsageAndExit();
        }
        final int minHiccupSize = Integer.parseInt(args[0]);
        final int reportInterval = Integer.parseInt(args[1]);
        HostAndPort bindAddress = HostAndPort.fromString(args[2]);
        InetSocketAddress address = new InetSocketAddress(bindAddress.getHostText(), bindAddress.getPort());
        ssc.socket().bind(address);

        for (int ii = 3; ii < args.length; ii++) {
            HostAndPort connectStuff = HostAndPort.fromString(args[ii]);
            InetSocketAddress connectAddress =
                    new InetSocketAddress(connectStuff.getHostText(), connectStuff.getPort());
            SocketChannel sc = SocketChannel.open(connectAddress);
            final Monitor m = new Monitor(sc);
            m_ses.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                    	m.printResults(minHiccupSize);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, reportInterval, reportInterval, TimeUnit.SECONDS);
        }

        while (true) {
            SocketChannel sc = ssc.accept();
            final Monitor m = new Monitor(sc);
            m_ses.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                    	m.printResults(minHiccupSize);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }, reportInterval, reportInterval, TimeUnit.SECONDS);
        }
    }

}
