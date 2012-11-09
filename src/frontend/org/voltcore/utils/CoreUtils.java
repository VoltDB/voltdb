/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltcore.utils;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;

import jsr166y.LinkedTransferQueue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class CoreUtils {
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    /**
     * Create an unbounded single threaded executor
     */
    public static ListeningExecutorService getSingleThreadExecutor(String name) {
        ExecutorService ste = Executors.newSingleThreadExecutor(CoreUtils.getThreadFactory(name));
        return MoreExecutors.listeningDecorator(ste);
    }

    /**
     * Create a bounded single threaded executor that rejects requests if more than capacity
     * requests are outstanding.
     */
    public static ListeningExecutorService getBoundedSingleThreadExecutor(String name, int capacity) {
        LinkedBlockingQueue<Runnable> lbq = new LinkedBlockingQueue<Runnable>(capacity);
        ThreadPoolExecutor tpe =
                new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, lbq, CoreUtils.getThreadFactory(name));
        return MoreExecutors.listeningDecorator(tpe);
    }

    /*
     * Have shutdown actually means shutdown. Tasks that need to complete should use
     * futures.
     */
    public static ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor(String name, int poolSize, int stackSize) {
        ScheduledThreadPoolExecutor ses = new ScheduledThreadPoolExecutor(poolSize, getThreadFactory(name));
        ses.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        ses.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        return ses;
    }

    public static ListeningExecutorService getListeningExecutorService(final String name, final int threads) {
        if (threads < 1) {
            throw new IllegalArgumentException("Must specify > 0 threads");
        }
        if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
        }
        return MoreExecutors.listeningDecorator(
                new ThreadPoolExecutor(threads, threads,
                        0L, TimeUnit.MILLISECONDS,
                        new LinkedTransferQueue<Runnable>(),
                        new ThreadFactory() {
                            private int threadIndex = 0;
                            @Override
                            public synchronized Thread  newThread(Runnable r) {
                                String nameToUse = threads == 1 ? name : name + " - " + threadIndex++;
                                Thread t = new Thread(null, r, nameToUse, 131072);
                                t.setDaemon(true);
                                return t;
                            }
                        }));
    }

    public static ThreadFactory getThreadFactory(String name) {
        return getThreadFactory(name, 131072);
    }

    public static ThreadFactory getThreadFactory(String groupName, String name) {
        return getThreadFactory(groupName, name, 131072);
    }

    public static ThreadFactory getThreadFactory(String name, int stackSize) {
        return getThreadFactory(null, name, stackSize);
    }

    /**
     * Creates a thread factory that creates threads within a thread group if
     * the group name is given. The threads created will catch any unhandled
     * exceptions and log them to the HOST logger.
     *
     * @param groupName
     * @param name
     * @param stackSize
     * @return
     */
    public static ThreadFactory getThreadFactory(final String groupName, final String name, final int stackSize) {
        ThreadGroup group = null;
        if (groupName != null) {
            group = new ThreadGroup(Thread.currentThread().getThreadGroup(), groupName);
        }
        final ThreadGroup finalGroup = group;

        return new ThreadFactory() {
            private final AtomicLong m_createdThreadCount = new AtomicLong(0);
            private final ThreadGroup m_group = finalGroup;
            @Override
            public Thread newThread(final Runnable r) {
                final String threadName = name + " - " + m_createdThreadCount.getAndIncrement();
                Runnable runnable = new Runnable() {
                    @Override
                    public void run() {
                        try {
                            r.run();
                        } catch (Throwable t) {
                            hostLog.error("Exception thrown in thread " + threadName, t);
                        }
                    }
                };

                Thread t = new Thread(m_group, runnable, threadName, stackSize);
                t.setDaemon(true);
                return t;
            }
        };
    }

    /**
     * Return the local hostname, if it's resolvable.  If not,
     * return the IPv4 address on the first interface we find, if it exists.
     * If not, returns whatever address exists on the first interface.
     * @return the String representation of some valid host or IP address,
     *         if we can find one; the empty string otherwise
     */
    public static String getHostnameOrAddress() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            return addr.getHostName();
        } catch (UnknownHostException e) {
            try {
                // XXX-izzy Won't we randomly pull localhost here sometimes?
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                if (interfaces == null) {
                    return "";
                }
                NetworkInterface intf = interfaces.nextElement();
                Enumeration<InetAddress> addresses = intf.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address) {
                        return address.getHostAddress();
                    }
                }
                addresses = intf.getInetAddresses();
                if (addresses.hasMoreElements())
                {
                    return addresses.nextElement().getHostAddress();
                }
                return "";
            } catch (SocketException e1) {
                return "";
            }
        }
    }

    /**
     * Return the local IP address, if it's resolvable.  If not,
     * return the IPv4 address on the first interface we find, if it exists.
     * If not, returns whatever address exists on the first interface.
     * @return the String representation of some valid host or IP address,
     *         if we can find one; the empty string otherwise
     */
    public static InetAddress getLocalAddress() {
        try {
            final InetAddress addr = InetAddress.getLocalHost();
            return addr;
        } catch (UnknownHostException e) {
            try {
                Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                if (interfaces == null) {
                    return null;
                }
                NetworkInterface intf = interfaces.nextElement();
                Enumeration<InetAddress> addresses = intf.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address instanceof Inet4Address) {
                        return address;
                    }
                }
                addresses = intf.getInetAddresses();
                if (addresses.hasMoreElements()) {
                    return addresses.nextElement();
                }
                return null;
            } catch (SocketException e1) {
                return null;
            }
        }
    }

    public static long getHSIdFromHostAndSite(int host, int site) {
        long HSId = site;
        HSId = (HSId << 32) + host;
        return HSId;
    }

    public static int getHostIdFromHSId(long HSId) {
        return (int) (HSId & 0xffffffff);
    }

    public static String hsIdToString(long hsId) {
        return Integer.toString((int)hsId) + ":" + Integer.toString((int)(hsId >> 32));
    }

    public static String hsIdCollectionToString(Collection<Long> ids) {
        List<String> idstrings = new ArrayList<String>();
        for (Long id : ids) {
            idstrings.add(hsIdToString(id));
        }
        // Easy hack, sort hsIds lexically.
        Collections.sort(idstrings);
        StringBuilder sb = new StringBuilder();
        boolean first = false;
        for (String id : idstrings) {
            if (!first) {
                first = true;
            } else {
                sb.append(", ");
            }
            sb.append(id);
        }
        return sb.toString();
    }

    public static int getSiteIdFromHSId(long siteId) {
        return (int)(siteId>>32);
    }

    public static <K,V> ImmutableMap<K, ImmutableList<V>> unmodifiableMapCopy(Map<K, List<V>> m) {
        ImmutableMap.Builder<K, ImmutableList<V>> builder = ImmutableMap.builder();
        for (Map.Entry<K, List<V>> e : m.entrySet()) {
            builder.put(e.getKey(), ImmutableList.<V>builder().addAll(e.getValue()).build());
        }
        return builder.build();
    }

    public static byte[] urlToBytes(String url) {
        if (url == null) {
            return null;
        }
        try {
            // get the URL/path for the deployment and prep an InputStream
            InputStream input = null;
            try {
                URL inputURL = new URL(url);
                input = inputURL.openStream();
            } catch (MalformedURLException ex) {
                // Invalid URL. Try as a file.
                try {
                    input = new FileInputStream(url);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                }
            } catch (IOException ioex) {
                throw new RuntimeException(ioex);
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            byte readBytes[] = new byte[1024 * 8];
            while (true) {
                int read = input.read(readBytes);
                if (read == -1) {
                    break;
                }
                baos.write(readBytes, 0, read);
            }

            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String throwableToString(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();
        return sw.toString();
    }

    public static String hsIdKeyMapToString(Map<Long, ?> m) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<Long, ?> entry : m.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(CoreUtils.hsIdToString(entry.getKey()));
            sb.append(entry.getValue());
        }
        sb.append('}');
        return sb.toString();
    }

    public static int availableProcessors() {
        return Math.max(1, Runtime.getRuntime().availableProcessors());
    }
}
