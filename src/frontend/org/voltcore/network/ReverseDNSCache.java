/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.network;

import com.google_voltpatches.common.base.Function;
import com.google_voltpatches.common.cache.Cache;
import com.google_voltpatches.common.cache.CacheBuilder;
import org.voltcore.utils.CoreUtils;

import java.net.InetAddress;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A configurable cache mapping from InetAddress to the hostnames of InetAddresses.
 * Tracks failed lookups allowing for a longer timeout to be specified. This saves on allocating threads
 * to DNS lookups that will time out and works around the lack of async DNS lookups in Java
 */
public class ReverseDNSCache {
    public static final ThreadPoolExecutor m_es =
        new ThreadPoolExecutor(0, 16, 1, TimeUnit.SECONDS,
                               new SynchronousQueue<Runnable>(),
                               CoreUtils.getThreadFactory("Reverse DNS lookups"));

    public static final long DEFAULT_MAX_SUCCESS = 1000 * 10;
    public static final long DEFAULT_MAX_FAILURE = 1000 * 10;
    public static final long DEFAULT_SUCCESS_TIMEOUT = 600L;  //10 minutes
    public static final long DEFAULT_FAILURE_TIMEOUT = 3600L; //1 hr
    public static final TimeUnit DEFAULT_TIMEOUT_UNIT =  TimeUnit.SECONDS;

    private static final Function<InetAddress, String> DNS_RESOLVER = new Function<InetAddress, String>() {

        @Override
        public String apply(java.net.InetAddress inetAddress) {
            return inetAddress.getHostName();
        }
    };

    private static final ReverseDNSCache m_instance =
            new ReverseDNSCache(
                DEFAULT_MAX_SUCCESS,
                DEFAULT_MAX_FAILURE,
                DEFAULT_SUCCESS_TIMEOUT,
                DEFAULT_FAILURE_TIMEOUT,
                DEFAULT_TIMEOUT_UNIT);

    private static final String DUMMY = "";

    private final Cache<InetAddress, String> m_successes;
    private final Cache<InetAddress, String> m_failures;



    /**
     * Passing in null for entries or timeout results in no limit being set
     *
     * Timeout is based on the last time a lookup was performed and not access time
     * @param successEntries
     * @param failureEntries
     * @param successTimeout
     * @param failureTimeout
     * @param timeoutUnit
     */
    public ReverseDNSCache(
            Long successEntries,
            Long failureEntries,
            Long successTimeout,
            Long failureTimeout,
            TimeUnit timeoutUnit) {
        m_successes = getCache(successEntries, successTimeout, timeoutUnit);
        m_failures = getCache(failureEntries, failureTimeout, timeoutUnit);
    }

    public Cache<InetAddress, String> getCache(Long entries, Long timeout, TimeUnit timeoutUnit) {
        CacheBuilder<Object, Object> b = CacheBuilder.newBuilder();
        if (entries != null) b.maximumSize(entries);
        if (timeout != null) b.expireAfterWrite(timeout, timeoutUnit);
        return b.build();
    }

    public String getHostnameOrAddress(InetAddress address) {
        //Check for it in the success cache
        String hostname = m_successes.getIfPresent(address);
        if (hostname == null) {
            //Check for it in the failure cache
            hostname = m_failures.getIfPresent(address);
            if (hostname != null) {
                //Lookup failed recently, return the address string
                return address.getHostAddress();
            }
        } else {
            return hostname;
        }

        //It's not in either cache, do the lookup and see if it succeeded.
        hostname = DNS_RESOLVER.apply(address);
        if (hostname.equals(address.getHostAddress())) {
            m_failures.put(address, DUMMY);
        } else {
            m_successes.put(address, hostname);
        }

        return hostname;
    }

    public static String hostnameOrAddress(InetAddress address) {
        return m_instance.getHostnameOrAddress(address);
    }
}
