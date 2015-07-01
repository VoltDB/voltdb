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

package org.voltcore.network;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SelectionKey;

import com.google_voltpatches.common.base.Function;
import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class TestReverseDNSCache extends TestCase {
    static Function<InetAddress, String> DNS_RESOLVER;
    private Field resolverField;

    public static InetAddress getAddress(final Integer address) throws Exception {
        ByteBuffer addressBytes = ByteBuffer.allocate(4);
        addressBytes.putInt(address);
        return InetAddress.getByAddress(addressBytes.array());
    }

    /*
     * An experiment with madness
     */
    @BeforeClass
    public void setUp() throws Exception {
        resolverField = ReverseDNSCache.class.getDeclaredField("DNS_RESOLVER");
        resolverField.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(resolverField, resolverField.getModifiers() & ~Modifier.FINAL);
        DNS_RESOLVER = (Function<InetAddress, String>)resolverField.get(null);
    }

    private void toggleDNSResolver(final String retval) throws Exception {
        if (retval == null) {
           resolverField.set(null, DNS_RESOLVER);
        } else {
            resolverField.set(null, new Function<InetAddress, String>() {

                @Override
                public String apply(java.net.InetAddress inetAddress) {
                    return retval;
                }
            });
        }
    }

    @Test
    public void testCaching() throws Exception {
        final InetAddress addr = getAddress(42);
        toggleDNSResolver("foozle");
        ReverseDNSCache.hostnameOrAddress(addr);
        toggleDNSResolver(addr.getHostAddress());
        assertEquals("foozle", ReverseDNSCache.hostnameOrAddress(addr));
    }

    @Test
    public void testFailedStaysFailed() throws Exception {
        final InetAddress addr = getAddress(43);
        toggleDNSResolver(addr.getHostAddress());
        ReverseDNSCache.hostnameOrAddress(addr);
        toggleDNSResolver("foozle");
        assertFalse("foozle".equals(ReverseDNSCache.hostnameOrAddress(addr)));
    }
}
