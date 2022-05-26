/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.client;

import org.voltdb.client.ClientImpl;
import com.google_voltpatches.common.net.HostAndPort;

import org.junit.Test;
import junit.framework.TestCase;

public class TestClientHostParsing extends TestCase {

    @Override
    public void setUp() {
        System.out.printf("++++++++ %s ++++++++%n", getName());
    }

    public void posTest(String input, String expectedHost, int expectedPort) {
        HostAndPort result = ClientImpl.parseHostAndPort(input);
        System.out.printf("%s -> host %s port %d %n", input, result.getHost(), result.getPort());
        assertEquals(expectedHost, result.getHost());
        assertEquals(expectedPort, result.getPort());
    }

    public void negTest(String input) {
        try {
            HostAndPort result = ClientImpl.parseHostAndPort(input);
            System.out.printf("%s -> host %s port %d %n", input, result.getHost(), result.getPort());
            fail(String.format("Expected '%s' to fail, but it did not", input));
        } catch (IllegalArgumentException ex) {
            System.out.printf("%s -> %s (expected) %n", input, ex.getMessage());
        }
    }

    public void testGoodParse() throws Exception {
        posTest("localhost", "localhost", 21212);
        posTest("localhost:1234", "localhost", 1234);
        posTest("192.168.99.88", "192.168.99.88", 21212);
        posTest("192.168.99.88:1234", "192.168.99.88", 1234);
        posTest("[fe80::936c:ba3c:50c4:828c]", "fe80::936c:ba3c:50c4:828c", 21212);
        posTest("[fe80::936c:ba3c:50c4:828c]:1234", "fe80::936c:ba3c:50c4:828c", 1234);
        posTest("[::1]", "::1", 21212);
        posTest("[::1]:1234", "::1", 1234);
    }

    public void testBadParse() throws Exception {
        negTest("localhost:1234:5678"); // 2 colons, looks like IPv6, not bracketed
        negTest("192.168.99.88:1234:5678"); // ditto
        negTest("[fe80::936c:ba3c:50c4:828c]1234:3678"); // 2 ports (last colon not after bracket)
        negTest("fe80::936c:ba3c:50c4:828c"); // no brackets
        negTest("[fe80::936c:ba3c:50c4:828c]fubar"); // not colon after bracket
        negTest("[fe80::936c:ba3c:50c4:828c"); // no close bracket
        negTest("[192.168.11.22]"); // not ipv6 in brackets
    }

    public void testBadPort() throws Exception {
        negTest("[fe80::936c:ba3c:50c4:828c]:fubar"); // not numeric
        negTest("192.168.11.22:fubar"); // ditto
        negTest("192.168.11.22:65536"); // out of range
        negTest("192.168.11.22:-2"); // out of range
    }
}
