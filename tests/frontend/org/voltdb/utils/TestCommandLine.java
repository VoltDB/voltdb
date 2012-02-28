/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.utils;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestCommandLine
{
    @Test
    public void testCommandLineAndTestOpts()
    {
        CommandLine cl = new CommandLine();
        cl.addTestOptions(true);
        cl.setInitialHeap(2048);
        System.out.println(cl);
        assertTrue(cl.toString().contains("-DLOG_SEGMENT_SIZE"));
        assertTrue(cl.toString().contains("-DVoltFilePrefix"));
        assertTrue(cl.toString().contains("MaxDirectMemorySize"));
        assertTrue(cl.toString().contains("timestampsalt"));
        assertFalse(cl.toString().contains("HeapDumpPath=/tmp"));
        assertFalse(cl.toString().contains("-Xms"));
        cl.addTestOptions(false);
        assertFalse(cl.toString().contains("-DLOG_SEGMENT_SIZE"));
        assertFalse(cl.toString().contains("-DVoltFilePrefix"));
        assertFalse(cl.toString().contains("MaxDirectMemorySize"));
        assertFalse(cl.toString().contains("timestampsalt"));
        assertTrue(cl.toString().contains("HeapDumpPath=/tmp"));
        assertTrue(cl.toString().contains("-Xms"));
    }

    @Test
    public void testCopy()
    {
        // Check a naive copy
        CommandLine cl = new CommandLine();
        // Set at least the CommandLine local fields to non-defaults
        cl.addTestOptions(true);
        cl.debugPort(1234);
        cl.zkport(4321);
        cl.buildDir("dood");
        cl.javaLibraryPath("sweet");
        cl.log4j("whats");
        cl.voltFilePrefix("mine");
        cl.setInitialHeap(470);
        cl.setMaxHeap(740);
        cl.classPath("say");
        cl.javaExecutable("megajava");
        cl.jmxPort(909);
        cl.jmxHost("notreal");
        cl.rejoinUser("nobody");
        cl.rejoinPassword("cares");

        CommandLine cl2 = cl.makeCopy();
        assertEquals(cl.toString(), cl2.toString());
    }

    @Test
    public void testStartCommand()
    {
        CommandLine cl = new CommandLine();
        cl.startCommand("START");
        assertTrue(cl.toString().contains("start"));
        cl.startCommand("CREATE");
        assertTrue(cl.toString().contains("create"));
        cl.startCommand("RECOVER");
        assertTrue(cl.toString().contains("recover"));
        cl.startCommand("NONSENSE");
        assertTrue(cl.toString().contains("start"));
    }

    @Test
    public void testRejoin()
    {
        CommandLine cl = new CommandLine();
        cl.startCommand("RECOVER");
        cl.rejoinHostAndPort("127.0.0.1:6666");
        assertTrue(cl.toString().contains("rejoinhost 127.0.0.1:6666"));
        assertFalse(cl.toString().contains("start"));
        assertFalse(cl.toString().contains("recover"));
        // add user and then password and make sure right stuff happens
        cl.rejoinUser("super");
        assertTrue(cl.toString().contains("rejoinhost super@127.0.0.1:6666"));
        cl.rejoinPassword("duper");
        assertTrue(cl.toString().contains("rejoinhost super:duper@127.0.0.1:6666"));
        // no user mean no password
        cl.rejoinUser(null);
        assertTrue(cl.toString().contains("rejoinhost 127.0.0.1:6666"));
    }

    @Test
    public void testInterfaces()
    {
        CommandLine cl = new CommandLine();
        assertFalse(cl.toString().contains("internalinterface"));
        assertFalse(cl.toString().contains("externalinterface"));
        cl.internalInterface("10.0.0.10");
        assertTrue(cl.toString().contains("internalinterface 10.0.0.10"));
        assertFalse(cl.toString().contains("externalinterface"));
        cl.externalInterface("192.168.0.123");
        assertTrue(cl.toString().contains("internalinterface 10.0.0.10"));
        assertTrue(cl.toString().contains("externalinterface 192.168.0.123"));
    }
}
