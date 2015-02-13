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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.voltdb.StartAction;

public class TestCommandLine
{
    @Test
    public void testCommandLineAndTestOpts()
    {
        CommandLine cl = new CommandLine(StartAction.CREATE);
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
        CommandLine cl = new CommandLine(StartAction.CREATE);
        // Set at least the CommandLine local fields to non-defaults
        cl.addTestOptions(true);
        cl.debugPort(1234);
        cl.zkport(4321);
        cl.buildDir("dood");
        cl.voltRoot("goober");
        cl.javaLibraryPath("sweet");
        cl.rmiHostName("springsteen");
        cl.log4j("whats");
        cl.voltFilePrefix("mine");
        cl.setInitialHeap(470);
        cl.setMaxHeap(740);
        cl.classPath("say");
        cl.javaExecutable("megajava");
        cl.jmxPort(909);
        cl.jmxHost("notreal");

        CommandLine cl2 = cl.makeCopy();
        assertEquals(cl.toString(), cl2.toString());
    }

    @Test
    public void testStartCommand()
    {
        CommandLine cl = new CommandLine(StartAction.CREATE);
        assertTrue(cl.toString().contains("create"));
        cl.startCommand("RECOVER");
        assertTrue(cl.toString().contains("recover"));
        cl.startCommand("LIVE    REJOIN");
        assertTrue(cl.toString().contains("live rejoin"));
        cl.startCommand("RECOVER    SAFEMODE");
        assertTrue(cl.toString().contains("recover safemode"));
        try
        {
            cl.startCommand("NONSENSE");
        }
        catch (RuntimeException rte)
        {
            assertTrue(rte.getMessage().contains("Unknown action"));
        }
        try
        {
            cl.startCommand("start");
        }
        catch (RuntimeException rte)
        {
            assertTrue(rte.getMessage().contains("Unknown action"));
        }
        cl = new CommandLine(StartAction.LIVE_REJOIN);
        assertTrue(cl.toString().contains("live rejoin"));
        cl = new CommandLine(StartAction.SAFE_RECOVER);
        assertTrue(cl.toString().contains("recover safemode"));
    }

    @Test
    public void testRejoin()
    {
        CommandLine cl = new CommandLine(StartAction.REJOIN);
        cl.leader("127.0.0.1:6666");
        System.err.println(cl.toString());
        assertTrue(cl.toString().contains("rejoin host 127.0.0.1:6666"));
        assertFalse(cl.toString().contains("start"));
        assertFalse(cl.toString().contains("recover"));
    }

    @Test
    public void testLiveRejoin()
    {
        CommandLine cl = new CommandLine(StartAction.LIVE_REJOIN);
        cl.leader("127.0.0.1:6666");
        System.err.println(cl.toString());
        assertTrue(cl.toString().contains("live rejoin host 127.0.0.1:6666"));
        assertFalse(cl.toString().contains("start"));
        assertFalse(cl.toString().contains("recover"));
    }

    @Test
    public void testInterfaces()
    {
        CommandLine cl = new CommandLine(StartAction.CREATE);
        assertFalse(cl.toString().contains("internalinterface"));
        assertFalse(cl.toString().contains("externalinterface"));
        cl.internalInterface("10.0.0.10");
        assertTrue(cl.toString().contains("internalinterface 10.0.0.10"));
        assertFalse(cl.toString().contains("externalinterface"));
        cl.externalInterface("192.168.0.123");
        assertTrue(cl.toString().contains("internalinterface 10.0.0.10"));
        assertTrue(cl.toString().contains("externalinterface 192.168.0.123"));
    }

    /**
     * Hack to override mutability of the map returned by {@code System.getenv()}
     * <p>
     * See {@linkplain http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java StackOverflow article}
     * @param envValue new value for VOLTDB_OPTS
     * @throws Exception
     */
    public static void setVoltDbOpts(String envValue) throws Exception
    {
        Map<String, String> newenv = new HashMap<String, String>(System.getenv());
        newenv.put("VOLTDB_OPTS", envValue);
        Map<String, String> env = System.getenv();
        Class<?> cl = env.getClass();
        if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
        }
    }

    @Test
    public void testExtraJvmOptsAgentSpec() throws Exception
    {
        String agentSpec = "-javaagent:/path/to/jolokia-jvm-1.0.1-agent.jar=port=11159,agentContext=/,host=0.0.0.0";
        setVoltDbOpts(agentSpec);

        CommandLine cl = new CommandLine(StartAction.CREATE);
        String cmd = cl.toString();

        assertTrue(cmd.contains(" " + agentSpec + " "));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") > cmd.indexOf(agentSpec));
    }

    @Test
    public void testExtraJvmOptsGcAndPropsSpec() throws Exception
    {
        String propOne = "-Done.prop=\"yolanda is a nice gal:\"";
        String propTwo = "-Dtwo.prop=\"yobo is: a nice guy\"";
        String propThree = "-Dsingle.quote='In single quote \"bliss\"'";
        String voltOne   = "enableIv2";
        String voltTwo   = "project";
        String voltThree = "\"/a/file/with a space.xml\"";
        String minHeap = "-Xms1024m";
        String maxHeap = "-Xmx4096m";
        String gcSpec  = "-XX:+UseConcMarkSweepGC";
        String agentSpec = "-javaagent:jolokia.jar=port=11159,desc=\"cool  loking\\ agent\"";
        setVoltDbOpts(propOne
                + " " + propTwo
                + " " + propThree
                + " -voltdb:" + voltOne
                + " -voltdb:" + voltTwo
                + " -voltdb:" + voltThree
                + " " + agentSpec
                + " " + minHeap
                + " " + maxHeap
                + " " + gcSpec
                + " -cp one.jar:some/dir/*.jar:tooranda.jar:. -d32"
                + " -Djava.library.path=/some/diryolanda.so:/tmp/hackme.so"
                + " sgra rehto emos"); // reverse of 'some other args'

        CommandLine cl = new CommandLine(StartAction.CREATE);
        String cmd = cl.toString();

        assertTrue(cmd.contains(" " + propOne + " "));
        assertTrue(cmd.contains(" " + propTwo+ " "));
        assertTrue(cmd.contains(" " + propThree+ " "));
        assertTrue(cmd.contains(" " + voltOne + " "));
        assertTrue(cmd.contains(" " + voltTwo + " "));
        assertTrue(cmd.contains(" " + voltThree + " "));
        assertTrue(cmd.contains(" " + agentSpec + " "));
        assertTrue(cmd.contains(" " + gcSpec + " "));
        assertTrue(cmd.contains(" sgra rehto emos"));

        assertFalse(cmd.contains(" -cp" ));
        assertFalse(cmd.contains(" -d32" ));
        assertFalse(cmd.contains(" -voltdb:" ));
        assertFalse(cmd.contains("/some/diryolanda.so:/tmp/hackme.so"));
        assertFalse(cmd.contains("tooranda.jar"));
        assertFalse(cmd.contains(" " + minHeap + " "));
        assertFalse(cmd.contains(" " + maxHeap + " "));

        assertTrue(cmd.indexOf("org.voltdb.VoltDB") > cmd.indexOf(propOne));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") > cmd.indexOf(propTwo));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") > cmd.indexOf(propThree));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") > cmd.indexOf(agentSpec));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") > cmd.indexOf(gcSpec));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") < cmd.indexOf("sgra rehto emos"));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") < cmd.indexOf(voltOne));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") < cmd.indexOf(voltTwo));
        assertTrue(cmd.indexOf("org.voltdb.VoltDB") < cmd.indexOf(voltThree));
    }
}
