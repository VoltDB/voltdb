/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.EnumSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Joiner;

final public class TestInitStartAction {

    static File rootDH;
    static File cmdlogDH;

    private static final String[] deploymentXML = {
            "<?xml version=\"1.0\"?>",
            "<deployment>",
            "    <cluster hostcount=\"1\"/>",
            "    <paths>",
            "        <voltdbroot path=\"_VOLTDBROOT_PATH_\"/>",
            "        <commandlog path=\"_COMMANDLOG_PATH_\"/>",
            "    </paths>",
            "    <httpd enabled=\"true\">",
            "        <jsonapi enabled=\"true\"/>",
            "    </httpd>",
            "    <commandlog enabled=\"false\"/>",
            "</deployment>"
        };

    static final Pattern voltdbrootRE = Pattern.compile("_VOLTDBROOT_PATH_");
    static final Pattern commandlogRE = Pattern.compile("_COMMANDLOG_PATH_");
    static File legacyDeploymentFH;

    @ClassRule
    static public final TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void setupClass() throws Exception {
        rootDH = tmp.newFolder();
        cmdlogDH = new File(tmp.newFolder(), "commandlog");

        legacyDeploymentFH = new File(rootDH, "deployment.xml");
        try (FileWriter fw = new FileWriter(legacyDeploymentFH)) {
            Matcher mtc = voltdbrootRE.matcher(Joiner.on('\n').join(deploymentXML));
            String expnd = mtc.replaceAll(new File(rootDH, "voltdbroot").getPath());

            mtc = commandlogRE.matcher(expnd);
            expnd = mtc.replaceAll(cmdlogDH.getPath());

            fw.write(expnd);
        }
        System.setProperty("VOLT_JUSTATEST", "YESYESYES");
        VoltDB.ignoreCrash = true;
    }

    AtomicReference<Throwable> serverException = new AtomicReference<>(null);

    final Thread.UncaughtExceptionHandler handleUncaught = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            serverException.compareAndSet(null, e);
        }
    };

    @Test
    public void testInitStartAction() throws Exception {

        File deplFH = new VoltFile(new VoltFile(new VoltFile(rootDH, "voltdbroot"), "config"), "deployment.xml");
        Configuration c1 = new Configuration(
                new String[]{"initialize", "voltdbroot", rootDH.getPath(), "deployment", legacyDeploymentFH.getPath()});
        ServerThread server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);
        c1.m_forceVoltdbCreate = false;

        server.start();
        server.join();

        assertNotNull(serverException.get());
        if (!(serverException.get() instanceof VoltDB.SimulatedExitException)) {
            System.err.println("got an unexpected exception");
            serverException.get().printStackTrace(System.err);
            if (VoltDB.wasCrashCalled) {
                System.err.println("Crash message is:\n  "+ VoltDB.crashMessage);
            }
        }

        assertTrue(serverException.get() instanceof VoltDB.SimulatedExitException);
        VoltDB.SimulatedExitException exitex = (VoltDB.SimulatedExitException)serverException.get();
        assertEquals(0, exitex.getStatus());

        assertTrue(deplFH.exists() && deplFH.isFile() && deplFH.canRead());

        if (c1.m_isEnterprise) {
            assertTrue(cmdlogDH.exists()
                    && cmdlogDH.isDirectory()
                    && cmdlogDH.canRead()
                    && cmdlogDH.canWrite()
                    && cmdlogDH.canExecute());

            for (int i=0; i<10; ++i) {
                new FileOutputStream(new File(cmdlogDH, String.format("dummy-%02d.log", i))).close();
            }
            assertEquals(10, cmdlogDH.list().length);
        }

        serverException.set(null);
        // server thread sets m_forceVoltdbCreate to true by default
        c1 = new Configuration(
                new String[]{"initialize", "voltdbroot", rootDH.getPath(), "force", "deployment", legacyDeploymentFH.getPath()});
        assertTrue(c1.m_forceVoltdbCreate);
        server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();

        assertNotNull(serverException.get());
        assertTrue(serverException.get() instanceof VoltDB.SimulatedExitException);
        exitex = (VoltDB.SimulatedExitException)serverException.get();
        assertEquals(0, exitex.getStatus());

        assertTrue(deplFH.exists() && deplFH.isFile() && deplFH.canRead());
        if (c1.m_isEnterprise) {
            assertTrue(cmdlogDH.exists()
                    && cmdlogDH.isDirectory()
                    && cmdlogDH.canRead()
                    && cmdlogDH.canWrite()
                    && cmdlogDH.canExecute());
            assertEquals(0, cmdlogDH.list().length);
        }

        try {
            c1 = new Configuration(new String[]{"initialize", "voltdbroot", rootDH.getPath()});
            fail("did not detect prexisting initialization");
        } catch (VoltDB.SimulatedExitException e) {
            assertEquals(-1, e.getStatus());
        }

        VoltDB.wasCrashCalled = false;
        VoltDB.crashMessage = null;
        serverException.set(null);

        c1 = new Configuration(new String[]{"create", "deployment", legacyDeploymentFH.getPath(), "host", "localhost"});
        server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();

        assertNotNull(serverException.get());
        assertTrue(serverException.get() instanceof AssertionError);
        assertTrue(VoltDB.wasCrashCalled);
        assertTrue(VoltDB.crashMessage.contains("Cannot use legacy start action"));

        if (!c1.m_isEnterprise) return;

        VoltDB.wasCrashCalled = false;
        VoltDB.crashMessage = null;
        serverException.set(null);

        c1 = new Configuration(new String[]{"recover", "deployment", legacyDeploymentFH.getPath(), "host", "localhost"});
        server = new ServerThread(c1);
        server.setUncaughtExceptionHandler(handleUncaught);

        server.start();
        server.join();

        assertNotNull(serverException.get());
        assertTrue(serverException.get() instanceof AssertionError);
        assertTrue(VoltDB.wasCrashCalled);
        assertTrue(VoltDB.crashMessage.contains("Cannot use legacy start action"));

        // this test which action should be considered legacy
        EnumSet<StartAction> legacyOnes = EnumSet.complementOf(EnumSet.of(StartAction.INITIALIZE,StartAction.PROBE));
        assertTrue(legacyOnes.stream().allMatch(StartAction::isLegacy));
    }
}
