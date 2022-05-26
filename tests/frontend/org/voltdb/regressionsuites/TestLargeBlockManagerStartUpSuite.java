/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * This test needs to test three things:
 *  1. On initialization, the large_query_swap directory is created
 *  2. On database start, any existing files in there are deleted
 *  3. On shutdown, if any files are in the directory, they should be deleted
 */
public class TestLargeBlockManagerStartUpSuite extends RegressionSuite {

    public TestLargeBlockManagerStartUpSuite(String name) {
        super(name);
    }

    Path getLargeQuerySwapDir() {
        LocalCluster cluster = (LocalCluster)m_config;
        return Paths.get(cluster.getServerSpecificRoot("0")).resolve("large_query_swap");
    }

    @Test
    public void testLargeQuerySwapDirectory() throws Exception {
        LocalCluster cluster = (LocalCluster)m_config;

        // large_query_swap will get created on start up.
        Path largeQuerySwapDir = getLargeQuerySwapDir();
        assertTrue(Files.exists(largeQuerySwapDir));
        assertTrue(Files.isDirectory(largeQuerySwapDir));
        assertEquals(0, countFilesInDir(largeQuerySwapDir));

        cluster.shutDown();

        // Create a file in the swap dir,
        // and verify that it gets deleted when the database restarts
        Path spuriousFile = largeQuerySwapDir.resolve("leftover.block");
        Files.createFile(spuriousFile);
        assertTrue(Files.exists(spuriousFile));

        // Re-start the server:
        // DON'T clear local data directories
        // DO skip "init"
        cluster.startUp(false, true);

        // File gets deleted on start up
        assertFalse(Files.exists(spuriousFile));

        // Now, with the cluster running, re-create the spurious file
        // and verify that it gets deleted on shutdown.
        Files.createFile(spuriousFile);
        assertTrue(Files.exists(spuriousFile));

        // Perform a shutdown that gives server a chance to clean up
        Client client = getClient();
        try {
            client.callProcedure("@Shutdown");
        }
        catch (ProcCallException pce) {
            // This is expected, since the connection will be terminated
            // before a response is sent
        }

        cluster.waitForNodesToShutdown();

        // Spurious file is deleted
        assertFalse(Files.exists(spuriousFile));
    }

    private static int countFilesInDir(Path dir) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
            Iterator<Path> it = dirStream.iterator();
            while (it.hasNext()) {
                it.next();
                ++count;
            }
        }

        return count;
    }

    static public junit.framework.Test suite() {

        LocalCluster config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestLargeBlockManagerStartUpSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        boolean success;

        config = new LocalCluster("testLargeBlockManagerStartUpSuite-onesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        config.setHasLocalServer(false);
        success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
