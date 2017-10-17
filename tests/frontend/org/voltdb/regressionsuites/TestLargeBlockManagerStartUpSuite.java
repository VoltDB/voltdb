/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * This test needs to test three things:
 *  1. On database start up, the large_query_swap directory is created
 *  2. If it already exists at startup, any existing files in there are deleted
 *  3. On shutdown, if any files are in the directory, they should be deleted
 * This should be tested for both classic and new CLI.
 */
public class TestLargeBlockManagerStartUpSuite extends JUnit4LocalClusterTest {

    private static LocalCluster cluster = null;
    private static VoltProjectBuilder builder = null;

    private void init(boolean useNewCli) {
        builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);

        cluster = new LocalCluster("TestLargeBlockManagerStartUpSuite.jar",
                3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        cluster.setNewCli(useNewCli);
        cluster.setHasLocalServer(false);
        assertTrue(cluster.compile(builder));

        cluster.startUp();
    }

    Path getLargeQuerySwapDir() {
        assert (cluster.getHostRoots().get("0") != null);
        return Paths.get(cluster.getHostRoots().get("0") + "/voltdbroot/large_query_swap");
    }

    @Test
    public void testLargeQuerySwapDirectoryCreated() throws IOException {
        for (boolean useNewCli : new boolean[] {true, false}) {
            try {
                System.out.println("Testing with " + (useNewCli ? "new" : "legacy") + " CLI");
                init(useNewCli);

                Path largeQuerySwapDir = getLargeQuerySwapDir();
                assertTrue(Files.exists(largeQuerySwapDir));
                assertTrue(Files.isDirectory(largeQuerySwapDir));
                assertEquals(0, countFilesInDir(largeQuerySwapDir));
            }
            finally {
                try {
                    cluster.shutDown();
                }
                catch (Throwable t) {
                }
            }
        }
    }

    private static int countFilesInDir(Path dir) throws IOException {
        int count = 0;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
            Iterator<Path> it = dirStream.iterator();
            while (it.hasNext()) {
                Path p = it.next();
                System.out.println("--> " + p);
                ++count;
            }
        }

        return count;
    }
}
