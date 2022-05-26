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
import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.NativeLibraryLoader;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestEELibraryLoaderSuite extends RegressionSuite {

    private static int SITES = 1;
    private static int HOSTS = 1;
    private static int KFACTOR = 0;
    private static boolean hasLocalServer = false;

    public TestEELibraryLoaderSuite(String name) {
        super(name);
    }

    public void testDontLoadFromJar() throws Exception {
        // Verify that the server started with use.nativelibs flag and we can connect to it fine.
        getClient();
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestEELibraryLoaderSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();

        Map<String, String> additionalEnv = new HashMap<>();
        additionalEnv.put(NativeLibraryLoader.USE_JAVA_LIBRARY_PATH, "true");
        config = new LocalCluster("ee-library-loader",
                SITES, HOSTS, KFACTOR,
                BackendTarget.NATIVE_EE_JNI,
                additionalEnv);

        ((LocalCluster) config).setHasLocalServer(hasLocalServer);
        boolean success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
