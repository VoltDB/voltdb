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

package org.voltdb.compiler;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import junit.framework.TestCase;

import org.voltdb.VoltDB;
import org.voltdb.utils.BuildDirectoryUtils;

/**
 * Simple and single execution of VoltCompiler.
 * Intended to flush out gross problems independently of
 * the comprehensive VoltCompiler tests. The aim here is
 * simple: to see if we can run the compiler. We don't
 * care about anything more than that.
 */
@PowerMockIgnore({"org.voltdb.compiler.procedures.*",
                  "org.voltdb.VoltProcedure",
                  "org.voltdb_testprocs.regressionsuites.fixedsql.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(VoltDB.class)
public class TestCompilerCanary extends TestCase {

    private String testout_jar;

    @Override
    @Before
    public void setUp() {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
    }

    @Override
    @After
    public void tearDown() {
        (new File(testout_jar)).delete();
    }

    public void testCompile() {
        boolean succ = false;
        try {
            succ = execTest();
        }
        catch (Exception ex) {
            System.out.printf("*** Caught %s exception ***\n", ex.getClass());
            ex.printStackTrace();
            fail("testCompile got an exception");
        }
        assertTrue("Compilation returned inline but failed", succ);
    }

    boolean execTest() throws Exception {
        File file = VoltProjectBuilder.writeStringToTempFile("create table canary (somecolumn integer);");
        System.out.printf("*** Creating compiler instance ***\n");
        VoltCompiler compiler = new VoltCompiler(false);
        System.out.printf("*** Calling compiler ***\n");
        boolean succ = compiler.compileFromDDL(testout_jar, file.getPath());
        System.out.printf("*** Compiler returned '%b' ***\n", succ);
        return succ;
    }
}
