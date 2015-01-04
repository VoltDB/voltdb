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

package org.voltdb.regressionsuites;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.SecureRandom;

import static junit.framework.Assert.fail;
import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSecurityNoAdminUser extends TestCase {

    PipeToFile pf;

    public TestSecurityNoAdminUser(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
        try {
            //Build the catalog
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema("");
            builder.setSecurityEnabled(true, false);
            String catalogJar = "dummy.jar";

            LocalCluster config = new LocalCluster(catalogJar, 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

            config.setHasLocalServer(false);
            //We expect it to crash
            config.setExpectedToCrash(true);

            boolean success = config.compile(builder);
            assertTrue(success);

            config.startUp();
            pf = config.m_pipes.get(0);
            Thread.currentThread().sleep(10000);
        } catch (IOException ex) {
            fail(ex.getMessage());
        } finally {
        }
    }

    /*
     *
     */
    public void testSecurityNoUsers() throws Exception {
        BufferedReader bi = new BufferedReader(new FileReader(new File(pf.m_filename)));
        String line;
        boolean failed = true;
        final CharSequence cs = "Cannot enable security without defining at least one user in the built-in ADMINISTRATOR role in the deployment file.";
        while ((line = bi.readLine()) != null) {
            System.out.println(line);
            if (line.contains(cs)) {
                failed = false;
                break;
            }
        }
        assertFalse(failed);
    }
}
