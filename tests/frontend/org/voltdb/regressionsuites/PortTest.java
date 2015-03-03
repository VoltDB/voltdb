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
import java.security.SecureRandom;

import junit.framework.TestCase;

import org.voltdb.compiler.DeploymentBuilder;

public abstract class PortTest extends TestCase {
    private PortListener ncprocess;
    private PipeToFile pf;
    private final boolean m_loopback;
    protected Process liveprocess; // optional



    public PortTest(String name) {
        super(name);
        m_loopback = false;
    }

    public PortTest(String name, boolean loopback) {
        super(name);
        m_loopback = loopback;
    }

    /**
     * JUnit special method called to setup the test. This instance will start the VoltDB server.
     */
    @Override
    public void setUp() throws Exception {
        int rport = SecureRandom.getInstance("SHA1PRNG").nextInt(2000) + 22000;
        System.out.println("Random port is: " + rport);
        ncprocess = new PortListener(rport, m_loopback);
        DeploymentBuilder db = customizeDeployment();
        LocalCluster config = LocalCluster.configure(getClass().getSimpleName(), "", db);
        assertNotNull("LocalCluster failed to compile", config);
        config.portGenerator.enablePortProvider(); //XXX: does this need to be virtually disabled for TestJMXPort?
        customizeConfig(config, rport);
        // We expect it to crash
        config.expectToCrash();

        config.startUp();
        pf = config.m_pipes.get(0);
        assertNotNull(pf);
        Thread.sleep(10000);
    }

    protected DeploymentBuilder customizeDeployment() { return new DeploymentBuilder(2); }

    abstract protected void customizeConfig(LocalCluster config, int rport);

    /**
     * JUnit special method called to shutdown the test. This instance will
     * stop the VoltDB server using the VoltServerConfig instance provided.
     */
    @Override
    public void tearDown() throws Exception {
        if (ncprocess != null) {
            ncprocess.close();
        }
        if (liveprocess != null) {
            liveprocess.destroy();
        }
    }

    protected void checkPort(final CharSequence pattern) throws Exception {
        BufferedReader bi = new BufferedReader(new FileReader(new File(pf.m_filename)));
        String line;
        boolean found = false;
        try {
            while ((line = bi.readLine()) != null) {
                System.out.println(line);
                if (line.contains(pattern)) {
                    found = true;
                    break;
                }
            }
        }
        finally {
            bi.close();
        }
        assertTrue(found);
    }
}
