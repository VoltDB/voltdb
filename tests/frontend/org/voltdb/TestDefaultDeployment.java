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

package org.voltdb;

import java.io.IOException;

import junit.framework.TestCase;

import org.voltcore.logging.VoltLogger;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.CatalogUtil;

public class TestDefaultDeployment extends TestCase {

    public void testDefaultDeploymentInitialization() throws IOException  {
        // the default deployment file includes an http server on port 8080.
        // do some verification without starting VoltDB, since that port
        // number conflicts with jenkins on some test servers.
        String absolutePath = RealVoltDB.setupDefaultDeployment(new VoltLogger("HOST"));

        DeploymentType dflt = CatalogUtil.parseDeployment(absolutePath);
        assertTrue(dflt != null);

        assertTrue(dflt.getCluster().getHostcount() == 1);
        assertTrue(dflt.getCluster().getSitesperhost() == 8);
    }
}
