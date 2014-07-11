/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.canonicalddl;

import java.net.URL;
import java.net.URLDecoder;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestCanonicalDDLThroughSQLcmd extends AdhocDDLTestBase
{
    String firstCanonicalDDL = null;
    String secondCanonicalDDL = null;

    public String getFirstCanonicalDDL() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("fullDDL.jar");
        VoltProjectBuilder builder = new VoltProjectBuilder();

        final URL url = TestCanonicalDDLThroughSQLcmd.class.getResource("fullDDL.sql");
        String schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        builder.addSchema(schemaPath);

        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        return builder.getCanonicalDDL();
    }

    public String getSecondCanonicalDDL() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        final URL url = TestCanonicalDDLThroughSQLcmd.class.getResource("emptyDDL.sql");
        String schemaPath = URLDecoder.decode(url.getPath(), "UTF-8");
        builder.addSchema(schemaPath);

        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        System.out.println(firstCanonicalDDL);

//        ClientResponse resp = m_client.callProcedure("@AdHoc", firstCanonicalDDL);
//        System.out.println(resp.getResults()[0]);

        teardownSystem();
        return builder.getCanonicalDDL();
    }

    @Test
    public void testCanonicalDDLRoundtrip() throws Exception {

        firstCanonicalDDL = getFirstCanonicalDDL();
        secondCanonicalDDL = getSecondCanonicalDDL();
//        System.out.println(firstCanonicalDDL);
//        System.out.println("===============");
//        System.out.println(secondCanonicalDDL);
    }

}
