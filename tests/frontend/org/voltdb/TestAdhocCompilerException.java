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

package org.voltdb;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.sysprocs.AdHocNTBase;
import org.voltdb.utils.MiscUtils;

public class TestAdhocCompilerException extends AdhocDDLTestBase
{
    @Test
    public void testEng7653UnexpectedException() throws Exception
    {
        // Enables special DDL string triggering artificial exception in AsyncCompilerAgent.
        System.setProperty("asynccompilerdebug", "true");

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        // Trigger an exception inside AsyncCompilerAgent
        try {
            startSystem(config);
            boolean threw = false;
            try {
                // Ten seconds should be long enough to detect a hang.
                String toxicDDL = AdHocNTBase.DEBUG_EXCEPTION_DDL + ";";
                m_client.callProcedureWithClientTimeout(
                        BatchTimeoutOverrideType.NO_TIMEOUT, "@AdHoc", 10, TimeUnit.SECONDS, toxicDDL);
            }
            catch (ProcCallException pce) {
                String message = pce.getLocalizedMessage();
                if (message.startsWith("No response received in the allotted time")) {
                    // Check that a network thread didn't die.
                    tryOldClientWithValidDDL();
                    tryNewClientWithValidDDL();
                    fail("Timeout, server was probably hung. " + message);
                }
                assertTrue(String.format("Unexpected exception message: %s...", message),
                           message.contains(AdHocNTBase.DEBUG_EXCEPTION_DDL));
                threw = true;
            }
            assertTrue("Expected exception", threw);
        }
        finally {
            teardownSystem();
        }
    }

    private void tryOldClientWithValidDDL() throws Exception
    {
        tryValidDDL(m_client);
    }

    private void tryNewClientWithValidDDL() throws Exception
    {
        // For fun see if a client can connect.
        Client client = ClientFactory.createClient();
        try {
            client.createConnection("localhost");
            tryValidDDL(client);
        }
        catch(IOException e) {
            fail("Additional client connection failed. " + e.getLocalizedMessage());
        }
        finally {
            client.close();
        }
    }

    static int validDDLAttempt = 0;

    private void tryValidDDL(Client client) throws Exception
    {
        validDDLAttempt++;
        String tableName = String.format("FOO%d", validDDLAttempt);
        String ddl = String.format("create table %s (id smallint not null);", tableName);
        try {
            ClientResponse resp2 = client.callProcedureWithClientTimeout(
                    BatchTimeoutOverrideType.NO_TIMEOUT, "@AdHoc", 20, TimeUnit.SECONDS, ddl);
            assertTrue(String.format("Valid DDL attempt #%d failed.", validDDLAttempt),
                       resp2.getStatus() == ClientResponse.SUCCESS);
        }
        catch(ProcCallException pce2) {
            fail("Valid DDL from new client failed with exception after previous one had exception. "
                    + pce2.getLocalizedMessage());
        }
        assertTrue(String.format("Failed to create table %s.", tableName),
                   findTableInSystemCatalogResults(tableName));
    }
}
