/* This file is part of VoltDB.
 * Copyright (C) 2021-2022 Volt Active Data Inc.
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

import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.utils.BuildDirectoryUtils;

import junit.framework.TestCase;

/**
 * This test was previously in TestVoltCompiler.
 * It was moved into its own file because it's significantly
 * different to all other compiler tests.
 */

@PowerMockIgnore({"org.voltdb.compiler.procedures.*",
                  "org.voltdb.VoltProcedure",
                  "org.voltdb_testprocs.regressionsuites.fixedsql.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(VoltDB.class)
public class TestCopyParamsMsg extends TestCase {

    private String testout_jar;

    @Override
    @Before
    public void setUp() {
        testout_jar = BuildDirectoryUtils.getBuildDirectoryPath() + File.pathSeparator + "testout.jar";
        // Hack to get libcatalog loaded before we start mocking pieces of the catalog implementation.
        // The Catalog constructor creates a CatalogOperator which has static initializer to load libcatalog.
        new Catalog();
    }

    @Override
    @After
    public void tearDown() {
        (new File(testout_jar)).delete();
    }

    public void testCopyParametersMessage(){
        VoltDBInterface voltdb = VoltDB.instance();

        org.voltdb.compiler.deploymentfile.ObjectFactory factory =
                    new org.voltdb.compiler.deploymentfile.ObjectFactory();
        DeploymentType deployment = factory.createDeploymentType();
        SystemSettingsType systemSettings =  factory.createSystemSettingsType();
        SystemSettingsType.Procedure procedureType = factory.createSystemSettingsTypeProcedure();
        procedureType.setCopyparameters(true);
        systemSettings.setProcedure(procedureType);
        deployment.setSystemsettings(systemSettings);

        CatalogContext catalog = mock(CatalogContext.class);
        when(catalog.getDeployment()).thenReturn(deployment);
        VoltDBInterface mockedVolt = spy(voltdb);
        when(mockedVolt.getCatalogContext()).thenReturn(catalog);
        when(mockedVolt.getKFactor()).thenReturn(1);

        PowerMockito.mockStatic(VoltDB.class);
        when(VoltDB.instance()).thenReturn(mockedVolt);

        String ddl = "CREATE TABLE FOO ( PKEY INTEGER NOT NULL, PRIMARY KEY (PKEY) ); " +
                "PARTITION TABLE FOO ON COLUMN PKEY; " +
                "CREATE PROCEDURE FROM CLASS org.voltdb.compiler.procedures.ProcedureWithArrayParams; " +
                "PARTITION PROCEDURE ProcedureWithArrayParams ON TABLE FOO COLUMN PKEY; ";
        String expectedError =
                "Procedure ProcedureWithArrayParams contains a mutable array parameter " +
                        "but the database is configured not to copy parameters before execution. " +
                        "This can result in unpredictable behavior, crashes or data corruption " +
                        "if stored procedure modifies the content of the parameters. " +
                        "Set the copyparameters configuration option to true to avoid this danger " +
                        "if the stored procedures might modify parameter content.";

        procedureType.setCopyparameters(false);

        VoltCompiler compiler = new VoltCompiler(false);
        boolean success = compileDDL(ddl, compiler);
        assertTrue(success);
        boolean status = isFeedbackPresent(expectedError,compiler.m_warnings);
        assertTrue(status);
    }

    private boolean compileDDL(String ddl, VoltCompiler compiler) {
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(ddl);
        String schemaPath = schemaFile.getPath();
        return compiler.compileFromDDL(testout_jar, schemaPath);
    }

    private boolean isFeedbackPresent(String expectedError, ArrayList<Feedback> fbs) {
        for (Feedback fb : fbs) {
            if (fb.getStandardFeedbackLine().contains(expectedError)) {
                return true;
            }
        }
        return false;
    }
}
