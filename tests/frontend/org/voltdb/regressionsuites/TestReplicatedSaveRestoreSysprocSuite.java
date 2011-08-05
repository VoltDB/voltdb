/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb_testprocs.regressionsuites.saverestore.CatalogChangeSingleProcessServer;
import org.voltdb_testprocs.regressionsuites.saverestore.SaveRestoreTestProjectBuilder;


/**
 * Need an entire separate version of the suite to reproduce ENG-696. Need to run the suite
 * with no partitioned tables. Run as many of the tests as is practical without taking forever.
 */
public class TestReplicatedSaveRestoreSysprocSuite extends TestSaveRestoreSysprocSuite {

    public TestReplicatedSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception
    {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
    }

    @Override
    public void testSaveRestoreJumboRows() {}

    @Override
    public void testSaveAndRestorePartitionedTable() {}

    @Override
    public void testCorruptedFiles() {}

    @Override
    public void testCorruptedFilesRandom() {}

    @Override
    public void testRepartition() {}

    @Override
    public void testChangeDDL() {}

    @Override
    public void testGoodChangeAttributeTypes() {}

    @Override
    public void testBadChangeAttributeTypes() {}

    @Override
    public void testRestore12Snapshot() {}

    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestReplicatedSaveRestoreSysprocSuite.class);

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaultsNoPartitioning();

        config =
            new CatalogChangeSingleProcessServer("sysproc-threesites.jar", 3,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
