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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteSelect;

public class TestTwoSitePlans extends TestCase {
    private ExecutionEngine ee1;
    private ExecutionEngine ee2;

    private Catalog catalog = null;
    private Cluster cluster = null;
    private Procedure selectProc = null;
    private Statement selectStmt = null;
    private PlanFragment selectTopFrag = null;
    private PlanFragment selectBottomFrag = null;
    private PlanFragment insertFrag = null;

    static final String JAR = "distplanningregression.jar";

    @SuppressWarnings("deprecation")
    @Override
    public void setUp() throws IOException, InterruptedException {
        VoltDB.instance().readBuildInfo("Test");
        CatalogBuilder cb = TPCCProjectBuilder.catalogBuilderNoProcs()
        .addProcedures(MultiSiteSelect.class, InsertNewOrder.class);
        File catalogJar = File.createTempFile(getClass().getSimpleName(), ".jar");
        catalogJar.deleteOnExit();
        assertTrue("Failed to compile catalog", cb.compile(catalogJar.getAbsolutePath()));

        // load a catalog
        byte[] bytes = MiscUtils.fileToBytes(catalogJar);
        // create the catalog (that will be passed to the ClientInterface
        catalog = CatalogUtil.deserializeCatalogFromJarFileBytes(bytes);

        // update the catalog with the data from the deployment file (WHY?)
        String pathToDeployment = new DeploymentBuilder(2).writeXMLToTempFile();
        assertTrue(CatalogUtil.compileDeploymentForTest(catalog, pathToDeployment) == null);

        cluster = catalog.getClusters().get("cluster");
        CatalogMap<Procedure> procedures = cluster.getDatabases().get("database").getProcedures();
        Procedure insertProc = procedures.get("InsertNewOrder");
        assert(insertProc != null);
        selectProc = procedures.get("MultiSiteSelect");
        assert(selectProc != null);

        // Each EE needs its own thread for correct initialization.
        final AtomicReference<ExecutionEngine> site1Reference = new AtomicReference<ExecutionEngine>();
        final byte configBytes[] = LegacyHashinator.getConfigureBytes(2);
        Thread site1Thread = new Thread() {
            @Override
            public void run() {
                site1Reference.set(
                        new ExecutionEngineJNI(
                                cluster.getRelativeIndex(),
                                1,
                                0,
                                0,
                                "",
                                100,
                                new HashinatorConfig(HashinatorType.LEGACY, configBytes, 0, 0), false));
            }
        };
        site1Thread.start();
        site1Thread.join();

        final AtomicReference<ExecutionEngine> site2Reference = new AtomicReference<ExecutionEngine>();
        Thread site2Thread = new Thread() {
            @Override
            public void run() {
                site2Reference.set(
                        new ExecutionEngineJNI(
                                cluster.getRelativeIndex(),
                                2,
                                1,
                                0,
                                "",
                                100,
                                new HashinatorConfig(HashinatorType.LEGACY, configBytes, 0, 0), false));
            }
        };
        site2Thread.start();
        site2Thread.join();

        // create two EEs
        ee1 = site1Reference.get();
        ee1.loadCatalog(0, catalog.serialize());
        ee2 = site2Reference.get();
        ee2.loadCatalog(0, catalog.serialize());

        // cache some plan fragments
        selectStmt = selectProc.getStatements().get("selectAll");
        assert(selectStmt != null);
        int i = 0;
        // this kinda assumes the right order
        for (PlanFragment f : selectStmt.getFragments()) {
            if (i == 0) selectTopFrag = f;
            else selectBottomFrag = f;
            i++;
        }
        assert(selectTopFrag != null);
        assert(selectBottomFrag != null);

        if (selectTopFrag.getHasdependencies() == false) {
            PlanFragment temp = selectTopFrag;
            selectTopFrag = selectBottomFrag;
            selectBottomFrag = temp;
        }

        // get the insert frag
        Statement insertStmt = insertProc.getStatements().get("insert");
        assert(insertStmt != null);

        for (PlanFragment f : insertStmt.getFragments())
            insertFrag = f;

        // populate plan cache
        ActivePlanRepository.clear();
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectBottomFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectBottomFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(selectTopFrag),
                Encoder.decodeBase64AndDecompressToBytes(selectTopFrag.getPlannodetree()),
                selectStmt.getSqltext());
        ActivePlanRepository.addFragmentForTest(
                CatalogUtil.getUniqueIdForFragment(insertFrag),
                Encoder.decodeBase64AndDecompressToBytes(insertFrag.getPlannodetree()),
                insertStmt.getSqltext());

        // insert some data
        ParameterSet params = ParameterSet.fromArrayNoCopy(1L, 1L, 1L);

        VoltTable[] results = ee2.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(insertFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                1,
                1,
                0,
                42,
                Long.MAX_VALUE);
        assert(results.length == 1);
        assert(results[0].asScalarLong() == 1L);

        params = ParameterSet.fromArrayNoCopy(2L, 2L, 2L);

        results = ee1.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(insertFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { insertStmt.getSqltext() },
                2,
                2,
                1,
                42,
                Long.MAX_VALUE);
        assert(results.length == 1);
        assert(results[0].asScalarLong() == 1L);
    }

    public void testMultiSiteSelectAll() {
        ParameterSet params = ParameterSet.emptyParameterSet();

        int outDepId = 1 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        VoltTable dependency1 = ee1.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE)[0];
        try {
            System.out.println(dependency1.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(dependency1 != null);

        VoltTable dependency2 = ee2.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                null,
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE)[0];
        try {
            System.out.println(dependency2.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(dependency2 != null);

        ee1.stashDependency(outDepId, dependency1);
        ee1.stashDependency(outDepId, dependency2);

        dependency1 = ee1.executePlanFragments(
                1,
                new long[] { CatalogUtil.getUniqueIdForFragment(selectTopFrag) },
                new long[] { outDepId },
                new ParameterSet[] { params },
                new String[] { selectStmt.getSqltext() },
                3, 3, 2, 42, Long.MAX_VALUE)[0];
        try {
            System.out.println("Final Result");
            System.out.println(dependency1.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
