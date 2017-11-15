/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteSelect;

public class TestTwoSitePlans extends TestCase {

    static final String JAR = "distplanningregression.jar";

    ExecutorService site1Thread;
    ExecutorService site2Thread;
    ExecutionEngine ee1;
    ExecutionEngine ee2;

    Catalog catalog = null;
    Cluster cluster = null;
    Procedure selectProc = null;
    Statement selectStmt = null;
    PlanFragment selectTopFrag = null;
    PlanFragment selectBottomFrag = null;
    PlanFragment insertFrag = null;

    @SuppressWarnings("deprecation")
    @Override
    public void setUp() throws IOException, InterruptedException, ExecutionException {
        VoltDB.instance().readBuildInfo("Test");

        // compile a catalog
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + JAR;

        TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(MultiSiteSelect.class, InsertNewOrder.class);

        pb.compile(catalogJar, 2, 0);

        // load a catalog
        byte[] bytes = MiscUtils.fileToBytes(new File(catalogJar));
        String serializedCatalog =
            CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes, false).getFirst());

        // create the catalog (that will be passed to the ClientInterface
        catalog = new Catalog();
        catalog.execute(serializedCatalog);

        // update the catalog with the data from the deployment file
        String pathToDeployment = pb.getPathToDeployment();
        assertTrue(CatalogUtil.compileDeployment(catalog, pathToDeployment, false) == null);

        cluster = catalog.getClusters().get("cluster");
        CatalogMap<Procedure> procedures = cluster.getDatabases().get("database").getProcedures();
        Procedure insertProc = procedures.get("InsertNewOrder");
        assert(insertProc != null);
        selectProc = procedures.get("MultiSiteSelect");
        assert(selectProc != null);

        // Each EE needs its own thread for correct initialization.
        final AtomicReference<ExecutionEngine> site1Reference = new AtomicReference<ExecutionEngine>();
        final byte configBytes[] = LegacyHashinator.getConfigureBytes(2);
        site1Thread = Executors.newSingleThreadExecutor();

        site1Thread.submit(new Runnable() {
            @Override
            public void run() {
                site1Reference.set(
                        new ExecutionEngineJNI(
                                cluster.getRelativeIndex(),
                                1,
                                0,
                                2,
                                0,
                                "",
                                0,
                                64*1024,
                                100,
                                new HashinatorConfig(HashinatorType.LEGACY, configBytes, 0, 0), true));
            }
        }).get();

        final AtomicReference<ExecutionEngine> site2Reference = new AtomicReference<ExecutionEngine>();
        site2Thread = Executors.newSingleThreadExecutor();
        site2Thread.submit(new Runnable() {
            @Override
            public void run() {
                site2Reference.set(
                        new ExecutionEngineJNI(
                                cluster.getRelativeIndex(),
                                2,
                                1,
                                2,
                                0,
                                "",
                                0,
                                64*1024,
                                100,
                                new HashinatorConfig(HashinatorType.LEGACY, configBytes, 0, 0), false));
            }
        }).get();

        // create two EEs
        ee1 = site1Reference.get();
        Future<?> loadComplete = site1Thread.submit(new Runnable() {
            @Override
            public void run() {
                ee1.loadCatalog( 0, catalog.serialize());
            }
        });

        ee2 = site2Reference.get();
        site2Thread.submit(new Runnable() {
            @Override
            public void run() {
                ee2.loadCatalog( 0, catalog.serialize());
            }
        }).get();
        loadComplete.get();

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
        final ParameterSet params = ParameterSet.fromArrayNoCopy(1L, 1L, 1L);

        Future<VoltTable[]> ft = site2Thread.submit(new Callable<VoltTable[]>() {
            @Override
            public VoltTable[] call() throws Exception {
                return ee2.executePlanFragments(
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
            }
        });
        VoltTable[] results = ft.get();
        assert(results.length == 1);
        assert(results[0].asScalarLong() == 1L);

        final ParameterSet params2 = ParameterSet.fromArrayNoCopy(2L, 2L, 2L);

        ft = site1Thread.submit(new Callable<VoltTable[]>() {
            @Override
            public VoltTable[] call() throws Exception {
                return ee1.executePlanFragments(
                        1,
                        new long[] { CatalogUtil.getUniqueIdForFragment(insertFrag) },
                        null,
                        new ParameterSet[] { params2 },
                        new String[] { insertStmt.getSqltext() },
                        2,
                        2,
                        1,
                        42,
                        Long.MAX_VALUE);
            }
        });
        results = ft.get();
        assert(results.length == 1);
        assert(results[0].asScalarLong() == 1L);
    }

    public void testMultiSiteSelectAll() throws InterruptedException, ExecutionException {
        ParameterSet params = ParameterSet.emptyParameterSet();

        int outDepId = 1 | DtxnConstants.MULTIPARTITION_DEPENDENCY;
        Future<VoltTable> ft = site1Thread.submit(new Callable<VoltTable>() {
            @Override
            public VoltTable call() throws Exception {
                return ee1.executePlanFragments(
                        1,
                        new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                        null,
                        new ParameterSet[] { params },
                        new String[] { selectStmt.getSqltext() },
                        3, 3, 2, 42, Long.MAX_VALUE)[0];
            }
        });
        VoltTable dependency1 = ft.get();
        try {
            System.out.println(dependency1.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(dependency1 != null);

        ft = site2Thread.submit(new Callable<VoltTable>() {
            @Override
            public VoltTable call() throws Exception {
                return ee2.executePlanFragments(
                        1,
                        new long[] { CatalogUtil.getUniqueIdForFragment(selectBottomFrag) },
                        null,
                        new ParameterSet[] { params },
                        new String[] { selectStmt.getSqltext() },
                        3, 3, 2, 42, Long.MAX_VALUE)[0];
            }
        });
        VoltTable dependency2 = ft.get();
        try {
            System.out.println(dependency2.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(dependency2 != null);

        ee1.stashDependency(outDepId, dependency1);
        ee1.stashDependency(outDepId, dependency2);

        ft = site1Thread.submit(new Callable<VoltTable>() {
            @Override
            public VoltTable call() throws Exception {
                return ee1.executePlanFragments(
                        1,
                        new long[] { CatalogUtil.getUniqueIdForFragment(selectTopFrag) },
                        new long[] { outDepId },
                        new ParameterSet[] { params },
                        new String[] { selectStmt.getSqltext() },
                        3, 3, 2, 42, Long.MAX_VALUE)[0];
            }
        });
        dependency1 = ft.get();
        try {
            System.out.println("Final Result");
            System.out.println(dependency1.toString());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            site1Thread.shutdown();
            site2Thread.shutdown();
        }
    }
}
