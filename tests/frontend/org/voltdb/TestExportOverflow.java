/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.export.ExportDataProcessor;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;

/**
 * End to end Export tests using the injected custom export.
 */

public class TestExportOverflow extends RegressionSuite {

    public TestExportOverflow(String name)
    {
        super(name);
    }

    private File findExportOverflowDir(File root) {
        if (!root.isDirectory()) {
            return null;
        }

        File foundFile = null;
        for (File f : root.listFiles()) {
            if (f.getName().contains("export_overflow")) {
                foundFile = f;
            } else {
                foundFile = findExportOverflowDir(f);
            }
            if (foundFile!=null) {
                break;
            }
        }

        return foundFile;
    }

    public static Map<String, FileTime> getFileTimeAttributesRecursively(File root) throws IOException {
        Map<String, FileTime> attributes = new HashMap<>();
        if (root.isDirectory()) {
            for (File f: root.listFiles()) {
                Path path = Paths.get(f.toURI());
                BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
                attributes.put(f.getName(), attr.creationTime());
            }
        }
        else {
            Path path = Paths.get(root.toURI());
            BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
            attributes.put(root.getName(), attr.creationTime());
        }
        return attributes;
    }

    public void testExportOverflowAutomoticDeletion() throws Exception {
        if (isValgrind()) {
            return;
        }
        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        // insert rows
        for (int i=0; i<1000; i++) {
            ClientResponse response = client.callProcedure("stream1.Insert", i, i+ "str");
            VoltTable result = response.getResults()[0];
            result.advanceRow();
            assertEquals(1, result.getLong(0));
        }
        client.drain();
        File overflowDir;
        boolean newCli = ((LocalCluster)m_config).isNewCli();
        if (newCli) {
            overflowDir = new File(((LocalCluster)m_config).getServerSpecificRoot("0") + "/export_overflow");
        } else {
            ArrayList<File> subroots = ((LocalCluster) m_config).getSubRoots();
            overflowDir = findExportOverflowDir(subroots.get(0));
        }
        Map<String, FileTime> fileTimes = getFileTimeAttributesRecursively(overflowDir);
        String[] oldOverflowFiles = overflowDir.list();
        assertTrue(oldOverflowFiles.length>0);

        // shutdown and startup with force flag
        // and verify that export overflow directory is cleared
        m_config.shutDown();
        Map<String, Long> lastModifiedMap = new HashMap<>();
        for (String f : oldOverflowFiles) {
            lastModifiedMap.put(f, (new File(f)).lastModified());
        }
        ((LocalCluster) m_config).setForceVoltdbCreate(true);
        m_config.startUp(false);
        Map<String, FileTime> newFileTimes = getFileTimeAttributesRecursively(overflowDir);
        assertTrue(fileTimes.size() == newFileTimes.size());

        assertTrue(oldOverflowFiles.length == newFileTimes.size());

        String fileName;
        FileTime newer;
        FileTime older;
        for (int i = 0; i < oldOverflowFiles.length; i++) {
            fileName = oldOverflowFiles[i];
            newer = newFileTimes.get(fileName);
            older = fileTimes.get(fileName);
            assertTrue(newer != null);
            assertTrue(older != null);
            assertTrue(newer.compareTo(older) > 0);
        }
    }

    static public junit.framework.Test suite() throws Exception
    {
        final MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestExportOverflow.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        // configure export
        project.addLiteralSchema(
                "CREATE STREAM stream1 EXPORT TO TARGET rejecting1 (id integer NOT NULL, value varchar(25) NOT NULL);");
        project.addExport(true, "custom", null, "rejecting1");

        // set up default export connector
        System.setProperty(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");
        Map<String, String> additionalEnv = new HashMap<String, String>();
        additionalEnv.put(ExportDataProcessor.EXPORT_TO_TYPE, "org.voltdb.exportclient.RejectingExportClient");

        LocalCluster config = new LocalCluster("export-overflow-test.jar", 1, 1, 0,
                BackendTarget.NATIVE_EE_JNI, LocalCluster.FailureState.ALL_RUNNING, true, additionalEnv);
        config.setHasLocalServer(false);
        // This is only for testing create --force.
        config.setNewCli(false);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config);

        return builder;
    }
}
