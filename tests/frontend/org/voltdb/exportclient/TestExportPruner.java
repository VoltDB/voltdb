/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.exportclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class TestExportPruner {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    @Rule
    public final TestName testname = new TestName();

    final String dateFmt = "yyyyMMddHHmmss";
    final SimpleDateFormat sdf = new SimpleDateFormat(dateFmt);

    File baseDir;
    long startTime;

    @Before
    public void before() throws IOException {
        System.out.printf("-=-=-=--=--=-=- Start of test %s -=-=-=--=--=-=-\n", testname.getMethodName());
       baseDir = tmp.newFolder("file_export");
        startTime = System.currentTimeMillis();
    }

    @After
    public void after() throws IOException {
        System.out.printf("-=-=-=--=--=-=- End of test %s -=-=-=--=--=-=-\n", testname.getMethodName());
    }

    // Utilities for generating and parsing file names

    String unbatchFileName(String nonce, int gen, String table, int time) {
        return nonce + "-" +
               String.format("%019d", gen) + "-" +
               table + "-" +
               sdf.format(startTime + time) +
               ".csv";
    }

    Pattern unbatchPattern(String nonce) {
        return Pattern.compile("^" +
                               nonce + "-" +
                               "(\\d+)" + "-" +
                               "(\\w+)" + "-" +
                               "(?<TS>\\d{14})" +
                               ".csv" +
                               "$");
    }

    Pattern unbatchPattern(String nonce, String table) {
        return Pattern.compile("^" +
                               nonce + "-" +
                               "(\\d+)" + "-" +
                               table + "-" +
                               "(?<TS>\\d{14})" +
                               ".csv" +
                               "$");
    }

    String batchDirName(String nonce, long time) {
        return nonce + "-" +
               sdf.format(startTime + time);
    }

    String batchFileName(int gen, String table) {
        return String.format("%019d", gen) + "-" +
               table +
               ".csv";
    }

    Pattern batchPattern(String nonce) {
        return Pattern.compile("^" +
                               nonce + "-" +
                               "(?<TS>\\d{14})" +
                               "$");
    }

    // Some handy relative times, in millisecs

    static final int m40 = -40 * 60 * 1000;
    static final int m30 = -30 * 60 * 1000;
    static final int m20 = -20 * 60 * 1000;
    static final int m10 = -10 * 60 * 1000;

    // Create 8 fake csv files, all with timestamps in the past,
    // and corresponding to 2 tables
    void populateUnbatched() throws IOException {
        int[] timeOffs = { m40, m30, m20, m10 };
        String[] tables = { "TABLE", "LISTE" };
        int gen = 1000;
        for (int time : timeOffs) {
            for (String table : tables) {
                String name = unbatchFileName("NONCE", gen, table, time);
                assertTrue("test has bug in file pattern", unbatchPattern("NONCE").matcher(name).matches());
                File file = new File(baseDir, name);
                boolean created = file.createNewFile();
                assertTrue("collision on create", created);
                gen++;
            }
        }
    }

    // Create 4 fake batch dirs, all with timestamps in the past,
    // each with 2 fake csv files, for different tables
    void populateBatched() throws IOException {
        int[] timeOffs = { m40, m30, m20, m10 };
        String[] tables = { "TABLE", "LISTE" };
        int gen = 1000;
        for (int time : timeOffs) {
            String name = batchDirName("NONCE", time);
            assertTrue("test has bug in dir pattern", batchPattern("NONCE").matcher(name).matches());
            File dir = new File(baseDir, name);
            boolean created = dir.mkdir();
            assertTrue("collision on dir create", created);
            for (String table : tables) {
                String fname = batchFileName(gen, table);
                File file = new File(dir, fname);
                boolean fcreated = file.createNewFile();
                assertTrue("collision on file create", fcreated);
                gen++;
            }
        }
    }

    // Standard retention is 30 fake seconds
    static final long retention = 30 * 60 * 1000;

    // Enumerate and print files in the base dir
    String[] listDir(String note) {
        String[] files = baseDir.list();
        System.out.printf("%s: directory %s, %d files\n", note, baseDir.getName(), files.length);
        for (String f : files) {
            System.out.println(f);
        }
        return files;
    }

    // After pruning, ensure all files remaining have times
    // more recent than 30 secs ago (the retention period.
    void checkResults(String[] names, Pattern patt) {
        checkResults(names, patt, null);
    }

    // 'Unpruned' is provided when only one table of the two
    // was expected to be pruned - so we skip the unpruned one.
    void checkResults(String[] names, Pattern pruned, Pattern unpruned) {
        String cutoff = sdf.format(startTime - retention);
        for (String name : names) {
            if (unpruned == null || !unpruned.matcher(name).matches()) {
                Matcher matcher = pruned.matcher(name);
                assertTrue("match bug", matcher.matches());
                String ts = matcher.group("TS");
                assertEquals("bad ts", 14, ts.length());
                assertTrue("old file unpruned", ts.compareTo(cutoff) > 0);
            }
        }
    }

    // Unbatched mode, pruning by age.
    // Create files of varying apparent age (by timestamp).
    // Run the pruner.
    // Expect half of them to be pruned, and the
    // remainder to all have 'recent' timestamps
    @Test
    public void testUnbatchedByAge() throws Exception {
        populateUnbatched();
        Thread.sleep(10);
        String[] files1 = listDir("Before pruning");
        assertEquals("bug in setup", 8, files1.length);
        System.out.println("... Pruning ...");
        ExportFilePruner pruner = new ExportFilePruner(unbatchPattern("NONCE"), dateFmt);
        pruner.pruneByAge(baseDir, retention);
        String[] files2 = listDir("After pruning");
        assertEquals("unexpected pruned state", 4, files2.length);
        checkResults(files2, unbatchPattern("NONCE"));
    }

    // Batched mode, pruning by age.
    // Create batch dirs of varying apparent age (by timestamp).
    // Run the pruner.
    // Expect half of the batch dirs to be pruned, and
    // the remainder to all have 'recent' timestamps
    @Test
    public void testBatchedByAge() throws Exception {
        populateBatched();
        Thread.sleep(10);
        String[] files1 = listDir("Before pruning");
        assertEquals("bug in setup", 4, files1.length);
        System.out.println("... Pruning ...");
        ExportFilePruner pruner = new ExportFilePruner(batchPattern("NONCE"), dateFmt);
        pruner.pruneByAge(baseDir, retention);
        String[] files2 = listDir("After pruning");
        assertEquals("unexpected pruned state", 2, files2.length);
        checkResults(files2, batchPattern("NONCE"));
    }

    // Unbatched mode, pruning by count.
    // Create files (age does not matter).
    // Run the pruner FOR A SPECIFIC TABLE NAME - this
    // case is different, because it's supposed to have
    // 'count' retained for each table.
    // Expect the exact number of unpruned files, and
    // they should have 'recent' timestamps.
    @Test
    public void testUnbatchedByCount() throws Exception {
        populateUnbatched();
        Thread.sleep(10);
        String[] files1 = listDir("Before pruning");
        assertEquals("bug in setup", 8, files1.length);
        System.out.println("... Pruning ...");
        ExportFilePruner pruner = new ExportFilePruner(unbatchPattern("NONCE", "LISTE"), dateFmt);
        pruner.pruneByCount(baseDir, 2);
        String[] files2 = listDir("After pruning");
        assertEquals("unexpected pruned state", 6, files2.length); // TABLE:4 (unpruned) + LISTE:2 (pruned)
        checkResults(files2, unbatchPattern("NONCE", "LISTE"), unbatchPattern("NONCE", "TABLE"));
    }

    // Batched mode, pruning by count.
    // Create batch dirs (age does not matter).
    // Run the pruner.
    // Expect the exact number of unpruned files, and
    // they should have 'recent' timestamps.
    @Test
    public void testBatchedByCount() throws Exception {
        populateBatched();
        Thread.sleep(10);
        String[] files1 = listDir("Before pruning");
        assertEquals("bug in setup", 4, files1.length);
        System.out.println("... Pruning ...");
        ExportFilePruner pruner = new ExportFilePruner(batchPattern("NONCE"), dateFmt);
        pruner.pruneByCount(baseDir, 1);
        String[] files2 = listDir("After pruning");
        assertEquals("unexpected pruned state", 1, files2.length);
        checkResults(files2, batchPattern("NONCE"));
    }

}
