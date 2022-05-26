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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSnapshotArchiver {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private String baseSnap;

    private String fakeSnap(int n, boolean empty) throws IOException {
        String name = "fakesnap" + (n > 0 ? "." + n : "");
        File snap = tmp.newFolder(name);
        String fname = snap.getAbsolutePath() + "/x" + name;
        if (!empty) {
            if (!(new File(fname)).mkdir()) {
                throw new IOException("mkdir: " + fname);
            }
        }
        return snap.getAbsolutePath();
    }

    private void populate(int retain) throws IOException {
        baseSnap = fakeSnap(0, false);
        System.out.printf("-=-=- Snapshot dir: %s -=-=-\n", baseSnap);
        for (int n=1; n<=retain; n++) {
            fakeSnap(n, false);
        }
    }

    private void populateNoCurrent(int retain) throws IOException {
        baseSnap = fakeSnap(0, true); // empty
        System.out.printf("-=-=- Snapshot dir: %s -=-=-\n", baseSnap);
        for (int n=1; n<=retain; n++) {
            fakeSnap(n, false);
        }
    }

    private boolean archiveExists(int n) {
        String name = baseSnap + "." + n;
        return (new File(name)).exists();
    }

    private int countArchives() {
        int count = 0;
        while (archiveExists(count+1)) {
            count++;
        }
        return count;
    }

    private void execTest(int retain, String name) {
        System.out.printf("Execute archiver for %s\n", name);
        try {
            SnapshotArchiver.archiveSnapshotDirectory(baseSnap, retain);
        }
        catch (Exception ex) {
            System.out.printf("**** EXCEPTION in %s : %s\n", name, ex);
            System.err.printf("**** EXCEPTION in %s : %s\n", name, ex);
            ex.printStackTrace();
            fail();
        }
    }

    // Fully populated (archives 1,2,3) before.
    // Run archiver.
    // Expect same count afterwards (old 3 deleted, one new archive).
    @Test
    public void testSimple() throws IOException {
        int retain = 3;
        populate(retain);
        assertEquals("bad count before", retain, countArchives());
        execTest(retain, "testSimple");
        assertEquals("bad count after", retain, countArchives());
    }

    // Archive 1 before, empty current snap dir.
    // Run archiver.
    // Expect 1 archive after (no new archive created).
    @Test
    public void testNoCurrentSnap() throws IOException {
        int initial = 1, retain = 9;
        populateNoCurrent(initial);
        assertEquals("bad count before", initial, countArchives());
        execTest(retain, "testNoCurrentSnap");
        assertEquals("bad count after", initial, countArchives());
    }

    // Fully populated (archives 1,2) before.
    // Run archiver.
    // Expect no archives after (all deleted because retain=0).
    @Test
    public void testZero() throws IOException {
        int initial = 2, retain = 0;
        populate(initial);
        assertEquals("bad count before", initial, countArchives());
        execTest(retain, "testZero");
        assertEquals("bad count after", retain, countArchives());
    }
}
