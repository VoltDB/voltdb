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

package org.voltdb.regressionsuites;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;


public class SaveRestoreBase extends RegressionSuite {
    protected static final String TMPDIR = "/tmp/" + System.getProperty("user.name");;
    protected static final String TMPDIR_MOVED = TMPDIR + "/moved";
    protected static final String TESTNONCE = "testnonce";
    protected static final String MAGICNONCE = "MANUAL";
    protected static final String JAR_NAME = "sysproc-threesites.jar";
    private static boolean s_cleanUp = true;

    public SaveRestoreBase(String s) {
        super(s);
    }

    @Override
    public void setUp() throws Exception
    {
        File tempDir = new File(TMPDIR);
        if (!tempDir.exists()) {
            assertTrue(tempDir.mkdirs());
        }
        if (s_cleanUp) {
            // Clenaup during the first setUp for the suite
            deleteTestFiles(TESTNONCE);
            s_cleanUp = false;
        }
        super.setUp();
        org.voltdb.sysprocs.SnapshotRegistry.clear();
    }

    @Override
    public void tearDown() throws Exception
    {
        super.tearDown();
        if (m_completeShutdown) {
            deleteTestFiles(TESTNONCE);
        }
        System.gc();
        System.runFinalization();
    }

    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            for (File f2 : f.listFiles()) {
                deleteRecursively(f2);
            }
            boolean deleted = f.delete();
            if (!deleted) {
                if (!f.exists()) return;
                System.err.println("Couldn't delete " + f.getPath());
                System.err.println("Remaining files are:");
                for (File f2 : f.listFiles()) {
                    System.err.println("    " + f2.getPath());
                }
                //Recurse until stack overflow trying to delete, y not rite?
                deleteRecursively(f);
            }
        } else  {
            boolean deleted = f.delete();
            if (!deleted) {
                if (!f.exists()) return;
                System.err.println("Couldn't delete " + f.getPath());
            }
            assertTrue(deleted);
        }
    }

    protected void deleteTestFiles(final String nonce)
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                return file.startsWith(nonce) ||
                file.endsWith(".vpt") ||
                file.endsWith(".digest") ||
                file.endsWith(".tsv") ||
                file.endsWith(".csv") ||
                file.endsWith(".incomplete") ||
                new File(dir, file).isDirectory();
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            deleteRecursively(tmp_file);
        }
    }

    static protected GeographyPointValue getGeographyPointValue(int index) {
        double lat = index % 90;
        double lng = (index * 2) % 180;
        if ((index & 0x1) != 0) {
            lat *= -1.0;
        }

        if ((index & 0x2) != 0) {
            lng *= -1.0;
        }

        return new GeographyPointValue(lng, lat);
    }

    static protected GeographyValue getGeographyValue(int index) {
        List<GeographyPointValue> loop = new ArrayList<GeographyPointValue>();
        for (int i = 0; i < 3; ++i) {
            loop.add(getGeographyPointValue(index + i));
        }
        loop.add(loop.get(0));

        List<List<GeographyPointValue>> loops = new ArrayList<>();
        loops.add(loop);

        // It would be nice to test multiple loops here, but to do it right
        // would require that the inner loops don't cross the outer one,
        // which is not trivial to do.

        return new GeographyValue(loops);
    }
}
