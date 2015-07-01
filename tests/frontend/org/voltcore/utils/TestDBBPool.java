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

package org.voltcore.utils;

import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.junit.Test;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.EELibraryLoader;

public class TestDBBPool extends TestCase {

    /*
     * Allocate 4 gigs of memory, should not OOM
     */
    @Test
    public void testBasicBehavior() {
        for (int ii = 0; ii < (2097152 * 2); ii++) {
            DBBPool.allocateDirect(1024).discard();
        }
    }

    @Test
    public void testChecksum() {
        EELibraryLoader.loadExecutionEngineLibrary(true);
        final long seed = System.currentTimeMillis();
        Random r = new Random(seed);
        System.out.println("Seed is " + seed);
        for (int ii = 0; ii < 10000; ii++) {
            int nextLength = r.nextInt(4096);
            byte bytes[] = new byte[nextLength];
            r.nextBytes(bytes);
            PureJavaCrc32C checksum = new PureJavaCrc32C();
            checksum.update(bytes);
            int javaSum = (int)checksum.getValue();
            BBContainer cont = DBBPool.allocateDirect(nextLength);
            cont.b().put(bytes);
            int cSum = DBBPool.getCRC32C(cont.address(), 0, nextLength);
            cont.discard();
            assertEquals(javaSum, cSum);
        }
    }
}
