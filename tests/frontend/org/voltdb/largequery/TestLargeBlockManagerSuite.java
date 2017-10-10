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

package org.voltdb.largequery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.utils.VoltFile;

public class TestLargeBlockManagerSuite {

    private static Path m_tempDir = null;

    @BeforeClass
    public static void setUp() throws IOException {
        m_tempDir = Files.createTempDirectory("TestLargeBlockManagerSuite");
        assertTrue(Files.exists(m_tempDir));
    }

    @AfterClass
    public static void tearDown() throws IOException {
        if (m_tempDir != null) {
            VoltFile.recursivelyDelete(m_tempDir);
            assert(! Files.exists(m_tempDir));
        }
    }

    @Test
    public void testBasic() throws IOException {
        assert (m_tempDir != null);
        assertTrue(Files.exists(m_tempDir));
        Path lttSwapPath = m_tempDir.resolve("large_query_swap");

        Files.createDirectory(lttSwapPath);

        LargeBlockManager lbm = new LargeBlockManager(lttSwapPath);
        assertNotNull(lbm);

        ByteBuffer block = ByteBuffer.allocate(32); // space for four longs
        for (long i = 1000; i < 5000; i += 1000) {
            block.putLong(i);
        }

        // Store a block...
        long blockId = 333;
        lbm.storeBlock(blockId, block);

        Path blockPath = lbm.makeBlockPath(blockId);
        assertTrue(blockPath.toString().endsWith("large_query_swap/333.lttblock"));
        assertTrue(Files.exists(blockPath));

        try {
            // Redundantly store a block (should fail)
            lbm.storeBlock(blockId, block);
            fail("Expected redundant store to throw an exception");
        }
        catch (IllegalArgumentException iac) {
            assert(iac.getMessage().contains("Request to store block that is already stored"));
        }

        ByteBuffer loadedBlock = ByteBuffer.allocateDirect(32);
        lbm.loadBlock(blockId, loadedBlock);

        loadedBlock.position(0);
        for (long i = 1000;  i < 5000; i += 1000) {
            assertEquals(i, loadedBlock.getLong());
        }
    }
}
