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

package org.voltdb.largequery;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

public class TestLargeBlockManagerSuite {

    private static Path m_tempDir = null;
    private static Path m_largeQuerySwapPath = null;

    @BeforeClass
    public static void setUp() throws IOException {
        m_tempDir = Files.createTempDirectory("TestLargeBlockManagerSuite");
        m_largeQuerySwapPath = m_tempDir.resolve("large_query_swap");
        Files.createDirectory(m_largeQuerySwapPath);
        LargeBlockManager.startup(m_largeQuerySwapPath);
    }

    @AfterClass
    public static void tearDown() throws IOException {
        assert (m_tempDir != null);
        FileUtils.deleteDirectory(m_tempDir.toFile());
        assert (! Files.exists(m_tempDir));
    }

    @After
    public void droolCheck() throws IOException {
        assertTrue(swapDirIsEmpty());
    }

    @Test
    public void testExecutorServiceInterface() throws Exception {
        LargeBlockManager lbm = LargeBlockManager.getInstance();
        assertNotNull(lbm);

        // Create a mocked large temp table block:
        int blockSize = 12 + 32; // block header and space for four longs
        long address = 0xDEADBEEF;
        ByteBuffer block = ByteBuffer.allocate(blockSize);
        block.putLong(address);  // original address of block in memory
        block.putInt(4);  // tuple count of block
        // Insert "data": this is not what actual data from the EE would look like here,
        // but it does not matter here.
        for (long i = 1000; i < 5000; i += 1000) {
            block.putLong(i);
        }

        // Store a block...
        BlockId blockId = new BlockId(555, 333);
        LargeBlockTask storeTask = LargeBlockTask.getStoreTask(blockId, block);
        Future<LargeBlockResponse> responseFuture = lbm.submitTask(storeTask);
        assertTrue(responseFuture.get().wasSuccessful());

        // Make sure we actually wrote something
        Path blockPath = lbm.makeBlockPath(blockId);
        assertThat(blockPath.toString(), endsWith("large_query_swap/555___333.block"));
        assertTrue(Files.exists(blockPath));

        // Load the block back into memory
        ByteBuffer loadedBlock = ByteBuffer.allocateDirect(blockSize);
        LargeBlockTask loadTask = LargeBlockTask.getLoadTask(blockId, loadedBlock);
        responseFuture = lbm.submitTask(loadTask);
        assertTrue(responseFuture.get().wasSuccessful());
        loadedBlock.position(0);
        long actualAddress = loadedBlock.getLong();
        assertEquals(address, actualAddress);
        int tupleCount = loadedBlock.getInt();
        assertEquals(4, tupleCount);
        for (long i = 1000;  i < 5000; i += 1000) {
            assertEquals(i, loadedBlock.getLong());
        }

        // Release the block.
        LargeBlockTask releaseTask = LargeBlockTask.getReleaseTask(blockId);
        responseFuture = lbm.submitTask(releaseTask);
        assertTrue(responseFuture.get().wasSuccessful());
    }

    @Test
    public void testShutDownAndStartUp() throws IOException {
        LargeBlockManager lbm = LargeBlockManager.getInstance();
        long[] ids = {11, 22, 33, 101, 202, 303, 505, 606, 707};
        long address = 0xDEADBEEF;

        int blockSize = 12 + 32; // block header and space for four longs
        for (long id : ids) {
            ByteBuffer block = ByteBuffer.allocate(blockSize);
            block.putLong(address);
            block.putInt(4);
            for (long i = 1000; i < 5000; i += 1000) {
                block.putLong(i);
            }

            lbm.storeBlock(new BlockId(id + 100, id), block);
        }

        for (long id : ids) {
            BlockId blockId = new BlockId(id+100, id);
            Path blockPath = lbm.makeBlockPath(blockId);
            assertThat(blockPath.toString(), endsWith("large_query_swap/" + (id + 100) + "___" + id + ".block"));
            assertTrue(Files.exists(blockPath));
        }

        // create another spurious file, just to show that shutdown will clean it up
        Path spuriousFile = lbm.makeBlockPath(new BlockId(0, 999));
        Files.createFile(spuriousFile);

        LargeBlockManager.shutdown();

        // All blocks and spurious file deleted:
        assertTrue(swapDirIsEmpty());

        // Recreate the file and show that starting up the block manager also deletes it
        Files.createFile(spuriousFile);

        LargeBlockManager.startup(m_largeQuerySwapPath);

        // Spurious file deleted again:
        assertTrue(swapDirIsEmpty());
    }

    @Test
    public void testErrors() throws Exception {
        LargeBlockManager lbm = LargeBlockManager.getInstance();

        int blockSize = 12 + 32; // block header and space for four longs
        long address = 0xDEADBEEF;

        ByteBuffer block = ByteBuffer.allocate(blockSize);
        block.putLong(address);
        block.putInt(4);
        for (long i = 1000; i < 5000; i += 1000) {
            block.putLong(i);
        }

        // Store a block...
        BlockId blockId = new BlockId(555, 555);
        LargeBlockTask storeTask = LargeBlockTask.getStoreTask(blockId, block);
        Future<LargeBlockResponse> responseFuture = lbm.submitTask(storeTask);
        assertTrue(responseFuture.get().wasSuccessful());

        Path blockPath = lbm.makeBlockPath(blockId);
        assertThat(blockPath.toString(), endsWith("large_query_swap/555___555.block"));
        assertTrue(Files.exists(blockPath));

        // Redundantly store a block (should fail)
        responseFuture = lbm.submitTask(storeTask);
        LargeBlockResponse response = responseFuture.get();
        assertFalse(response.wasSuccessful());
        assertThat(response.getException().getMessage(),
                containsString("Request to store block that is already stored"));

        LargeBlockTask loadTask = LargeBlockTask.getLoadTask(new BlockId(555, 444), block);
        responseFuture = lbm.submitTask(loadTask);
        response = responseFuture.get();
        assertFalse(response.wasSuccessful());
        assertThat(response.getException().getMessage(),
                containsString("Request to load block that is not stored: 555::444"));

        LargeBlockTask releaseTask = LargeBlockTask.getReleaseTask(new BlockId(110, 444));
        responseFuture = lbm.submitTask(releaseTask);
        response = responseFuture.get();
        assertFalse(response.wasSuccessful());
        assertThat(response.getException().getMessage(),
                containsString("Request to release block that is not stored: 110::444"));

        // Clean up
        releaseTask = LargeBlockTask.getReleaseTask(blockId);
        responseFuture = lbm.submitTask(releaseTask);
        assertTrue(responseFuture.get().wasSuccessful());
    }

    @Test
    public void testFilenames() {
        LargeBlockManager lbm = LargeBlockManager.getInstance();

        // Any value of long is a valid block ID.  Make sure that the block
        // manager formats the ID as unsigned because file names starting with
        // a "-" would be weird.

        Path path = lbm.makeBlockPath(new BlockId(0,  0));
        assertThat(path.toString(), endsWith("large_query_swap/0___0.block"));

        path = lbm.makeBlockPath(new BlockId(100, 1));
        assertThat(path.toString(), endsWith("large_query_swap/100___1.block"));

        path = lbm.makeBlockPath(new BlockId(Long.MAX_VALUE, Long.MAX_VALUE));
        assertThat(path.toString(), endsWith("large_query_swap/" + Long.MAX_VALUE + "___" + Long.MAX_VALUE + ".block"));

        path = lbm.makeBlockPath(new BlockId(-1, -1));
        BigInteger unsignedMinusOne = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
        assertThat(path.toString(), endsWith("large_query_swap/" + unsignedMinusOne.toString() + "___" + unsignedMinusOne.toString() + ".block"));

        path = lbm.makeBlockPath(new BlockId(Long.MIN_VALUE, Long.MIN_VALUE));
        BigInteger unsignedMinLong = BigInteger.ONE.shiftLeft(63);
        assertThat(path.toString(), endsWith("large_query_swap/" + unsignedMinLong + "___" + unsignedMinLong + ".block"));
    }

    private boolean swapDirIsEmpty() throws IOException {
        int count = 0;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(m_largeQuerySwapPath)) {
            Iterator<Path> it = dirStream.iterator();
            while (it.hasNext()) {
                it.next();
                ++count;
            }
        }

        return count == 0;
    }
}
