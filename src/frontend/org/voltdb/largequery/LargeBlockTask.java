/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltdb.largequery;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

/**
 * This is the superclass that represents large block tasks, of which there are three kinds:
 * - store:    store a provided large block to disk
 * - load:     loads a previously stored large block from disk
 * - release:  deletes a stored large block from disk
 * LargeBlockTasks can be submitted to the LargeBlockManager for execution.
 */
public abstract class LargeBlockTask implements Callable<LargeBlockResponse> {

    /**
     * Get a new "store" task
     * @param blockId   The block id of the block to store
     * @param block     A ByteBuffer containing the block data
     * @return  An instance of LargeBlockTask that will store a block
     */
    public static LargeBlockTask getStoreTask(BlockId blockId, ByteBuffer block) {
        return new LargeBlockTask() {
            @Override
            public LargeBlockResponse call() throws Exception {
                Exception theException = null;
                try {
                    LargeBlockManager.getInstance().storeBlock(blockId, block);
                }
                catch (Exception exc) {
                    theException = exc;
                }

                return new LargeBlockResponse(theException);
            }
        };
    }

    /**
     * Get a new "release" task
     * @param blockId   The block id of the block to release
     * @return  An instance of LargeBlockTask that will release a block
     */
    public static LargeBlockTask getReleaseTask(BlockId blockId) {
        return new LargeBlockTask() {
            @Override
            public LargeBlockResponse call() throws Exception {
                Exception theException = null;
                try {
                    LargeBlockManager.getInstance().releaseBlock(blockId);
                }
                catch (Exception exc) {
                    theException = exc;
                }

                return new LargeBlockResponse(theException);
            }
        };
    }

    /**
     * Get a new "load" task
     * @param blockId   The block id of the block to load
     * @param block     A ByteBuffer to write data intox
     * @return  An instance of LargeBlockTask that will load a block
     */
    public static LargeBlockTask getLoadTask(BlockId blockId, ByteBuffer block) {
        return new LargeBlockTask() {
            @Override
            public LargeBlockResponse call() throws Exception {
                Exception theException = null;
                try {
                    LargeBlockManager.getInstance().loadBlock(blockId, block);
                }
                catch (Exception exc) {
                    theException = exc;
                }

                return new LargeBlockResponse(theException);
            }
        };
    }
}
