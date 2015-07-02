/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.persist;

import org.hsqldb_voltpatches.lib.HsqlArrayList;

/**
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 1.9.0
 */
public class LobStoreMem implements LobStore {

    final int     lobBlockSize;
    int           blocksInLargeBlock = 128;
    int           largeBlockSize;
    HsqlArrayList byteStoreList;

    public LobStoreMem(int lobBlockSize) {

        this.lobBlockSize = lobBlockSize;
        largeBlockSize    = lobBlockSize * blocksInLargeBlock;
        byteStoreList     = new HsqlArrayList();
    }

    public byte[] getBlockBytes(int blockAddress, int blockCount) {

        byte[] dataBytes       = new byte[blockCount * lobBlockSize];
        int    dataBlockOffset = 0;

        while (blockCount > 0) {
            int    largeBlockIndex   = blockAddress / blocksInLargeBlock;
            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    blockOffset       = blockAddress % blocksInLargeBlock;
            int    currentBlockCount = blockCount;

            if ((blockOffset + currentBlockCount) > blocksInLargeBlock) {
                currentBlockCount = blocksInLargeBlock - blockOffset;
            }

            System.arraycopy(largeBlock, blockOffset * lobBlockSize,
                             dataBytes, dataBlockOffset * lobBlockSize,
                             currentBlockCount * lobBlockSize);

            blockAddress    += currentBlockCount;
            dataBlockOffset += currentBlockCount;
            blockCount      -= currentBlockCount;
        }

        return dataBytes;
    }

    public void setBlockBytes(byte[] dataBytes, int blockAddress,
                              int blockCount) {

        int dataBlockOffset = 0;

        while (blockCount > 0) {
            int largeBlockIndex = blockAddress / blocksInLargeBlock;

            if (largeBlockIndex >= byteStoreList.size()) {
                byteStoreList.add(new byte[largeBlockSize]);
            }

            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    blockOffset       = blockAddress % blocksInLargeBlock;
            int    currentBlockCount = blockCount;

            if ((blockOffset + currentBlockCount) > blocksInLargeBlock) {
                currentBlockCount = blocksInLargeBlock - blockOffset;
            }

            System.arraycopy(dataBytes, dataBlockOffset * lobBlockSize,
                             largeBlock, blockOffset * lobBlockSize,
                             currentBlockCount * lobBlockSize);

            blockAddress    += currentBlockCount;
            dataBlockOffset += currentBlockCount;
            blockCount      -= currentBlockCount;
        }
    }

    public void setBlockBytes(byte[] dataBytes, long position, int offset,
                              int length) {

        while (length > 0) {
            int largeBlockIndex = (int) (position / largeBlockSize);

            if (largeBlockIndex >= byteStoreList.size()) {
                byteStoreList.add(new byte[largeBlockSize]);
            }

            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    offsetInLargeBlock = (int) (position % largeBlockSize);
            int    currentLength      = length;

            if ((offsetInLargeBlock + currentLength) > largeBlockSize) {
                currentLength = largeBlockSize - offsetInLargeBlock;
            }

            System.arraycopy(dataBytes, offset, largeBlock,
                             offsetInLargeBlock, currentLength);

            position += currentLength;
            offset   += currentLength;
            length   -= currentLength;
        }
    }

    public int getBlockSize() {
        return lobBlockSize;
    }

    public long getLength() {
        return (long) byteStoreList.size() * largeBlockSize;
    }

    public void setLength(long length) {

        int largeBlockIndex = (int) (length / largeBlockSize);

        byteStoreList.setSize(largeBlockIndex + 1);
    }

    public void close() {
        byteStoreList.clear();
    }

    public void synch() {}
}
