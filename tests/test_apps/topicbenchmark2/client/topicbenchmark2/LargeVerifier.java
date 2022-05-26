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
package topicbenchmark2;

/**
 * A class performing large verifications using a set of {@link BitSet} in memory.This
 * verifier is limited to verifying keys < ({@code Integer.MAX_VALUE} * 4096).
 * <p>
 * The verifier operates in blocks and frees blocks that were completely verified in
 * order to try minimizing the memory footprint.
 */
public class LargeVerifier extends BaseVerifier {

    private static int BLOCK_SIZE = 4 * 1024;

    private final long m_maxKey;
    private final int m_lastBlockIndex;
    private final int m_lastBlockSize;
    private final FastVerifier[] m_blocks;

    private volatile int m_curBlockIndex;

    public LargeVerifier(long maxKey) {
        if (maxKey <= 0) {
            throw new IllegalArgumentException("Invalid maxKey: " + maxKey);
        }
        m_maxKey = maxKey;
        long lastBlockIndex = maxKey / BLOCK_SIZE;
        if (lastBlockIndex > Integer.MAX_VALUE) {
            // Are you crazy? Increase block size.
            throw new IllegalArgumentException("Too many blocks: " + lastBlockIndex + 1);
        }
        m_lastBlockIndex = (int) lastBlockIndex;
        m_lastBlockSize = (int) (maxKey % BLOCK_SIZE);

        m_blocks = new FastVerifier[m_lastBlockIndex + 1];
        m_blocks[0] = new FastVerifier(getBlockSize(0));
        m_curBlockIndex = 0;
    }

    private int getBlockSize(int blockIndex) {
        return blockIndex == m_lastBlockIndex ? m_lastBlockSize : BLOCK_SIZE;
    }

    @Override
    public boolean addKey(long key) throws IllegalArgumentException, IndexOutOfBoundsException {
        if (key < 0 || key >= m_maxKey) {
            throw new IllegalArgumentException("illegal key: " + key);
        }

        int blockIndex = (int) (key / BLOCK_SIZE);
        int keyIndex = (int) (key % BLOCK_SIZE);

        boolean added = false;
        synchronized(this) {
            if (blockIndex <= m_curBlockIndex) {
                // Existing block
                FastVerifier verifier = m_blocks[blockIndex];
                if (verifier != null) {
                    added = verifier.addKey(keyIndex);
                    if (added) {
                        ++m_setCount;
                    }
                    else {
                        ++m_dupCount;
                    }
                }
                else {
                    // Out-of-order key on block already closed = duplicate
                    ++m_dupCount;
                }
            }
            else {
                // New block - see if previous blocks can be closed and memory freed
                for (int i = 0; i <= m_curBlockIndex; i++) {
                    FastVerifier v = m_blocks[i];
                    if (v != null) {
                        if (v.cardinality() == getBlockSize(i)) {
                            // Block is full we can get rid of it
                            m_blocks[i] = null;
                        }
                    }
                }

                // Create new blocks
                for (int i = m_curBlockIndex + 1; i <= blockIndex; i++) {
                    m_blocks[i] = new FastVerifier(getBlockSize(i));
                }
                m_curBlockIndex = blockIndex;
                FastVerifier v = m_blocks[m_curBlockIndex];
                added = v.addKey(keyIndex);
                ++m_setCount;
            }
        }
        return added;
    }

    @Override
    public boolean hasGaps() {
        for (int i = 0; i <= m_curBlockIndex; i++) {
            FastVerifier v = m_blocks[i];
            if (v != null && v.hasGaps()) {
                return true;
            }
        }
        return false;
    }

}
