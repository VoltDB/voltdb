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

import java.util.BitSet;

/**
 * A class performing fast verifications using a {@link BitSet} instance in memory. This
 * verifier is limited to verifying keys < {@code Integer.MAX_VALUE}
 */
public class FastVerifier extends BaseVerifier {
    private final long m_maxKey;
    private volatile BitSet m_bitSet;

    public FastVerifier(long maxKey) {
        m_maxKey = maxKey;
        int maxIndex = indexFromKey(m_maxKey);
        m_bitSet = new BitSet(maxIndex);
    }

    @Override
    public boolean addKey(long key) throws IllegalArgumentException, IndexOutOfBoundsException {
        int index = indexFromKey(key);
        synchronized (this) {
            if (m_bitSet.get(index)) {
                ++m_dupCount;
                return false;
            }
            else {
                m_bitSet.set(index);
                ++m_setCount;
                return true;
            }
        }
    }

    @Override
    public synchronized boolean hasGaps() {
        // We have a gap if number if bits set < bitset size
        int maxIndex = indexFromKey(m_maxKey);
        return cardinality() < maxIndex;
    }

    private int indexFromKey(long key) throws IllegalArgumentException {
        if (key < 0 || key > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Illegal key: " + key);
        }
        return (int) key;
    }
}
