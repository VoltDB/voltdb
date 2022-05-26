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
 * A base verifier of the keys polled by TopicBenchmark2 subscriber threads allowing verifying
 * that all the expected records were successfully polled. The verifier may be updated from
 * multiple threads.
 * <p>
 * NOTE: the verifier assumes that TopicBenchmark2 client is reading the topic from the beginning,
 * i.e. from key == 1 and expects a final tally of [1, expected-count] row identifiers.
 */
public abstract class BaseVerifier {
    protected volatile long m_setCount;
    protected volatile long m_dupCount;

    public long cardinality() {
        return m_setCount;
    }

    public long duplicates() {
        return m_dupCount;
    }

    /**
     * Add a key to the tracker, or increment duplicates if already present
     *
     * @param key   the key to track
     * @return {@code true} if key was added otherwise duplicate key
     */
    public abstract boolean addKey(long key) throws IllegalArgumentException, IndexOutOfBoundsException;

    /**
     * @return {@code true} if the verifier has detected gaps
     */
    public abstract boolean hasGaps();
}
