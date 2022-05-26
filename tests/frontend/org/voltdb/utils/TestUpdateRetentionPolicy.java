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
package org.voltdb.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltdb.utils.BinaryDeque.RetentionPolicyType;
import org.voltdb.utils.BinaryDeque.UpdateResult;

public class TestUpdateRetentionPolicy {
    private static final VoltLogger s_log = new VoltLogger(TestUpdateRetentionPolicy.class.getSimpleName());

    @Rule
    public final TemporaryFolder m_dir = new TemporaryFolder();
    @Rule
    public final TestName m_name = new TestName();

    private PersistentBinaryDeque<Object> m_pbd;

    @Before
    public void setup() throws Exception {
        PersistentBinaryDeque.setupRetentionPolicyMgr(1, 1);

        m_pbd = PersistentBinaryDeque.builder(m_name.getMethodName(), m_dir.getRoot(), s_log)
                .initialExtraHeader(this, new BinaryDequeSerializer<Object>() {
                    @Override
                    public int getMaxSize(Object object) throws IOException {
                        return 0;
                    }

                    @Override
                    public void write(Object object, ByteBuffer buffer) throws IOException {}

                    @Override
                    public Object read(ByteBuffer buffer) throws IOException {
                        return null;
                    }
                }).build();
    }

    @After
    public void tearDown() throws Exception {
        m_pbd.close();
    }

    /*
     * Simple test that update runs entry updater when it is supposed to
     */
    @Test(timeout = 30_000)
    public void simpleUpdate() throws Exception {
        m_pbd.setRetentionPolicy(RetentionPolicyType.UPDATE, m_supplier, 125L);
        assertFalse(m_pbd.isRetentionPolicyEnforced());
        m_pbd.startRetentionPolicyEnforcement();
        assertTrue(m_pbd.isRetentionPolicyEnforced());

        for (int i = 0; i < 10; ++i) {
            m_pbd.offer(DBBPool.allocateDirect(1024));
        }

        m_pbd.updateExtraHeader(this);

        assertTrue(waitForCount(11, 30_000));

        for (int i = 0; i < 10; ++i) {
            m_pbd.offer(DBBPool.allocateDirect(1024));
        }

        // Assert that the count does not increase until after a new segment is created
        assertFalse(waitForCount(12, 500));

        m_pbd.updateExtraHeader(this);

        // Assert that count increases after new segment is created
        assertTrue(waitForCount(33, 30_000));

        // Validate that count does not go up when retention policy is stopped
        m_pbd.stopRetentionPolicyEnforcement();
        assertFalse(m_pbd.isRetentionPolicyEnforced());

        for (int i = 0; i < 10; ++i) {
            m_pbd.offer(DBBPool.allocateDirect(1024));
        }

        m_pbd.updateExtraHeader(this);

        // Assert that the count does not increase until after a new segment is created
        assertFalse(waitForCount(34, 500));
    }

    private Supplier<CountingUpdator> m_supplier = CountingUpdator::new;

    MutableLong m_count = new MutableLong();

    boolean waitForCount(int count, long waitMs) throws InterruptedException {
        synchronized (m_count) {
            if (m_count.longValue() < count) {
                m_count.wait(waitMs);
            }
        }
        return m_count.longValue() >= count;
    }

    private class CountingUpdator implements BinaryDeque.EntryUpdater<Object> {
        @Override
        public UpdateResult update(Object metadata, ByteBuffer entry) {
            m_count.increment();
            return UpdateResult.KEEP;
        }

        @Override
        public void close() {
            synchronized (m_count) {
                m_count.notifyAll();
            }
        }
    }
}
