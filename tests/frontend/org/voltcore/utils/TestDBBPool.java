/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltcore.utils.DBBPool.BBContainer;

public class TestDBBPool extends TestCase {

    final int NUM_BUFFERS = 10;
    final int SIZE1 = 8096;
    final int SIZE2 = 1337;

    public void testDefaultPoolBehavior() {
        assertEquals(0, DBBPool.getBytesAllocatedGlobally());
        BBContainer container1 = DBBPool.allocateDirect(SIZE1);
        assertEquals(SIZE1, DBBPool.getBytesAllocatedGlobally());
        container1.discard();

        BBContainer container2 = DBBPool.allocateDirect(SIZE1);
        // check that we don't allocate a new buffer but reuse the one we
        // just discarded
        assertEquals(SIZE1, DBBPool.getBytesAllocatedGlobally());
        // make sure second size works and is tracked separately
        container1 = DBBPool.allocateDirect(SIZE2);
        assertEquals(SIZE1 + SIZE2, DBBPool.getBytesAllocatedGlobally());
        container1.discard();
        assertEquals(SIZE1 + SIZE2, DBBPool.getBytesAllocatedGlobally());
        container1 = DBBPool.allocateDirect(SIZE2);
        assertEquals(SIZE1 + SIZE2, DBBPool.getBytesAllocatedGlobally());
    }
}
