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

package org.voltdb.iv2;

import junit.framework.TestCase;

public class TestTxnEgo extends TestCase
{

    public void testValidSequenceId1()
    {
        try {
            new TxnEgo(0, 0);
        } catch (IllegalArgumentException e) {
           return;
        }
        fail();
    }


    public void testValidSequenceId2()
    {
        try {
            new TxnEgo(TxnEgo.SEQUENCE_MAX_VALUE + 1, 0);
        } catch (IllegalArgumentException e) {
           return;
        }
        fail();
    }

    public void testValidPartitionId1()
    {
        try {
            new TxnEgo(TxnEgo.SEQUENCE_ZERO, -1);
        } catch (IllegalArgumentException e) {
            return;
        }
        fail();
    }

    public void testValidPartitionId2()
    {
        try {
            new TxnEgo(TxnEgo.SEQUENCE_ZERO, TxnEgo.PARTITIONID_MAX_VALUE + 1);
        } catch (IllegalArgumentException e) {
            return;
        }
        fail();
    }

    public void testResonableEgo() throws Exception
    {
        long then = System.currentTimeMillis();
        Thread.sleep(100);
        TxnEgo te = new TxnEgo(TxnEgo.SEQUENCE_ZERO, 16000);
        assertEquals(TxnEgo.SEQUENCE_ZERO, te.getSequence());
        assertEquals(16000, te.getPartitionId());
    }

    public void testSequenceCorrectness() throws Exception
    {
        long sequence = TxnEgo.SEQUENCE_ZERO + 1L;
        TxnEgo te = new TxnEgo(sequence, 1);
        assertEquals(sequence, te.getSequence());
    }


}
