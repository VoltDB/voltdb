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

package org.voltdb.iv2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import static org.mockito.Mockito.*;

import org.voltcore.messaging.VoltMessage;

import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import junit.framework.TestCase;
import org.junit.Test;

public class TestTerm extends TestCase
{
    Iv2RepairLogResponseMessage makeResponse(long spHandle)
    {
        Iv2RepairLogResponseMessage m = mock(Iv2RepairLogResponseMessage.class);
        when(m.getSpHandle()).thenReturn(spHandle);
        return m;
    }

    @Test
    public void testUnion() throws Exception
    {
        Term term = new Term(null, 0, 0L, null);

        // returned sphandles in a non-trivial order, with duplicates.
        long returnedSpHandles[] = new long[]{1L, 5L, 2L, 5L, 6L, 3L, 5L, 1L};
        long expectedUnion[] = new long[]{1L, 2L, 3L, 5L, 6L};

        for (long sp : returnedSpHandles) {
            term.m_repairLogUnion.add(makeResponse(sp));
        }

        assertEquals(expectedUnion.length, term.m_repairLogUnion.size());

        int i = 0;
        for (Iv2RepairLogResponseMessage li : term.m_repairLogUnion) {
            assertEquals(li.getSpHandle(), expectedUnion[i++]);
        }
    }

}

