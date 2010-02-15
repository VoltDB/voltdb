/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb;

import org.voltdb.ParameterSet;
import org.voltdb.messaging.FastSerializableTestUtil;

import junit.framework.TestCase;

public class TestParameterSet extends TestCase {
    ParameterSet params;
    public void setUp() {
        params = new ParameterSet();
    }

    public void testNull() {
        params.setParameters(new Object[]{null});

        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        assertEquals(1, out.toArray().length);
        assertNull(out.toArray()[0]);
    }

    public void testStrings() {
        params.setParameters(new Object[]{"foo"});
        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        assertEquals(1, out.toArray().length);
        assertEquals("foo", out.toArray()[0]);
    }

    public void testStringsAsByteArray() {
        params = new ParameterSet(true);
        params.setParameters(new Object[]{new byte[]{'f', 'o', 'o'}});
        ParameterSet out = FastSerializableTestUtil.roundTrip(params);
        assertEquals(1, out.toArray().length);

        // TODO(evanj): It would probably be best to use a byte array as the
        // "native" string format, and only convert to String where needed.
        // Hence, this probably should be a byte array.
        assertEquals("foo", out.toArray()[0]);
    }
}
