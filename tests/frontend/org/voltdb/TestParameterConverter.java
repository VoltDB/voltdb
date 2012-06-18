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


package org.voltdb;
import junit.framework.TestCase;


public class TestParameterConverter extends TestCase
{
    // Tests use naming convention:
    // (invocation deserialized type) To (stored procedure parameter type)

    public void testIntegerToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(true, false, int.class, null, new Integer(1));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1, ((Integer)r).intValue());
    }

    public void testIntToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(true, false, int.class, null, 2);
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(2, ((Integer)r).intValue());
    }

    public void testStringToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(true, false, int.class, null, "1000");
        assertTrue("exepct integer", r.getClass() == Integer.class);
        assertEquals(1000, ((Integer)r).intValue());
    }
}
