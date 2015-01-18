/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;

public class TestVoltTypeUtil extends TestCase
{
    public void testDetermineImplicitCastingExceptions()
    {
        // either INVALID results in VoltTypeException
        boolean caught = false;
        try
        {
            VoltTypeUtil.determineImplicitCasting(VoltType.INVALID,
                                                  VoltType.INTEGER);
        }
        catch (VoltTypeException e)
        {
            caught = true;
        }
        assertTrue("VoltType.INVALID failed to throw exception", caught);

        caught = false;
        try
        {
            VoltTypeUtil.determineImplicitCasting(VoltType.INTEGER,
                                                  VoltType.INVALID);
        }
        catch (VoltTypeException e)
        {
            caught = true;
        }
        assertTrue("VoltType.INVALID failed to throw exception", caught);

        // String and non-string throw VoltTypeException
        caught = false;
        try
        {
            VoltTypeUtil.determineImplicitCasting(VoltType.STRING,
                                                  VoltType.INTEGER);
        }
        catch (VoltTypeException e)
        {
            caught = true;
        }
        assertTrue("VoltType.STRING and non-string failed to throw exception",
                   caught);
    }

    public void testDecimalMixedTypes()
    {
        // only exact types can cast to decimal.
        boolean caught = false;
        try
        {
            VoltTypeUtil.determineImplicitCasting(VoltType.DECIMAL,
                                                  VoltType.FLOAT);
        }
        catch (VoltTypeException e)
        {
            caught = true;
        }

        assertTrue("VoltType.DECIMAL and VoltType.FLOAT threw" +
                   "exception", caught);

        // Check that DECIMAL + DECIMAL -> DECIMAL
        assertEquals(VoltTypeUtil.determineImplicitCasting(VoltType.DECIMAL,
                                                           VoltType.DECIMAL),
                                                           VoltType.DECIMAL);

        // D + SMALLINT = D
        assertEquals(VoltTypeUtil.determineImplicitCasting(VoltType.DECIMAL,
                                                           VoltType.SMALLINT),
                                                           VoltType.DECIMAL);
}

    public void testDetermineImplicitCastingNullWins()
    {
        VoltType[] types = { VoltType.BIGINT,
                             VoltType.DECIMAL,
                             VoltType.FLOAT,
                             VoltType.INTEGER,
                             VoltType.SMALLINT,
                             VoltType.STRING,
                             VoltType.TIMESTAMP,
                             VoltType.TINYINT };

        for (VoltType right : types)
        {
            assertEquals(VoltTypeUtil.determineImplicitCasting(VoltType.NULL,
                                                               right),
                         VoltType.NULL);
        }
    }

    public void testDetermineImplicitCastingOrder()
    {
        // Check that STRING + STRING -> STRING
        assertEquals(VoltTypeUtil.determineImplicitCasting(VoltType.STRING,
                                                           VoltType.STRING),
                     VoltType.STRING);

        // Check the easy non-coerced order
        VoltType[] winning_types = { VoltType.FLOAT,
                                     VoltType.TIMESTAMP,
                                     VoltType.BIGINT };
        VoltType[] losing_types = { VoltType.FLOAT,
                                    VoltType.TIMESTAMP,
                                    VoltType.BIGINT,
                                    VoltType.INTEGER,
                                    VoltType.SMALLINT,
                                    VoltType.TINYINT };
        for (int i = 0; i < winning_types.length; ++i)
        {
            for (int j = i; j < losing_types.length; ++j)
            {
                assertEquals(winning_types[i],
                             VoltTypeUtil.determineImplicitCasting(winning_types[i],
                                                                   losing_types[j]));
            }
        }

        // Finally, check the promotion of INT types if none of the winning types
        // was present
        VoltType[] promoted_types = { VoltType.INTEGER,
                                      VoltType.SMALLINT,
                                      VoltType.TINYINT };
        for (int i = 0; i < promoted_types.length; ++i)
        {
            for (int j = i; j < promoted_types.length; ++j)
            {
                assertEquals(VoltType.BIGINT,
                             VoltTypeUtil.determineImplicitCasting(promoted_types[i],
                                                                   promoted_types[j]));
            }
        }
    }
}
