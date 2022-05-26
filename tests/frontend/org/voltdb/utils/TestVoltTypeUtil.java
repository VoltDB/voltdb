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

package org.voltdb.utils;

import junit.framework.TestCase;

import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;

public class TestVoltTypeUtil extends TestCase
{
    public void testDetermineImplicitCastingExceptions()
    {
        // either INVALID results in VoltTypeException
        validateVoltTypeCastingException(VoltType.INVALID, VoltType.INTEGER);
        validateVoltTypeCastingException(VoltType.INTEGER, VoltType.INVALID);

        // String and non-string throw VoltTypeException
        validateVoltTypeCastingException(VoltType.STRING, VoltType.INTEGER);
        validateVoltTypeCastingException(VoltType.INTEGER, VoltType.STRING);
    }

    public void testDecimalMixedTypes()
    {
        validateVoltTypeCastingException(VoltType.DECIMAL, VoltType.STRING);
        validateVoltTypeCastingException(VoltType.STRING, VoltType.DECIMAL);
        validateVoltTypeCastingException(VoltType.DECIMAL, VoltType.TIMESTAMP);

        // Check that DECIMAL + FLOAT -> FLOAT
        assertEquals(VoltType.FLOAT,
                VoltTypeUtil.determineImplicitCasting(VoltType.DECIMAL,VoltType.FLOAT));

        // Check that FLOAT + DECIMAL -> FLOAT
        assertEquals(VoltType.FLOAT,
                VoltTypeUtil.determineImplicitCasting(VoltType.FLOAT,VoltType.DECIMAL));

        // Check that DECIMAL + DECIMAL -> DECIMAL
        assertEquals(VoltType.DECIMAL,
                VoltTypeUtil.determineImplicitCasting(VoltType.DECIMAL,VoltType.DECIMAL));

        // D + SMALLINT = D
        assertEquals(VoltType.DECIMAL,
                VoltTypeUtil.determineImplicitCasting(VoltType.DECIMAL,VoltType.SMALLINT));
    }

    private void validateVoltTypeCastingException(VoltType t1, VoltType t2) {
        try
        {
            VoltTypeUtil.determineImplicitCasting(t1,t2);
            fail();
        }
        catch (VoltTypeException e)
        {
            assertTrue(e.getMessage().contains(String.format(VoltTypeUtil.VoltTypeCastErrorMessage, t1, t2)));
        }
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
            assertEquals(VoltType.NULL,
                    VoltTypeUtil.determineImplicitCasting(VoltType.NULL,right));
        }
    }

    public void testDetermineImplicitCastingOrder()
    {
        // Check that STRING + STRING -> STRING
        assertEquals(VoltType.STRING,
                VoltTypeUtil.determineImplicitCasting(VoltType.STRING,VoltType.STRING));

        // Check the easy non-coerced order
        VoltType[] winning_types = { VoltType.FLOAT,
                                     VoltType.BIGINT };
        VoltType[] losing_types = { VoltType.FLOAT,
                                    VoltType.BIGINT,
                                    VoltType.INTEGER,
                                    VoltType.SMALLINT,
                                    VoltType.TINYINT };
        for (int i = 0; i < winning_types.length; ++i)
        {
            for (int j = i; j < losing_types.length; ++j)
            {
                assertEquals(winning_types[i],
                             VoltTypeUtil.determineImplicitCasting(winning_types[i],losing_types[j]));
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
                             VoltTypeUtil.determineImplicitCasting(promoted_types[i],promoted_types[j]));
            }
        }

        assertEquals(VoltType.TIMESTAMP, VoltTypeUtil.determineImplicitCasting(VoltType.TIMESTAMP,VoltType.TIMESTAMP));

        // check the invalid timestamp type operation
        for (int i = 0; i < losing_types.length; ++i) {
            validateVoltTypeCastingException(VoltType.TIMESTAMP, losing_types[i]);
            validateVoltTypeCastingException(losing_types[i], VoltType.TIMESTAMP);
        }
    }
}
