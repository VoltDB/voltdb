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

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMinimumRatioMaintainer {
    @Test
    public void testMRM() throws Exception {
        MinimumRatioMaintainer mrm = new MinimumRatioMaintainer(0.5);

        //First check that I can't do more restricted without breaking the 50/50 balance
        assertFalse(mrm.canDoRestricted());

        //Check that doing unrestricted allows restricted
        mrm.didUnrestricted();
        assertTrue(mrm.canDoRestricted());

        //And that doing the restricted prohibits doing another
        mrm.didRestricted();
        assertFalse(mrm.canDoRestricted());

        //Do a bunch of unrestricteds and valudate that they allow us
        //to do the same number of restricted
        for (int ii = 0; ii < 10; ii++) {
            mrm.didUnrestricted();
        }
        for (int ii = 0; ii < 10; ii++) {
            assertTrue(mrm.canDoRestricted());
            mrm.didRestricted();
        }

        //Now we shouldn't be able to anymore
        assertFalse(mrm.canDoRestricted());

        //Ignore the contract and do too many restricted
        for (int ii = 0; ii < 10; ii++) {
            assertFalse(mrm.canDoRestricted());
            mrm.didRestricted();
        }
        assertFalse(mrm.canDoRestricted());

        //Now check that the same number of unrestricted are necessary
        //for it to allow restricted again
        for (int ii = 0; ii < 9; ii++) {
            mrm.didUnrestricted();
            assertFalse(mrm.canDoRestricted());
        }
        mrm.didUnrestricted();
        mrm.didUnrestricted();
        assertTrue(mrm.canDoRestricted());
    }
}
