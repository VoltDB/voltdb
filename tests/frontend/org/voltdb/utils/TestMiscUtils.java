/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import com.google.common.primitives.Ints;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestMiscUtils {
    @Test
    public void testZipSameKeyValueCount()
    {
        zipKeysAndValuesAndCheck(new int[] {1, 2, 3},
                                 new int[] {101, 102, 103});
    }

    @Test
    public void testZipMoreValuesThanKeys()
    {
        zipKeysAndValuesAndCheck(new int[] {1, 2, 3},
                                 new int[] {101, 102, 103, 104, 105, 106, 107});
    }

    @Test
    public void testZipMoreKeysThanValues()
    {
        zipKeysAndValuesAndCheck(new int[] {1, 2, 3, 4, 5, 6},
                                 new int[] {101, 102, 103});
    }

    @Test
    public void testZipNoKeys()
    {
        zipKeysAndValuesAndCheck(new int[] {},
                                 new int[] {101, 102, 103, 104, 105, 106, 107});
    }

    @Test
    public void testZipNoValues()
    {
        zipKeysAndValuesAndCheck(new int[]{1, 2, 3},
                                 new int[]{});
    }

    @Test
    public void testZipNoKeysNoValues()
    {
        zipKeysAndValuesAndCheck(new int[] {},
                                 new int[] {});
    }

    private static void zipKeysAndValuesAndCheck(int[] keys, int[] values)
    {
        Map<Integer, Collection<Integer>> map = MiscUtils.zipToMap(Ints.asList(keys),
                                                                   Ints.asList(values));

        if (keys.length == 0 || values.length == 0) {
            assertNull(map);
            return;
        }

        for (int i = 0; i < keys.length; i++) {
            if (i < values.length) {
                // value should be mapped to the key
                assertTrue(map.get(keys[i]).contains(values[i]));
            } else {
                // more keys than values, key should not be in the map
                assertFalse(map.containsKey(keys[i]));
            }
        }

        if (values.length > keys.length) {
            // more values than keys, rest of the values should be assigned to the first key
            for (int i = keys.length; i < values.length; i++) {
                assertTrue(map.get(keys[0]).contains(values[i]));
            }
        }
    }
}
