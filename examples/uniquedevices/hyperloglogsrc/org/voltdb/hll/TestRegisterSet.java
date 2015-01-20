/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
/*
 * Copyright (C) 2012 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* This code was originally sourced from https://github.com/addthis/stream-lib
   in December 2014. */

package org.voltdb.hll;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestRegisterSet {

    @Test
    public void testBits() {
        final int RS_COUNT = 33;
        RegisterSet rs = new RegisterSet(RS_COUNT);
        for (int i = 0; i < RS_COUNT * RegisterSet.REGISTER_SIZE; i++) {
            System.out.printf("%d\n", i);
            System.out.flush();

            rs.setBit(i, i % 2);
            for (int j = 0; j <= i; j++) {
                assertEquals(j % 2, rs.getBit(j));
            }
        }

        for (int i = 0; i < RS_COUNT * RegisterSet.REGISTER_SIZE; i++) {
            System.out.printf("%d\n", i);
            System.out.flush();

            rs.setBit(i, (i % 2) == 0 ? 1 : 0);
            for (int j = 0; j <= i; j++) {
                assertEquals((j % 2) == 0 ? 1 : 0, rs.getBit(j));
            }
        }
    }

    @Test
    public void testBasic() {
        final int RS_COUNT = 128;
        RegisterSet rs = new RegisterSet(RS_COUNT);
        for (int i = 0; i < RS_COUNT; i++) {
            System.out.printf("%d\n", i);
            System.out.flush();

            rs.set(i, i % 32);
            for (int j = 0; j <= i; j++) {
                assertEquals(j % 32, rs.get(j));
            }
        }
    }

    @Test
    public void testUpdateIfGreater() {
        final int RS_COUNT = 128;
        RegisterSet rs = new RegisterSet(RS_COUNT);

        // set all ones
        for (int i = 0; i < RS_COUNT; i++) {
            rs.set(i, 1);
        }

        // update every other to 5s
        for (int i = 0; i < RS_COUNT; i++) {
            if (i % 2 == 0) {
                rs.set(i, 5);
            }
        }

        // update all to 3s if greater
        for (int i = 0; i < RS_COUNT; i++) {
            rs.updateIfGreater(i, 3);
        }

        // verify all 3s and 5s
        for (int i = 0; i < RS_COUNT; i++) {
            if (i % 2 == 0) {
                assertEquals(5, rs.get(i));
            }
            else {
                assertEquals(3, rs.get(i));
            }
        }
    }

}
