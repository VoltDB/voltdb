/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

package org.voltdb_hll;

public class RegisterSet {

    public final static int REGISTER_SIZE = 5;

    private final byte[] bytes;
    private boolean dirty = false;

    public RegisterSet(int count) {
        int bitsNeeded = count * REGISTER_SIZE;
        int bytesNeeded = bitsNeeded / 8;
        if ((bitsNeeded % 8) != 0) {
            bytesNeeded += 1;
        }
        this.bytes = new byte[bytesNeeded];
    }

    public RegisterSet(byte[] bytes) {
        this.bytes = bytes;
    }

    public int count() {
        return (bytes.length * 8) / REGISTER_SIZE;
    }

    public int size() {
        return bytes.length;
    }

    public boolean getDirty() {
        return dirty;
    }

    int getBit(final int bitPos) {
        // bitPos >>> 3 == bitPos / 8
        // bitpos & 7 == bitpos % 8
        final byte b = bytes[bitPos >>> 3];
        return (b >>> (bitPos & 7)) & 1;
    }

    void setBit(int bitPos, int value) {
        // bitPos >>> 3 == bitPos / 8
        // bitpos & 7 == bitpos % 8
        if (value > 0) {
            bytes[bitPos >>> 3] |= 1 << (bitPos & 7);
        }
        else {
            bytes[bitPos >>> 3] &= ~(1 << (bitPos & 7));
        }
    }

    public void set(int position, int value) {
        assert (position < count());
        assert (value >= 0);
        assert (value < (1 << REGISTER_SIZE));

        dirty = true;
        int bitPos = position * REGISTER_SIZE;
        for (int i = 0; i < REGISTER_SIZE; i++) {
            setBit(bitPos + i, value & (1 << i));
        }
    }

    public int get(final int position) {
        assert (position < count());

        int retval = 0;
        final int bitPos = position * REGISTER_SIZE;
        for (int i = 0; i < REGISTER_SIZE; i++) {
            retval |= getBit(bitPos + i) << i;
        }
        return retval;
    }

    public boolean updateIfGreater(int position, int value) {
        assert (position < count());
        assert (value >= 0);
        assert (value < (1 << REGISTER_SIZE));

        int oldVal = get(position);
        if (value > oldVal) {
            set(position, value);
            return true;
        }
        return false;
    }

    public void merge(RegisterSet that) {
        assert(that.size() == this.size());

        int count = count();
        for (int i = 0; i < count; i++) {
            int thisVal = this.get(i);
            int thatVal = that.get(i);
            if (thisVal < thatVal) set(i, thatVal);
        }
    }

    byte[] toBytes() {
        return bytes;
    }
}
