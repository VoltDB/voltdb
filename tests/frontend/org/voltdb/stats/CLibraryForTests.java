/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.stats;

import com.sun.jna.Native;
import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class CLibraryForTests {

    public static final int RLIMIT_NOFILE_LINUX = 7;
    public static final int RLIMIT_NOFILE_MAC_OS_X = 8;
    public static final int RLIMIT_NOFILE;

    static {
        Native.register("c");

        RLIMIT_NOFILE = System.getProperty("os.name").equals("Linux") ? RLIMIT_NOFILE_LINUX : RLIMIT_NOFILE_MAC_OS_X;
    }

    public static final class Rlimit extends Structure {
        public long rlim_cur = 0;
        public long rlim_max = 0;

        @Override
        protected List getFieldOrder() {
            return Arrays.asList("rlim_cur", "rlim_max");
        }
    }

    public static native int getrlimit(int resource, Rlimit rlimit);

    public static native int setrlimit(int resource, Rlimit rlimit);

    public static void setLimit(Rlimit rlimit) {
        int result = setrlimit(RLIMIT_NOFILE, rlimit);

        assertThat(result).withFailMessage("Setting open file limit for test case failed").isZero();
    }

    public static Rlimit getCurrentLimit() {
        Rlimit rlimit = new Rlimit();

        int result = getrlimit(RLIMIT_NOFILE, rlimit);

        assertThat(result).withFailMessage("Getting open file limit for test case failed").isZero();

        return rlimit;
    }
}
