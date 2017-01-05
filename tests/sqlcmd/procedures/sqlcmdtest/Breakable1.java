/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package sqlcmdtest;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class Breakable1 extends VoltProcedure {

    // The definition for this nested class will be killed by deleting its separate
    // class file before building the jar. This will show that sqlcmd will be notified
    // of the detected class loader exception.
    private static class NestedGetsKilled {
        static VoltTable[] method() {
            return new VoltTable[] {};
        }
    }

    private static VoltTable[] forceLoadTimeDependencyLoadError = NestedGetsKilled.method();
    //private static VoltTable[] forceLoadTimeDependencyLoadError = new VoltTable[] {};

    public VoltTable[] run() throws VoltAbortException {
        return forceLoadTimeDependencyLoadError;
    }

}
