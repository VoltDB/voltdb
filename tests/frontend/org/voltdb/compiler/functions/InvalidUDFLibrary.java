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

package org.voltdb.compiler.functions;

public class InvalidUDFLibrary {

    public static class UnsupportedType { }

    public UnsupportedType runWithUnsupportedReturnType() {
        return null;
    }

    public int runWithUnsupportedParamType(int arg0, double arg1, UnsupportedType arg2, String arg3) {
        return 0;
    }

    public Integer run(Integer arg0) {
        if (arg0 == null) {
            return null;
        }
        return 10;
    }

    private String run(String arg0) {
        return null;
    }

    public void run(Boolean arg0) {
        return;
    }

    public static Integer run(Integer arg0, Integer arg1) {
        if (arg0 == null) {
            return null;
        }
        return 10;
    }

    private static void run() {
        return;
    }

    public Boolean dup() {
        return true;
    }

    public Boolean dup(Double arg0) {
        return false;
    }

}
