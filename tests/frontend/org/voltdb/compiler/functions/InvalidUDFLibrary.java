/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
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
