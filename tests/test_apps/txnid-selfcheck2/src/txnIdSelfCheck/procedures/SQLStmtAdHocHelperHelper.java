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

package txnIdSelfCheck.procedures;

import org.voltdb.VoltProcedure;

import java.lang.reflect.Method;


public class SQLStmtAdHocHelperHelper {

    public static void voltQueueSQLExperimental(VoltProcedure proc, String sql, Object... args) {

        Class<?> classz = null;
        Class[] plist = null;
        Object[] np = null;
        Method m = null;

        try {
            classz = Class.forName("org.voltdb.SQLStmtAdHocHelper");

            plist = new Class[3];
            plist[0] = VoltProcedure.class;
            plist[1] = String.class;
            plist[2] = Object[].class;

            try {
                m = classz.getMethod("voltQueueSQLExperimental", plist);
            }
            catch (NoSuchMethodException e) {

                classz = Class.forName("org.voltdb.VoltProcedure");

                plist = new Class[2];
                plist[0] = String.class;
                plist[1] = Object[].class;

                m = classz.getMethod("voltQueueSQLExperimental", plist);

                np = new Object[2];
                np[0] = sql;
                np[1] = args;

                m.invoke(proc, np);
                return;
            }

            np = new Object[3];
            np[0] = proc;
            np[1] = sql;
            np[2] = args;

            m.invoke(null, np);

        } catch(Exception e) {
            throw new VoltProcedure.VoltAbortException("Error invoking voltQueueSQLExperimental " + e.toString());
        }
        return;
    }
}
