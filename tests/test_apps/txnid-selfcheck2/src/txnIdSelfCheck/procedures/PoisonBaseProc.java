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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class PoisonBaseProc extends VoltProcedure {
    final SQLStmt insert = new SQLStmt("insert into bigp values (?,?,?);");

    public static int SYSTEMDOTEXIT = 0;
    public static int NAVELGAZE = 1;

    public long run() {
        return 0; // never called in base procedure
    }

    protected long poisonTheWell(int toxinType)
    {
        // make sure it gets logged to the command log
        try {
            Thread.sleep(10 * 1000);
        } catch (InterruptedException ignoreIt) {}

        if (toxinType == SYSTEMDOTEXIT) {
            System.exit(37);
        }
        else if (toxinType == NAVELGAZE) {
            while (true) {}
        }
        return 0; // NOT REACHED
    }
}
