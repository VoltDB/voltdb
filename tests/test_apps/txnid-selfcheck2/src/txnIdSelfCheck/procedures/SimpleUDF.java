/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.voltdb.VoltTable;
import org.voltcore.logging.VoltLogger;


public class SimpleUDF extends VoltProcedure {
    VoltLogger info = new VoltLogger(getClass().getSimpleName());
    public final SQLStmt d_simpleUDF2 = new SQLStmt(
            "select simpleUDF2(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF3 = new SQLStmt(
            "select simpleUDF3(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF4 = new SQLStmt(
            "select simpleUDF4(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF5 = new SQLStmt(
            "select simpleUDF5(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF6 = new SQLStmt(
            "select simpleUDF6(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF7 = new SQLStmt(
            "select simpleUDF7(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF8 = new SQLStmt(
            "select simpleUDF8(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF9 = new SQLStmt(
            "select simpleUDF9(?) FROM dimension where cid=-1 limit 1");
    public final SQLStmt d_simpleUDF10 = new SQLStmt(
            "select simpleUDF10(?) FROM dimension where cid=-1 limit 1");

    public VoltTable[] run(long t, long exponent) {
        SQLStmt stmt = null;
        switch ((int)exponent) {
        case 2:
            stmt = d_simpleUDF2;
            break;
        case 3:
            stmt = d_simpleUDF3;
            break;
        case 4:
            stmt = d_simpleUDF4;
            break;
        case 5:
            stmt = d_simpleUDF5;
            break;
        case 6:
            stmt = d_simpleUDF6;
            break;
        case 7:
            stmt = d_simpleUDF7;
            break;
        case 8:
            stmt = d_simpleUDF8;
            break;
        case 9:
            stmt = d_simpleUDF9;
            break;
        case 10:
            stmt = d_simpleUDF10;
            break;
        }
        voltQueueSQL(stmt, t);
        info.info(String.format("Executing simple udf call(%d, %d).", t, exponent));
        VoltTable[] results = voltExecuteSQL(true);
        return results;
    }
}
