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

package procedures;

import org.voltdb.*;

public class InsertDeleteWithString extends VoltProcedure {

    public final SQLStmt insert0 = new SQLStmt("INSERT INTO tmp_s0 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert1 = new SQLStmt("INSERT INTO tmp_s1 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert2 = new SQLStmt("INSERT INTO tmp_s2 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert3 = new SQLStmt("INSERT INTO tmp_s3 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert4 = new SQLStmt("INSERT INTO tmp_s4 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert5 = new SQLStmt("INSERT INTO tmp_s5 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert6 = new SQLStmt("INSERT INTO tmp_s6 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert7 = new SQLStmt("INSERT INTO tmp_s7 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert8 = new SQLStmt("INSERT INTO tmp_s8 (appid,deviceid,val) VALUES (?,?,?);");
    public final SQLStmt insert9 = new SQLStmt("INSERT INTO tmp_s9 (appid,deviceid,val) VALUES (?,?,?);");

    public final SQLStmt delete0 = new SQLStmt("DELETE FROM tmp_s0 WHERE deviceid = ?;");
    public final SQLStmt delete1 = new SQLStmt("DELETE FROM tmp_s1 WHERE deviceid = ?;");
    public final SQLStmt delete2 = new SQLStmt("DELETE FROM tmp_s2 WHERE deviceid = ?;");
    public final SQLStmt delete3 = new SQLStmt("DELETE FROM tmp_s3 WHERE deviceid = ?;");
    public final SQLStmt delete4 = new SQLStmt("DELETE FROM tmp_s4 WHERE deviceid = ?;");
    public final SQLStmt delete5 = new SQLStmt("DELETE FROM tmp_s5 WHERE deviceid = ?;");
    public final SQLStmt delete6 = new SQLStmt("DELETE FROM tmp_s6 WHERE deviceid = ?;");
    public final SQLStmt delete7 = new SQLStmt("DELETE FROM tmp_s7 WHERE deviceid = ?;");
    public final SQLStmt delete8 = new SQLStmt("DELETE FROM tmp_s8 WHERE deviceid = ?;");
    public final SQLStmt delete9 = new SQLStmt("DELETE FROM tmp_s9 WHERE deviceid = ?;");


    public VoltTable[] run(int appid, long deviceid, String val)
        throws VoltAbortException {

        voltQueueSQL(insert0,appid,deviceid,val);
        voltQueueSQL(insert1,appid,deviceid,val);
        voltQueueSQL(insert2,appid,deviceid,val);
        voltQueueSQL(insert3,appid,deviceid,val);
        voltQueueSQL(insert4,appid,deviceid,val);
        voltQueueSQL(insert5,appid,deviceid,val);
        voltQueueSQL(insert6,appid,deviceid,val);
        voltQueueSQL(insert7,appid,deviceid,val);
        voltQueueSQL(insert8,appid,deviceid,val);
        voltQueueSQL(insert9,appid,deviceid,val);

        voltQueueSQL(delete0,deviceid);
        voltQueueSQL(delete1,deviceid);
        voltQueueSQL(delete2,deviceid);
        voltQueueSQL(delete3,deviceid);
        voltQueueSQL(delete4,deviceid);
        voltQueueSQL(delete5,deviceid);
        voltQueueSQL(delete6,deviceid);
        voltQueueSQL(delete7,deviceid);
        voltQueueSQL(delete8,deviceid);
        voltQueueSQL(delete9,deviceid);

        return voltExecuteSQL();
    }
}
