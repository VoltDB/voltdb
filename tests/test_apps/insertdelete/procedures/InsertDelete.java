/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */package procedures;

import org.voltdb.*;

public class InsertDelete extends VoltProcedure {

    public final SQLStmt insert0 = new SQLStmt("INSERT INTO tmp_0 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert1 = new SQLStmt("INSERT INTO tmp_1 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert2 = new SQLStmt("INSERT INTO tmp_2 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert3 = new SQLStmt("INSERT INTO tmp_3 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert4 = new SQLStmt("INSERT INTO tmp_4 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert5 = new SQLStmt("INSERT INTO tmp_5 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert6 = new SQLStmt("INSERT INTO tmp_6 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert7 = new SQLStmt("INSERT INTO tmp_7 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert8 = new SQLStmt("INSERT INTO tmp_8 (appid,deviceid) VALUES (?,?);");
    public final SQLStmt insert9 = new SQLStmt("INSERT INTO tmp_9 (appid,deviceid) VALUES (?,?);");

    public final SQLStmt delete0 = new SQLStmt("DELETE FROM tmp_0 WHERE deviceid = ?;");
    public final SQLStmt delete1 = new SQLStmt("DELETE FROM tmp_1 WHERE deviceid = ?;");
    public final SQLStmt delete2 = new SQLStmt("DELETE FROM tmp_2 WHERE deviceid = ?;");
    public final SQLStmt delete3 = new SQLStmt("DELETE FROM tmp_3 WHERE deviceid = ?;");
    public final SQLStmt delete4 = new SQLStmt("DELETE FROM tmp_4 WHERE deviceid = ?;");
    public final SQLStmt delete5 = new SQLStmt("DELETE FROM tmp_5 WHERE deviceid = ?;");
    public final SQLStmt delete6 = new SQLStmt("DELETE FROM tmp_6 WHERE deviceid = ?;");
    public final SQLStmt delete7 = new SQLStmt("DELETE FROM tmp_7 WHERE deviceid = ?;");
    public final SQLStmt delete8 = new SQLStmt("DELETE FROM tmp_8 WHERE deviceid = ?;");
    public final SQLStmt delete9 = new SQLStmt("DELETE FROM tmp_9 WHERE deviceid = ?;");


    public VoltTable[] run(int appid, long deviceid)
        throws VoltAbortException {

        voltQueueSQL(insert0,appid,deviceid);
        voltQueueSQL(insert1,appid,deviceid);
        voltQueueSQL(insert2,appid,deviceid);
        voltQueueSQL(insert3,appid,deviceid);
        voltQueueSQL(insert4,appid,deviceid);
        voltQueueSQL(insert5,appid,deviceid);
        voltQueueSQL(insert6,appid,deviceid);
        voltQueueSQL(insert7,appid,deviceid);
        voltQueueSQL(insert8,appid,deviceid);
        voltQueueSQL(insert9,appid,deviceid);

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
