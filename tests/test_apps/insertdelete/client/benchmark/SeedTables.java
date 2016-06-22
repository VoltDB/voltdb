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
 */

package benchmark;

import org.voltdb.*;
import org.voltdb.client.*;

public class SeedTables {

    public static void main(String[] args) throws Exception {

        /*
         * Instantiate a client and connect to the database.
         */
        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        myApp.createConnection("localhost");


        VoltTable p = myApp.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];
        int i = 0;
        while (p.advanceRow()) {
            i++;
            long id = p.getLong(0);
            myApp.callProcedure("TMP_0.insert",0,id,0);
            myApp.callProcedure("TMP_1.insert",0,id,0);
            myApp.callProcedure("TMP_2.insert",0,id,0);
            myApp.callProcedure("TMP_3.insert",0,id,0);
            myApp.callProcedure("TMP_4.insert",0,id,0);
            myApp.callProcedure("TMP_5.insert",0,id,0);
            myApp.callProcedure("TMP_6.insert",0,id,0);
            myApp.callProcedure("TMP_7.insert",0,id,0);
            myApp.callProcedure("TMP_8.insert",0,id,0);
            myApp.callProcedure("TMP_9.insert",0,id,0);

            myApp.callProcedure("TMP_s0.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s1.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s2.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s3.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s4.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s5.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s6.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s7.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s8.insert",0,id,0,"FOO");
            myApp.callProcedure("TMP_s9.insert",0,id,0,"FOO");

        }
        System.out.println("Finished seeding " + i + " partitions.");
    }
}
