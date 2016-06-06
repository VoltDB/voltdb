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
 *//* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 */
package benchmark;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import com.google_voltpatches.common.collect.ConcurrentHashMultiset;
import com.google_voltpatches.common.collect.Multiset;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class BenchmarkCallback implements ProcedureCallback {

    private static Multiset<String> stats = ConcurrentHashMultiset.create();
    private static ConcurrentHashMap<String,Integer> procedures = new ConcurrentHashMap<String,Integer>();
    String procedureName;
    long maxErrors;

    public static int count( String procedureName, String event ){
        return stats.add(procedureName + event, 1);
    }

    public static int getCount( String procedureName, String event ){
        return stats.count(procedureName + event);
    }

    public static void printProcedureResults(String procedureName) {
        System.out.println("  " + procedureName);
        System.out.println("        calls: " + getCount(procedureName,"call"));
        System.out.println("      commits: " + getCount(procedureName,"commit"));
        System.out.println("    rollbacks: " + getCount(procedureName,"rollback"));
    }

    public static void printAllResults() {
        List<String> l = new ArrayList<String>(procedures.keySet());
        Collections.sort(l);
        for (String e : l) {
            printProcedureResults(e);
        }
    }

    public BenchmarkCallback(String procedure, long maxErrors) {
        super();
        this.procedureName = procedure;
        this.maxErrors = maxErrors;
    procedures.putIfAbsent(procedure,1);
    }

    public BenchmarkCallback(String procedure) {
        this(procedure, 5l);
    }

    @Override
    public void clientCallback(ClientResponse cr) {

        count(procedureName,"call");

        if (cr.getStatus() == ClientResponse.SUCCESS) {
            count(procedureName,"commit");
        } else {
            long totalErrors = count(procedureName,"rollback");

            if (totalErrors > maxErrors) {
                System.err.println("exceeded " + maxErrors + " maximum database errors - exiting client");
                System.exit(-1);
            }

            System.err.println("DATABASE ERROR: " + cr.getStatusString());
        }
    }
}

