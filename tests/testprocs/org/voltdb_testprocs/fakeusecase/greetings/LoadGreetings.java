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

package org.voltdb_testprocs.fakeusecase.greetings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/** A stored procedure which depends on a non-class resource in the jarfile. */
public class LoadGreetings extends VoltProcedure {

    private static final String GREETINGS_FILE_NAME = "/greetinglist.txt";
    private static final SQLStmt INSERT_GREETING_STMT = new SQLStmt("INSERT INTO greetings VALUES (?, ?, ?, 0);");

    public VoltTable[] run() {
        InputStream stream = this.getClass().getResourceAsStream(GREETINGS_FILE_NAME);
        if (stream == null) {
            throw new VoltAbortException("Could not find " + GREETINGS_FILE_NAME);
        }
        BufferedReader buffer = new BufferedReader(new InputStreamReader(stream));
        try {
            String line;
            while ((line = buffer.readLine()) != null) {
                String[] greeting = line.trim().split(",");
                if (greeting.length != 3) {
                    throw new VoltAbortException("Could not parse greeting - must be 3 comma separated values. \"" + line + '\"');
                }
                voltQueueSQL(INSERT_GREETING_STMT, EXPECT_EMPTY, greeting[0], greeting[1], greeting[2]);
            }
            return voltExecuteSQL(true);
        } catch (IOException e) {
            throw new VoltAbortException(e);
        } finally {
            try {
                buffer.close();
            } catch (IOException disregard) {
                disregard.printStackTrace(); // BSDBG this is just so I don't get spurious exceptions flying
            }
        }
    }
}
