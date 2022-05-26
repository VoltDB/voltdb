/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


public class UpdateByBlock extends VoltProcedure {

    static final String SET_INLINE = "SET "
            +"TINY  = 88, "
            +"SMALL = 8888, "
            +"INTEG = 888888, "
            +"BIG   = 888888888888, "
            +"FLOT  = 888888.888888, "
            +"DECML = 888888888888888888888888.888888888888, "
            +"TIMESTMP = '1888-08-08 08:58:58.888888 ', "
            +"VCHAR_INLINE     = 'Updated Row 88', "
            +"VCHAR_INLINE_MAX = 'Updated Row 888'";

    static final String SET_OUTLINE = SET_INLINE + ", "
            +"VCHAR_OUTLINE_MIN = 'Outline update 8', "
            +"VCHAR_OUTLINE     = 'Outline col update 8', "
            +"VCHAR_DEFAULT     = 'Out-line (i.e., non-inline) columns updated; 888888' ";

    static final SQLStmt UPDATE_SQL_INLINE = new SQLStmt(
            "UPDATE PARTITIONED " + SET_INLINE +" WHERE BLOCK_ID = ?;");

    static final SQLStmt UPDATE_SQL_OUTLINE = new SQLStmt(
            "UPDATE PARTITIONED  " + SET_OUTLINE+" WHERE BLOCK_ID = ?;");

    public VoltTable[] run(long blockId, int outline) throws VoltAbortException {
        voltQueueSQL((outline ==1 ? UPDATE_SQL_OUTLINE : UPDATE_SQL_INLINE), blockId);
        return voltExecuteSQL(true);
    }
}
