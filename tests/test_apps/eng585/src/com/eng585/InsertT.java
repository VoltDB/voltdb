/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package com.eng585;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

@ProcInfo(partitionInfo = "T.T_PK1: 0", singlePartition = true)
public class InsertT extends VoltProcedure {

        public final SQLStmt sql = new SQLStmt(
                        "INSERT INTO T ("
                                        + "T_PK1,T_PK2,V1,V2,S_PK,V3,V4,V5,"
                                        + "V6,V7,V8,V9,V10,V11,V12,V13,V14,V15,TS1,"
                                        + "V16,TS2,V17,V18,V19,V20,V21,V22,"
                                        + "V23,V24,BIGINT1,T_PK3,TS3,V25,TS4,"
                                        + "DECIMAL1,V26,"
                                        + "V27,BIGINT8,TS5,"
                                        + "V28,V29,V30,V31,V32,INT1)"
                                        + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);");

        public VoltTable[] run(String T_PK1, String T_PK2,
                        String V1, String V2, String S_PK,
                        String V3, String V4, String V5,
                        String V6, String V7, String V8,
                        String V9, String V10, String V11, String V12,
                        String V13, String V14, String V15, String TS1,
                        String V16, String TS2, String V17,
                        String V18, String V19, String V20,
                        String V21, String V22,
                        String V23, String V24,
                        String BIGINT1, String T_PK3, String TS3,
                        String V25, String TS4, String DECIMAL1,
                        String V26, String bigint2,
                        String bigint3, String bigint4,
                        String bigint5, String bigint6,
                        String bigint7, String V27,
                        String BIGINT8, String TS5,
                        String V28, String V29,
                        String V30, String V31,
                        String V32, String INT1)
                        throws VoltAbortException {

                VoltTable[] retval = null;
                try {
                        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");

                        Long emptylong = new Long(0);

                        Long s_pk = emptylong;
                        if ((S_PK != null) && (!"".equals(S_PK))) {
                                s_pk = Long.parseLong(S_PK);
                        }
                        Long bigint1 = emptylong;
                        if ((BIGINT1 != null) && (!"".equals(BIGINT1.trim()))) {
                                bigint1 = Long.parseLong(BIGINT1);
                        }

                        BigDecimal decimal1 = null;
                        if ((DECIMAL1 != null) && (!"".equals(DECIMAL1))) {
                                decimal1 = new BigDecimal(DECIMAL1);
                        }

                        TimestampType ts1 = null;
                        TimestampType ts2 = null;

                        if ((TS1 != null) && (!"".equals(TS1))) {
                                Date tempdate = null;
                                try {
                                        tempdate = sdf.parse(TS1);
                                        ts1 = new TimestampType(tempdate.getTime() * 1000);
                                } catch (ParseException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }

                        }

                        if ((TS2 != null) && (!"".equals(TS2))) {
                                Date tempdate = null;
                                try {
                                        tempdate = sdf.parse(TS2);
                                        ts2 = new TimestampType(tempdate.getTime() * 1000);
                                } catch (ParseException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }

                        }

                        TimestampType ts3 = null;

                        if ((TS3 != null) && (!"".equals(TS3))) {
                                Date tempdate = null;
                                try {
                                        tempdate = sdf.parse(TS3);
                                        ts3 = new TimestampType(tempdate.getTime() * 1000);
                                } catch (ParseException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }

                        }

                        TimestampType ts5 = null;

                        if ((TS5 != null)
                                        && (!"".equals(TS5))) {
                                Date tempdate = null;
                                try {
                                        tempdate = sdf.parse(TS5);
                                        ts5 = new TimestampType(tempdate
                                                        .getTime() * 1000);
                                } catch (ParseException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }

                        }

                        TimestampType ts4 = null;

                        if ((TS4 != null) && (!"".equals(TS4))) {
                                Date tempdate = null;
                                try {
                                        tempdate = sdf.parse(TS4);
                                        ts4 = new TimestampType(tempdate.getTime() * 1000);
                                } catch (ParseException e) {
                                        // TODO Auto-generated catch block
                                        e.printStackTrace();
                                }

                        }

                        Integer bigint8 = null;

                        if ((BIGINT8 != null) && (!"".equals(BIGINT8))) {
                                bigint8 = Integer.parseInt(BIGINT8);
                        }

                        Integer int1 = null;

                        if ((INT1 != null) && (!"".equals(INT1))) {
                                int1 = Integer.parseInt(INT1);
                        }

                        voltQueueSQL(sql, T_PK1, T_PK2, V1,
                                        V2, s_pk, V3, V4,
                                        V5, V6, V7, V8,
                                        V9, V10, V11, V12, V13, V14, V15,
                                        ts1, V16, ts2, V17,
                                        V18, V19, V20,
                                        V21,
                                        V22,
                                        V23,
                                        V24,
                                        bigint1,
                                        T_PK3,
                                        ts3,
                                        V25,
                                        ts4,
                                        decimal1,
                                        V26,
                                        V27, bigint8, ts5,
                                        V28, V29, V30,
                                        V31, V32,
                                        int1);

                        retval = voltExecuteSQL();
                } catch (Exception exp) {

                        System.out.println("Problem in run method of InsertT procedure");
                        System.out.println("T_PK1 is "+T_PK1+" T_PK2 is "+T_PK2);
                }

                return retval;
        }
}
