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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import java.math.BigDecimal;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class BadParamTypesForTimestamp extends VoltProcedure {
    private final static SQLStmt m_twoDaysFromTime = new SQLStmt("Select * from P2 where DATEADD(DAY, 2, ?) > TM order by ID");
    private final static SQLStmt m_twoMonthsFromTime = new SQLStmt("Select * from P2 where DATEADD(MONTH, 2, ?) > TM order by ID");
    private final static SQLStmt m_twoYearsFromTime = new SQLStmt("Select * from P2 where DATEADD(Year, 2, ?) > TM order by ID");

    private final static SQLStmt m_sinceEpochSecond= new SQLStmt("Select * from P2 where since_epoch(second, ?) = 1000 order by ID");
    private final static SQLStmt m_sinceEpochMilliSec = new SQLStmt("Select * from P2 where since_epoch(millis, ?) = 1000 order by ID");
    private final static SQLStmt m_sinceEpochMicrosSec = new SQLStmt("Select * from P2 where since_epoch(micros, ?) = 1000 order by ID");

    private final static SQLStmt m_truncateYear = new SQLStmt("Select truncate(YEAR, ?) from P2  order by ID");
    private final static SQLStmt m_truncateQuarter= new SQLStmt("Select truncate(Quarter, ?) from P2  order by ID");
    private final static SQLStmt m_truncateMonth= new SQLStmt("Select truncate(Month, ?) from P2  order by ID");
    private final static SQLStmt m_truncateDay = new SQLStmt("Select truncate(Day, ?) from P2  order by ID");
    private final static SQLStmt m_truncateHour= new SQLStmt("Select truncate(Hour, ?) from P2  order by ID");
    private final static SQLStmt m_truncateMinute = new SQLStmt("Select truncate(Minute, ?) from P2  order by ID");
    private final static SQLStmt m_truncateSecond = new SQLStmt("Select truncate(Second, ?) from P2  order by ID");
    private final static SQLStmt m_truncateMilli = new SQLStmt("Select truncate(MILLIS, ?) from P2  order by ID");

    /*
    // SQL statements below are commented out as parameterization of timestamp (TS) argument is not
    // supported. Revisit logic below to see the commented out sql statements can be enabled once
    // ENG-10145 has been addressed. When enabling statements, please add these sql statements to
    // ProcEntries also.
    public final static SQLStmt m_getSecondDaysTS = new SQLStmt("Select * from P2 where DAY(?) = 2 order by ID");
    public final static SQLStmt m_getSecondMonthsTS = new SQLStmt("Select * from P2 where MONTH(?) = 2 order by ID");
    public final static SQLStmt m_getSecondYearTS = new SQLStmt("Select * from P2 where YEAR(?) = 2 order by ID");
    public final static SQLStmt m_extractYear = new SQLStmt("Select extract(YEAR, ?) from P2  order by ID");
    public final static SQLStmt m_extractQuarter= new SQLStmt("Select extract(Quarter, ?) from P2  order by ID");
    public final static SQLStmt m_extractMonth= new SQLStmt("Select extract(Month, ?) from P2  order by ID");
    public final static SQLStmt m_extractWeek = new SQLStmt("Select extract(Week from ?) from P2  order by ID");
    public final static SQLStmt m_extractDay = new SQLStmt("Select extract(Day, ?) from P2  order by ID");
    public final static SQLStmt m_extractDayOfMonth = new SQLStmt("Select extract(Day_OF_MOTNH, ?) from P2 order by ID");
    public final static SQLStmt m_extractHour= new SQLStmt("Select extract(Hour from ?) from P2  order by ID");
    public final static SQLStmt m_extractMinute = new SQLStmt("Select extract(Minute, ?) from P2  order by ID");
    public final static SQLStmt m_extractSecond = new SQLStmt("Select extract(Second, ?) from P2  order by ID");
    public final static SQLStmt m_extractMilli = new SQLStmt("Select extract(MILLIS, ?) from P2  order by ID");
    */

    private final static byte tinyIntValue = 127;
    private final static short shortValue = 255;
    private final static int intValue = 1000;
    private final static long longValue = 1000000;
    private final static double doubleValue = 1232324;
    private final static BigDecimal bgValue = new BigDecimal(doubleValue);
    private final static String strValue = "2000-04-01 01:00:00.000000";

    public static enum ProcEntries {
        DateaddDays (m_twoDaysFromTime),
        DateaddMonths(m_twoMonthsFromTime),
        DateaddYears(m_twoYearsFromTime),
        EpochSeconds(m_sinceEpochSecond),
        EpochMilliSeconds(m_sinceEpochMilliSec),
        EpochMircoSeconds(m_sinceEpochMicrosSec),
        TruncateYear(m_truncateYear),
        TruncateQuarter(m_truncateQuarter),
        TruncateMonth(m_truncateMonth),
        TruncateDay(m_truncateDay),
        TruncateHour(m_truncateHour),
        TruncateMinute(m_truncateMinute),
        TruncateSecond(m_truncateSecond),
        TruncateMilli(m_truncateMilli);

        ProcEntries (SQLStmt stmt) {
            this.stmt = stmt;
        }
        public SQLStmt getStmt() {
            return stmt;
        }
        private SQLStmt stmt;
    }

    public static final ProcEntries[] procs =  ProcEntries.values();
    public final static Object[] values = {tinyIntValue, shortValue, intValue, longValue,
                                           doubleValue, bgValue, strValue};

    public VoltTable[] run(int procEntryIndex, int valueIndex) {
        assert (procEntryIndex < procs.length);
        assert (valueIndex < values.length);
        voltQueueSQL(procs[procEntryIndex].getStmt(), values[valueIndex]);
        return voltExecuteSQL();
    }
}
