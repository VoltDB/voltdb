package com.procedures;

import org.voltdb.*;
import java.util.Calendar;
import java.util.TimeZone;

@ProcInfo(
        partitionInfo = "VISIT_ARCHIVED.USERID: 0",
        singlePartition = true
)

public class RecordHit extends VoltProcedure {
    // check if player already archived today
    public final SQLStmt checkArchived = new SQLStmt("select userId from visit_archived where userId = ? and field1 = ? and field2 = ? and field3 = ? and visitTime >= ? and visitTime <= ?;");

    // get player visitTime from unarchived (if they are in there)
    public final SQLStmt checkUnArchived = new SQLStmt("select visitTime from visit_unarchived where userId = ? and field1 = ? and field2 = ? and field3 = ? and visitTime >= ? and visitTime <= ?;");

    // update player visitTime in unarchived
    public final SQLStmt updateUnArchived = new SQLStmt("update visit_unarchived set visitTime = ? where userId = ? and field1 = ? and field2 = ? and field3 = ? and visitTime >= ? and visitTime <= ?;");

    // insert player into unarchived
    public final SQLStmt insertUnArchived = new SQLStmt("insert into visit_unarchived (userId, field1, field2, field3, visitTime) values (?, ?, ?, ?, ?);");

    private final TimeZone tz = TimeZone.getTimeZone("US/Eastern");
    private final Calendar calTemp = Calendar.getInstance(tz);

    public VoltTable[] run(
            long userId,
            long field1,
            long field2,
            long field3,
            long visitTimeMillis,
            long called_time_milliseconds
    ) {
        calTemp.setTimeInMillis(visitTimeMillis);
        calTemp.set(Calendar.HOUR_OF_DAY, 0);
        calTemp.set(Calendar.MINUTE, 0);
        calTemp.set(Calendar.SECOND, 0);
        calTemp.set(Calendar.MILLISECOND, 0);

        long visitTimeStartOfDayMillis = calTemp.getTimeInMillis();

        calTemp.set(Calendar.HOUR_OF_DAY, 23);
        calTemp.set(Calendar.MINUTE, 59);
        calTemp.set(Calendar.SECOND, 59);
        calTemp.set(Calendar.MILLISECOND, 999);

        long visitTimeEndOfDayMillis = calTemp.getTimeInMillis();

        voltQueueSQL(checkArchived, userId, field1, field2, field3, visitTimeStartOfDayMillis, visitTimeEndOfDayMillis);
        voltQueueSQL(checkUnArchived, userId, field1, field2, field3, visitTimeStartOfDayMillis, visitTimeEndOfDayMillis);

        VoltTable results1[] = voltExecuteSQL();
        int sqlStatements = 0;

        if (results1[0].getRowCount() == 0) {
            // user is not yet archived

            if (results1[1].getRowCount() == 1) {
                // update unarchived row if older timestamp
                if (results1[1].fetchRow(0).getLong(0) > visitTimeMillis) {
                    voltQueueSQL(updateUnArchived, visitTimeMillis, userId, field1, field2, field3, visitTimeStartOfDayMillis, visitTimeEndOfDayMillis);
                    sqlStatements++;
                }
            } else {
                // insert unarchived row
                voltQueueSQL(insertUnArchived, userId, field1, field2, field3, visitTimeMillis);
                sqlStatements++;
            }

            if (sqlStatements > 0) {
                VoltTable results2[] = voltExecuteSQL(true);
            }
        }

        VoltTable vtLoad = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT));
        Object row[] = new Object[1];
        row[0] = called_time_milliseconds;
        vtLoad.addRow(row);

        final VoltTable[] vtReturn = {vtLoad};

        return vtReturn;
    }
} 
