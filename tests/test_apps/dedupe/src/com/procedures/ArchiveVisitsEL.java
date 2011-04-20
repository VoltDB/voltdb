package com.procedures;

import org.voltdb.*;

import java.util.Date;


@ProcInfo(
    partitionInfo = "VISIT_ARCHIVED.USERID: 0",
    singlePartition = true
)

public class ArchiveVisitsEL extends VoltProcedure {
    // grab <x> visits to archive
    public final SQLStmt getUnArchived = new SQLStmt("select userId, field1, field2, field3, visitTime from visit_unarchived where visitTime < ? order by visitTime, userId, field1, field2, field3 limit ?;");

    // insert an archived visit
    public final SQLStmt insertArchived = new SQLStmt("insert into visit_archived (userId, field1, field2, field3, visitTime) values (?, ?, ?, ?, ?);");

    // delete an unarchived visit
    public final SQLStmt deleteUnArchived = new SQLStmt("delete from visit_unarchived where userId = ? and field1 = ? and field2 = ? and field3 = ? and visitTime = ?;");

    // insert an archived visit for EL
    public final SQLStmt insertEL = new SQLStmt("insert into visit_el (userId, field1, field2, field3, visitTime) values (?, ?, ?, ?, ?);");



    public VoltTable[] run(
            long partitionId,
            long numMoves,
            long called_time_milliseconds
    ) {
        long timeFiveMinutesAgoMillis = called_time_milliseconds - (1000l * 60l * 5l);

        voltQueueSQL(getUnArchived, timeFiveMinutesAgoMillis, numMoves);

        VoltTable results1[] = voltExecuteSQL();

        int rowCount = results1[0].getRowCount();

        int sqlStatements = 0;

        if (rowCount > 0) {
            // move from unarchived to archived
            for (int ii = 0; ii < rowCount; ii++) {  
                VoltTableRow row = results1[0].fetchRow(ii);
                long userId = row.getLong(0);
                long field1 = row.getLong(1);
                long field2 = row.getLong(2);
                long field3 = row.getLong(3);
                long visitTimeLong = row.getLong(4);

                voltQueueSQL(insertArchived, userId, field1, field2, field3, visitTimeLong);
                voltQueueSQL(deleteUnArchived, userId, field1, field2, field3, visitTimeLong);
                voltQueueSQL(insertEL, userId, field1, field2, field3, visitTimeLong);

                sqlStatements += 3;

                if (sqlStatements > 900) {
                    // limit of 1000 sql statements
                    VoltTable results2[] = voltExecuteSQL();
                    sqlStatements = 0;
                }

            }

            if (sqlStatements > 0) {
                VoltTable results3[] = voltExecuteSQL(true);
            }
        }

        VoltTable vtMetric = new VoltTable(new VoltTable.ColumnInfo("called_time_milliseconds",VoltType.BIGINT),
                                           new VoltTable.ColumnInfo("rows_moved",VoltType.INTEGER));
        Object rowMetric[] = new Object[2];
        rowMetric[0] = called_time_milliseconds;
        rowMetric[1] = (long) rowCount;
        vtMetric.addRow(rowMetric);

        final VoltTable[] vtReturn = {vtMetric};

        return vtReturn;
    }
} 
