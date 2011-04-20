package com.procedures;

import org.voltdb.*;

@ProcInfo(
        partitionInfo = "VISIT_ARCHIVED.USERID: 0",
        singlePartition = true
)

public class CheckUserId extends VoltProcedure {
    public final SQLStmt selectData = new SQLStmt("select userid, field1, field2, field3, visittime from visit_unarchived where userid = ?;");

    public VoltTable[] run(
            long userId
    ) {
        voltQueueSQL(selectData, userId);

        return voltExecuteSQL(true);
    }
} 
