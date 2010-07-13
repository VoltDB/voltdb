package com.eng585;

import java.math.BigDecimal;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

//@ProcInfo(partitionInfo = "S.s_pk: 0", singlePartition = true)
@ProcInfo(singlePartition = false)
public class InsertS extends VoltProcedure
{
    public final SQLStmt sql =
        new SQLStmt("INSERT INTO S ("
                    + " S_PK, V1 , TS1 , V2 ,"
                    + " V3 , V4 , V5 , "
                    + " V6 , INT1 , INT2 ,"
                    + " INT3, INT4 , INT5 ,"
                    + " INT6 , V7 , INT7 , V8 , "
                    + " V9 , V10 , V11,"
                    + "BIGINT1, V12, BIGINT2, BIGINT3, "
                    + " DECIMAL1, DECIMAL2, DECIMAL3, DECIMAL4, "
                    + " BIGINT4, BIGINT5, V13,"
                    + "BIGINT6, BIGINT7, BIGINT8"
                    + ") VALUES ( ? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? ,? );");

    public VoltTable[] run( long s_pk, String V1, TimestampType ts1,
                            String V2, String V3, String V4, String V5,
                            String V6, int int1, int int2,
                            int int3, int int4,
                            int int5, int int6, String V7, int int7,
                            String V8, String V9, String V10, String V11,
                            long bigint1, String V12, long bigint2, long bigint3,
                            BigDecimal decimal1, BigDecimal decimal2, BigDecimal decimal3,
                            BigDecimal decimal4, long bigint4, long  bigint5,
                            String V13, long bigint6,
                            long bigint7, long bigint8)
    throws VoltAbortException
    {
        voltQueueSQL(sql, s_pk, V1, ts1,
                     V2, V3, V4, V5,
                     V6, int1, int2,
                     int3, int4,
                     int5, int6, V7, int7,
                     V8, V9, V10, V11,
                     bigint1, V12, bigint2, bigint3,
                     decimal1, decimal2, decimal3,
                     decimal4, bigint4, bigint5,
                     V13, bigint6,
                     bigint7, bigint8);

        VoltTable[] retval = voltExecuteSQL();

        return retval;
    }
}
