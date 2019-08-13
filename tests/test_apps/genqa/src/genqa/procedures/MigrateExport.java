/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
package genqa.procedures;

import java.util.Random;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;


public class MigrateExport extends VoltProcedure {
    public final SQLStmt migrate_kafka = new SQLStmt("MIGRATE FROM export_partitioned_table_kafka WHERE NOT MIGRATING AND type_not_null_timestamp < DATEADD(SECOND, ?, NOW)");
    public final SQLStmt migrate_rabbit = new SQLStmt("MIGRATE FROM export_partitioned_table_rabbit WHERE NOT MIGRATING AND type_not_null_timestamp < DATEADD(SECOND, ?, NOW)");
    public final SQLStmt migrate_file = new SQLStmt("MIGRATE FROM export_partitioned_table_file WHERE NOT MIGRATING AND type_not_null_timestamp < DATEADD(SECOND, ?, NOW)");
    public final SQLStmt migrate_jdbc = new SQLStmt("MIGRATE FROM export_partitioned_table_jdbc WHERE NOT MIGRATING AND type_not_null_timestamp < DATEADD(SECOND, ?, NOW)");
    // public final SQLStmt migrate = new SQLStmt("MIGRATE FROM export_partitioned_table WHERE NOT MIGRATING AND type_not_null_timestamp < DATEADD(SECOND, ?, NOW)");

    // public  VoltTable[] run()
    public long run(int seconds)
    {
        // ad hoc kinda like "MIGRATE FROM export_partitioned_table where <records older than "seconds" ago>
        voltQueueSQL(migrate_kafka, -seconds);
        voltQueueSQL(migrate_rabbit, -seconds);
        voltQueueSQL(migrate_file, -seconds);
        voltQueueSQL(migrate_jdbc, -seconds);

        // Execute last statement batch
         VoltTable[] v = voltExecuteSQL(true);
        // voltExecuteSQL(true);

        // Return to caller
        // return v;
        return seconds;
    }
}
