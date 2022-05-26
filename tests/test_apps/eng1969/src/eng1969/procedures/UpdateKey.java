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
package eng1969.procedures;

import org.voltdb.DeprecatedProcedureAPIAccess;
import org.voltdb.SQLStmt;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class UpdateKey extends VoltProcedure {

    private final long MAX_GROUPS = 1000;

    // update a group / key combination
    public final SQLStmt update =
            new SQLStmt("update backed set atime = ?, payload = ? where rowid_group = ? and rowid = ?;");

    // insert a key back in to the database
    public final SQLStmt insert =
            new SQLStmt("insert into backed values (?, ?, ?, ?);");

    // is a rowid key in memory
    public final SQLStmt check =
            new SQLStmt("select count(rowid) from backed where rowid_group = ? and rowid = ?;");

    // how many groups are in memory
    public final SQLStmt residentGroups =
            new SQLStmt("select count(*) from partitioned_table_group");

    // get oldest group
    public final SQLStmt minAtimeGroupId =
        new SQLStmt("select rowid_group from backed order by atime limit 1");

    // lowest group id with minimum access time
    public final SQLStmt evictedGroup =
            new SQLStmt("select rowid, payload from backed where rowid_group = ?");

    // delete a group id
    public final SQLStmt deleteEvicted =
            new SQLStmt("delete from backed where rowid_group = ?");

    byte[] makeKey(long rowid_group, long rowid) {
        Long group = Long.valueOf(rowid_group);
        Long id = Long.valueOf(rowid);
        String key = group.toString() + "_" + id.toString();
        return key.getBytes();
    }

    @SuppressWarnings("deprecation")
    public long run(long rowid, long rowid_group, byte[] payload)
    {
        DB db = m_site.getLevelDBInstance();
        voltQueueSQL(check, rowid_group, rowid);
        VoltTable[] r1 = voltExecuteSQL();
        if (r1[0].asScalarLong() == 0) {
            // System.out.println("Not found: " + rowid_group + "_" + rowid);
            // evict a key if necessary
            voltQueueSQL(residentGroups);
            VoltTable[] r2 = voltExecuteSQL();
            if (r2[0].asScalarLong() >= MAX_GROUPS) {
                // System.out.printf("\tCurrent group count (%d) exceeds max (%d) - evicting!\n",
                //                r2[0].asScalarLong(), MAX_GROUPS);
                voltQueueSQL(minAtimeGroupId);
                VoltTable[] r3 = voltExecuteSQL();
                long groupId = r3[0].asScalarLong();
                voltQueueSQL(evictedGroup, groupId);
                VoltTable evictions = voltExecuteSQL()[0];
                long evictedCount = 0;
                while (evictions.advanceRow()) {
                    ++evictedCount;
                    // System.out.printf("\tEvicting (%d) groupId (%d_%d)\n", evictedCount,  groupId, evictions.getLong(0));
                    byte[] key = makeKey(groupId, evictions.getLong(0));
                    db.put(key, evictions.getVarbinary(1));
                }

                voltQueueSQL(deleteEvicted, EXPECT_SCALAR_MATCH(evictedCount), groupId);
                VoltTable[] r4 = voltExecuteSQL();
                // System.out.println("Evicted " + r4[0].asScalarLong() + " rows. Expected: " + evictedCount);
            }

            // TODO: this needs to iterate for all rowid keys
            byte[] oldpayload = db.get(makeKey(rowid_group, rowid));
            if (oldpayload == null) {
                VoltDB.crashLocalVoltDB("Could not find payload from expected key: " +
                                        rowid_group + "_" + rowid, false, null);
            }
            // System.out.println("\tMigrating key: " + rowid_group + "_" + rowid);
            voltQueueSQL(insert, EXPECT_SCALAR_MATCH(1), rowid_group, rowid, DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this), oldpayload);
            voltExecuteSQL();
        }

        // perform in the in-memory update
        // System.out.println("Updating atime on key: " + rowid_group + "_" + rowid);
        voltQueueSQL(update, EXPECT_SCALAR_MATCH(1), DeprecatedProcedureAPIAccess.getVoltPrivateRealTransactionId(this), payload, rowid_group, rowid);
        return 0;
    }
}
