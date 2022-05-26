/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.sysprocs.SnapshotRestoreResultSet.RestoreResultKey;
import org.voltdb.sysprocs.SnapshotRestoreResultSet.RestoreResultValue;

/**
 * Restore result set with data de-duped by unique host/partition/table key.
 */
public class SnapshotRestoreResultSet extends TreeMap<RestoreResultKey, RestoreResultValue>
{
    private static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    private static final long serialVersionUID = -7968937051509766792L;

    /**
     * Restore result key data (host ID / partition ID / table name).
     */
    public static class RestoreResultKey implements Comparable<RestoreResultKey>
    {
        public Integer m_hostId;
        public Integer m_partitionId;
        public String m_table;

        public RestoreResultKey(Integer hostId, Integer partitionId, String table)
        {
            m_hostId = hostId;
            m_partitionId = partitionId;
            m_table = table;
        }

        @Override
        public int compareTo(RestoreResultKey o)
        {
            int res = m_hostId.compareTo(o.m_hostId);
            if (res == 0) {
                res = m_partitionId.compareTo(o.m_partitionId);
                if (res == 0) {
                    res = m_table.compareTo(o.m_table);
                }
            }
            return res;
        }
    }

    /**
     * Non-key restore result data (host name / site ID / success codes / errors).
     * One instance captures all the data that is uniquely identified by a
     * host/partition/table key. There are multiple success codes and error
     * messages that are either expanded back to multiple result rows for
     * replicated tables or merged to one result row for partitioned tables.
     */
    public static class RestoreResultValue
    {
        public final Integer m_siteId;
        public final String m_hostName;
        public List<Boolean> m_successes = new ArrayList<Boolean>();
        public List<String> m_errMsgs = new ArrayList<String>();

        public RestoreResultValue(int siteId, boolean success, String hostName, String errMsg)
        {
            m_siteId = siteId;
            m_hostName = hostName;
            m_successes.add(success);
            m_errMsgs.add(errMsg);
        }

        /**
         * Merge restore result value data.
         * @param value  restore result value to merge.
         */
        public void mergeData(boolean success, String errMsg)
        {
            m_successes.add(success);
            m_errMsgs.add(errMsg);
        }

        /**
         * Get the number of entries that are represented here.
         * @return  entry count
         */
        public int getCount()
        {
            assert(m_successes.size() == m_errMsgs.size());
            return m_successes.size();
        }

        /**
         * Merge the success flags.
         * @return  true if all were successful
         */
        public boolean mergeSuccess()
        {
            for (Boolean success : m_successes) {
                if (!success) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Produce the merged result column value.
         * @return  "SUCCESS" or "FAILURE"
         */
        public String getSuccessColumnValue()
        {
            return mergeSuccess() ? "SUCCESS" : "FAILURE";
        }

        /**
         * Produce the merged error message column value.
         * @return  merged error message string
         */
        public String getErrorMessageColumnValue()
        {
            // Join the non-empty error messages.
            StringBuilder sb = new StringBuilder();
            for (String errMsg : m_errMsgs) {
                if (errMsg != null && !errMsg.isEmpty()) {
                    if (sb.length() > 0) {
                        sb.append(" | ");
                    }
                    sb.append(errMsg);
                }
            }
            return sb.toString();
        }
    }

    /**
     * Parse a restore result table row and add to the set.
     * @param vt  restore result table
     */
    public void parseRestoreResultRow(VoltTable vt)
    {
        RestoreResultKey key = new RestoreResultKey(
                (int)vt.getLong("HOST_ID"),
                (int)vt.getLong("PARTITION_ID"),
                vt.getString("TABLE"));
        if (containsKey(key)) {
            get(key).mergeData(vt.getString("RESULT").equals("SUCCESS"),
                               vt.getString("ERR_MSG"));
        }
        else {
            put(key, new RestoreResultValue((int)vt.getLong("SITE_ID"),
                                            vt.getString("RESULT").equals("SUCCESS"),
                                            vt.getString("HOSTNAME"),
                                            vt.getString("ERR_MSG")));
        }
    }

    /**
     * Add restore result row(s). Replicated table results are expanded
     * to multiple rows. Partitioned table results are merged.
     *
     * @param key  result key
     * @param vt  output table
     * @return  true if a row was successfully added
     */
    public boolean addRowsForKey(RestoreResultKey key, VoltTable vt)
    {
        if (vt == null || key == null) {
            return false;
        }
        RestoreResultValue value = get(key);
        if (value == null) {
            return false;
        }
        try {
            if (key.m_partitionId == -1) {
                // Re-expand replicated table results.
                for (int i = 0; i < value.getCount(); ++i) {
                    vt.addRow(key.m_hostId,
                              value.m_hostName,
                              value.m_siteId,
                              key.m_table,
                              key.m_partitionId,
                              value.m_successes.get(i) ? "SUCCESS" : "FAILURE",
                              value.m_errMsgs.get(i));
                }
            }
            else {
                // Partitioned table results merge redundant partition results.
                vt.addRow(key.m_hostId,
                          value.m_hostName,
                          value.m_siteId,
                          key.m_table,
                          key.m_partitionId,
                          value.getSuccessColumnValue(),
                          value.getErrorMessageColumnValue());
            }
        }
        catch(RuntimeException e) {
            SNAP_LOG.error("Exception received while adding snapshot restore result row.", e);
            throw e;
        }
        return true;
    }
}
