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

package org.voltdb.client;

/**
 * Collect the client's opinion of the operation of client affinity.
 * <p>
 * <b>Single-partition procedures</b>
 * <p>
 * For the given partition id, affinityWrites and affinityReads track
 * transactions that the client believes it knows the master for, and
 * for which it has an active network connection.
 * <p>
 * rrWrites and rrReads reflect transactions routed using round-robin,
 * which can happen if the client has no active connection to the
 * partition master.
 * <p>
 * <b>Multi-partition procedures</b>
 * <p>
 * Multipartitioned procedures are treated as single-partition procedures,
 * using the special partition id <code>16383</code>.
 * <p>
 * <b>Other cases</b>
 * <p>
 * A statistics entry with partition id <code>-1</code> is used when:
 * <ul>
 * <li> The client has not yet acquired partitioning data from the
 *      VoltDB server.
 * <li> The procedure name is not yet known to the client, or does
 *      not exist in the catalog.
 * <li> The application has failed to provide a value for the
 *      partitioning column.
 * <li> The procedure is a 'compound procedure'; this type of
 *      procedure has no natural affinity to any one node.
 * </ul>
 */
public class ClientAffinityStats {

    private int m_partitionId;
    private long m_affinityWrites;
    private long m_rrWrites;
    private long m_affinityReads;
    private long m_rrReads;

    ClientAffinityStats(int partitionId) {
        m_partitionId = partitionId;
    }

    ClientAffinityStats(int partitionId, long affinityWrites, long rrWrites,
            long affinityReads, long rrReads)
    {
        m_partitionId = partitionId;
        m_affinityWrites = affinityWrites;
        m_rrWrites = rrWrites;
        m_affinityReads = affinityReads;
        m_rrReads = rrReads;
    }

    /**
     * Subtract one ClientAffinityStats instance from another to produce a third.
     *
     * @param newer More recent ClientAffinityStats instance.
     * @param older Less recent ClientAffinityStats instance.
     * @return New instance representing the difference.
     */
    public static ClientAffinityStats diff(ClientAffinityStats newer, ClientAffinityStats older) {
        if (newer.m_partitionId != older.m_partitionId) {
            throw new IllegalArgumentException("Can't diff these ClientAffinityStats instances.");
        }

        ClientAffinityStats retval = new ClientAffinityStats(older.m_partitionId,
                newer.m_affinityWrites - older.m_affinityWrites,
                newer.m_rrWrites - older.m_rrWrites,
                newer.m_affinityReads - older.m_affinityReads,
                newer.m_rrReads - older.m_rrReads);
        return retval;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    @Override
    protected Object clone() {
        return new ClientAffinityStats(m_partitionId, m_affinityWrites, m_rrWrites, m_affinityReads,
               m_rrReads);
    }

    void addAffinityWrite()
    {
        m_affinityWrites++;
    }

    /**
     * Get the number of writes that used affinity for this time period.
     *
     * @return The count as a long.
     */
    public long getAffinityWrites()
    {
        return m_affinityWrites;
    }

    void addRrWrite()
    {
        m_rrWrites++;
    }

    /**
     * Get the number of writes that used round-robin distribution
     * for this time period.
     *
     * @return The count as a long.
     */
    public long getRrWrites()
    {
        return m_rrWrites;
    }

    void addAffinityRead()
    {
        m_affinityReads++;
    }

    /**
     * Get the number of reads that used affinity for this time period.
     *
     * @return The count as a long.
     */
    public long getAffinityReads()
    {
        return m_affinityReads;
    }

    void addRrRead()
    {
        m_rrReads++;
    }

    /**
     * Get the number of reads that used round-robin distribution
     * for this time period.
     *
     * @return The count as a long.
     */
    public long getRrReads()
    {
        return m_rrReads;
    }

    @Override
    public String toString()
    {
        String afdisplay = "Partition ID %d:  %d affinity writes, %d affinity reads, " +
            "%d round-robin writes, %d round-robin reads";
        return String.format(afdisplay, m_partitionId, m_affinityWrites, m_affinityReads,
                m_rrWrites, m_rrReads);
    }
}
