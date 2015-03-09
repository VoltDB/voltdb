/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 * Collect the client's opinion of the operation of client affinity.  For the
 * given partition ID, affinityWrites tracks write transactions that the client
 * believes it knows the master for.  AffinityReads tracks read transactions
 * the the client believes it found a replica for.  Round-robin stats reflect
 * the client's lack of information when client affinity is on and indicate
 * transactions that were routed using the default round-robin algorithm.
 */
public class ClientAffinityStats {

    private int m_partitionId;
    private long m_affinityWrites;
    private long m_rrWrites;
    private long m_affinityReads;
    private long m_rrReads;

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
