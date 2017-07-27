/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.export;

/**
 * The Export data source metadata
 */
public class AdvertisedDataSource
{
    final public int partitionId;
    final public long systemStartTimestamp;
    final public ExportFormat exportFormat;

    /*
     * Enumeration defining what format the blocks of export data are in.
     *
     * New export format added 7.x. This is the the format export uses.
     * In 7.x format, each exported row contains schema of the tuple row -
     * tablename, column info (type, name and length), and data is all
     * wrapped in the row.
     *
     * Updated for 4.4 to use smaller values for integers and a binary variable size
     * representation for decimals so that the format would be more efficient and
     * shareable with other features
     */
    public enum ExportFormat {
        ORIGINAL, FOURDOTFOUR, SEVENDOTX;
    }

    public AdvertisedDataSource(int p_id,
            long systemStartTimestamp,
            ExportFormat exportFormat)
    {
        partitionId = p_id;
        this.systemStartTimestamp = systemStartTimestamp;

        this.exportFormat = exportFormat;
    }

    @Override
    public String toString() {
        return " partition " + partitionId
                + " systemStartTimestamp " + systemStartTimestamp;
    }
}
