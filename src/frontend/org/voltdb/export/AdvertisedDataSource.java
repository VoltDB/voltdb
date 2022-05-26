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

package org.voltdb.export;

import java.util.Objects;

/**
 * The Export data source metadata
 */
public class AdvertisedDataSource
{
    final public int partitionId;
    final public String tableName;

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, tableName.hashCode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AdvertisedDataSource)) {
            return false;
        }
        AdvertisedDataSource other = (AdvertisedDataSource) o;
        return partitionId == other.partitionId && Objects.equals(tableName, other.tableName);
    }

    public AdvertisedDataSource(int p_id, String t_name)
    {
        partitionId = p_id;
        tableName = t_name;
    }

    @Override
    public String toString() {
        return "Table: " + tableName + " partition " + partitionId;
    }
}
