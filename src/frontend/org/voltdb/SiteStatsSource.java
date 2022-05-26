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
package org.voltdb;

import java.util.ArrayList;

import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;

/**
 * Super class of sources of statistical information that are tied to an ExecutionSite.
 */
public abstract class SiteStatsSource extends StatsSource {

    public enum SiteStats {
        SITE_ID                   (VoltType.INTEGER);

        public final VoltType m_type;
        SiteStats(VoltType type) { m_type = type; }
    }

    /**
     * CatalogId of the site this source is associated with
     */
    private final long m_siteId;

    public SiteStatsSource(long siteId, boolean isEE) {
        super(isEE);
        this.m_siteId = siteId;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        for (SiteStats col : SiteStats.values()) {
            columns.add(new VoltTable.ColumnInfo(col.name(), col.m_type));
        }
    }

    @Override
    protected <E extends Enum<E>> void populateColumnSchema(ArrayList<ColumnInfo> columns, Class<E> extraColumns) {
        super.populateColumnSchema(columns, SiteStats.class);
        populateExtraColumns(columns, extraColumns);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object rowValues[]) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        rowValues[offset + SiteStats.SITE_ID.ordinal()] = CoreUtils.getSiteIdFromHSId(m_siteId);
        return offset + SiteStats.values().length;
    }
}
