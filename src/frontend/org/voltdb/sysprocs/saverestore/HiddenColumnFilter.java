/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.voltdb.TableType;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.collect.ImmutableSet;

/**
 * Enum to describe any schema filtering which is to be performed during a snapshot.
 * <p>
 * Must be in sync with EE enum of the same name
 */
public enum HiddenColumnFilter {
    /** Perform no filtering */
    NONE(0, ImmutableSet.of()),
    /** Exclude the hidden migrate column from the snapshot */
    EXCLUDE_MIGRATE(1, ImmutableSet.of(CatalogUtil.MIGRATE_HIDDEN_COLUMN_INFO));

    private final byte m_id;
    private final Set<ColumnInfo> m_excludes;

    private HiddenColumnFilter(int id, Set<ColumnInfo> excludes) {
        m_id = (byte) id;
        m_excludes = excludes;
    }

    /**
     * Create a {@link VoltTable} instance which describes the schema for {@code table} with any hidden columns that are
     * part of the table added.
     *
     * @param cluster {@link Cluster} catalog
     * @param table   {@link Table} which the schema will be based upon
     * @return {@link VoltTable} with hidden columns added
     */
    public VoltTable createSchema(Cluster cluster, Table table) {
        return createSchema(DrRoleType.XDCR.value().equals(cluster.getDrrole()), table);
    }

    /**
     * Create a {@link VoltTable} instance which describes the schema for {@code table} with any hidden columns that are
     * part of the table added.
     *
     * @param isXdcr Whether or not xdcr is enabled
     * @param table  {@link Table} which the schema will be based upon
     * @return {@link VoltTable} with hidden columns added
     */
    public VoltTable createSchema(boolean isXdcr, Table table) {
        List<ColumnInfo> columns = CatalogUtil.getSortedCatalogItems(table.getColumns(), "index").stream()
                .map(CatalogUtil::catalogColumnToInfo).collect(Collectors.toList());
        int visibleColumnCount = columns.size();

        if (isXdcr && table.getIsdred()) {
            addHiddenColumn(columns, CatalogUtil.DR_HIDDEN_COLUMN_INFO, visibleColumnCount);
        }
        if (CatalogUtil.needsViewHiddenColumn(table)) {
            addHiddenColumn(columns, CatalogUtil.VIEW_HIDDEN_COLUMN_INFO, visibleColumnCount);
        }
        if (TableType.isPersistentMigrate(table.getTabletype())) {
            addHiddenColumn(columns, CatalogUtil.MIGRATE_HIDDEN_COLUMN_INFO, visibleColumnCount);
        }

        return new VoltTable(columns.toArray(new ColumnInfo[columns.size()]));
    }

    private void addHiddenColumn(List<ColumnInfo> columns, ColumnInfo hiddenColumn, int visibleColumnCount) {
        if (!m_excludes.contains(hiddenColumn)) {
            assert ((hiddenColumn
                    .equals(CatalogUtil.VIEW_HIDDEN_COLUMN_INFO) == (columns.size() - visibleColumnCount == 0))
                    || !hiddenColumn.equals(CatalogUtil.VIEW_HIDDEN_COLUMN_INFO))
                    && !columns
                            .contains(CatalogUtil.VIEW_HIDDEN_COLUMN_INFO) : "View hidden column must be alone: adding "
                                    + hiddenColumn + " to " + columns;
            columns.add(hiddenColumn);
        }
    }

    public byte getId() {
        return m_id;
    }
}
