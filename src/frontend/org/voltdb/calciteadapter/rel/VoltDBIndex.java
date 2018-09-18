/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter.rel;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.utils.CatalogUtil;

import java.util.ArrayList;
import java.util.List;

import static org.voltdb.calciteadapter.rel.VoltDBTable.toRelDataType;

/**
 * This is an adaptor between <code>org.voltdb.catalog.Index</code> and
 * <code>org.apache.calcite.schema.Table</code>, that wraps a Volt index
 * and expose to calcite planner.
 *
 * In the Calcite world, an index is a special subtype of table. Currently
 * Mike A. retrieves index directly from VoltDB catalog, but it should be
 * better to retrieve from Calcite's <code>SchemaPlus</code> instead.
 *
 * Not in use/tested.
 */
public class VoltDBIndex implements Table {
    private final Index m_catIndex;

    public VoltDBIndex(Index index) {
        assert(index != null);
        m_catIndex = index;
    }

    public Index getCatIndex() {
        return m_catIndex;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new RelDataTypeFactory.Builder(typeFactory) {{
            CatalogUtil
                    .getSortedCatalogItems(getCatIndex().getColumns(), "index")
                    .forEach(catColumnRef -> {
                        final Column col = catColumnRef.getColumn();
                        add(col.getName(), typeFactory.createTypeWithNullability(toRelDataType(typeFactory,
                                VoltType.get((byte)col.getType()),
                                col.getSize()),
                                col.getNullable()));
                    });
        }}.build();
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.INDEX;
    }

    @Override
    public Statistic getStatistic() {
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return null;
            }
            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return false;
            }
            @Override
            public List<RelCollation> getCollations() {
                return new ArrayList<>();
            }
            @Override
            public RelDistribution getDistribution() {
                return null;        // TODO:   /** Returns the distribution of the data in this table. */
            }
            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                return null;
            }
        };
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
                                                SqlNode parent, CalciteConnectionConfig config) {
        return false;
    }
}
