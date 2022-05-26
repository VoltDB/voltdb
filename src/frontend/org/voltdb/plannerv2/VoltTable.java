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

package org.voltdb.plannerv2;

import java.util.ArrayList;
import java.util.List;

import org.aeonbits.owner.util.Collections;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;
import org.voltdb.utils.CatalogUtil;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link TranslateableTable} representing a {@link org.voltdb.catalog.Table}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltTable implements TranslatableTable {

    private final org.voltdb.catalog.Table m_catTable;

    /**
     * Build a {@code VoltTable} from a catalog table.
     *
     * @param catTable the catalog table.
     */
    public VoltTable(org.voltdb.catalog.Table catTable) {
        Preconditions.checkNotNull(catTable, "org.voltdb.catalog.Table cannot be null");
        m_catTable = catTable;
    }

    @Override public TableType getJdbcTableType() {
        return TableType.TABLE;
    }

    @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new RelDataTypeFactory.Builder(typeFactory) {{
            CatalogUtil.getSortedCatalogItems(getCatalogTable().getColumns(), "index")
                .forEach(catColumn ->
                    add(catColumn.getName(),
                        typeFactory.createTypeWithNullability(
                                toRelDataType(typeFactory,
                                              VoltType.get((byte)catColumn.getType()),
                                              catColumn.getSize()),
                                              catColumn.getNullable())));
        }}.build();
    }

    @Override public Statistic getStatistic() {
        return new Statistic() {
            //Rough estimate for number of rows in a table
            static public final double ESTIMATE_TABLE_ROW_COUNT = 1000000.;

            @Override public Double getRowCount() {
                return ESTIMATE_TABLE_ROW_COUNT;
            }
            @Override public boolean isKey(ImmutableBitSet columns) {
                return false;
            }
            @Override public List<RelCollation> getCollations() {
                return new ArrayList<>();
            }
            @Override public RelDistribution getDistribution() {
                if (m_catTable.getIsreplicated()) {
                    return RelDistributions.SINGLETON;
                } else {
                    Column partitionColumn = m_catTable.getPartitioncolumn();
                    // partitionColumn == null when it is a Multi-partitioned view
                    if (partitionColumn == null) {
                        return RelDistributions.RANDOM_DISTRIBUTED;
                    }
                    List<Integer> partitionColumnIds = Collections.list(partitionColumn.getIndex());
                    RelDistribution hashDist = RelDistributions.hash(partitionColumnIds);
                    return hashDist;
                }
            }
            @Override public List<RelReferentialConstraint> getReferentialConstraints() {
                /** TODO: Returns the collection of referential constraints (foreign-keys)
                 * for this table. */
                return null;
            }
        };
    }

    @Override public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        // The corresponding relational expression for the table is VoltLogicalTableScan.
        return new VoltLogicalTableScan(cluster,
                cluster.traitSet(),
                relOptTable,
                this);
    }

    @Override public boolean isRolledUp(String column) {
        // VoltDB does not support RollUp
        return false;
    }

    @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            SqlNode parent, CalciteConnectionConfig config) {
        // VoltDB does not support RollUp
        return false;
    }

    @Override public Integer getPartitionColumn() {
        if (m_catTable.getPartitioncolumn() == null) {  // could be either replicated table, or view
            return null;
        } else {
            return m_catTable.getPartitioncolumn().getIndex();
        }
    }

    /**
     * @return the table information stored in the VoltDB catalog.
     */
    public org.voltdb.catalog.Table getCatalogTable() {
        return m_catTable;
    }

    /**
     * Create a {@link RelDataType} from a {@link VoltType}.
     *
     * @param typeFactory the RelDataTypeFactory.
     * @param vt VoltDB type.
     * @param prec the precision
     * @return the created {@link org.apache.calcite.rel.type.RelDataType}.
     */
    public static RelDataType toRelDataType(RelDataTypeFactory typeFactory, VoltType vt, int prec) {
        SqlTypeName sqlTypeName = ColumnTypes.getCalciteType(vt);
        RelDataType rdt;
        switch (vt) {
            case STRING:
            case VARBINARY:
            case GEOGRAPHY:
                // The precision for VARBINARY, VARCHAR (STRING) and GEOGRAPHY is set in CreateTableUtils.addColumn
                rdt = typeFactory.createSqlType(sqlTypeName, prec);
                break;
            default:
                rdt = typeFactory.createSqlType(sqlTypeName);
        }
        return rdt;
    }
}
