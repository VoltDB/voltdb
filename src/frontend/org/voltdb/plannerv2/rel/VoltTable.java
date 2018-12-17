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

package org.voltdb.plannerv2.rel;

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
import org.voltdb.plannerv2.rel.logical.VoltDBLTableScan;
import org.voltdb.utils.CatalogUtil;

/**
 * An adapter between a {@link org.voltdb.catalog.Table} and a {@link TranslateableTable}
 * which can be translated into a relational expression ({@link org.apache.calcite.rel.RelNode}).
 * A relational expression can fulfill roles to be sort-able, join-able, project-able,
 * filter-able, scan-able, and sample-able.
 * @since 8.4
 * @author Michael Alexeev
 */
public class VoltTable implements TranslatableTable {

    final private org.voltdb.catalog.Table m_catTable;

    /**
     * Build a {@code VoltTable} from a catalog table.
     * @param catTable the catalog table.
     */
    public VoltTable(org.voltdb.catalog.Table catTable) {
        assert(catTable != null) : "Null org.voltdb.catalog.Table";
        m_catTable = catTable;
    }

    /**
     *
     * @param typeFactory
     * @param vt
     * @param prec
     * @return
     */
    public static RelDataType toRelDataType(RelDataTypeFactory typeFactory, VoltType vt, int prec) {
        SqlTypeName sqlTypeName = SqlTypeName.get(vt.toSQLString().toUpperCase());
        RelDataType rdt;
        // This doesn't seem quite right. Build a VoltDB TypeSystem inherits from RelDataTypeSystemImpl,
        // ENG-14727
        switch (vt) {
            case STRING:
                // This doesn't seem quite right...
                rdt = typeFactory.createSqlType(sqlTypeName, prec);
                //rdt = typeFactory.createTypeWithCharsetAndCollation(rdt, Charset.forName("UTF-8"), SqlCollation.IMPLICIT);
                break;
            case VARBINARY:
                // The default precision for VARBINARY and VARCHAR (STRING) is not specified.
                rdt = typeFactory.createSqlType(sqlTypeName, prec);
                break;
            default:
                rdt = typeFactory.createSqlType(sqlTypeName);
        }
        return rdt;
    }

    @Override
    public TableType getJdbcTableType() {
        return TableType.TABLE;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new RelDataTypeFactory.Builder(typeFactory) {{
            CatalogUtil
                    .getSortedCatalogItems(getCatTable().getColumns(), "index")
                    .forEach(catColumn ->
                            add(catColumn.getName(),
                                    typeFactory.createTypeWithNullability(toRelDataType(typeFactory,
                                            VoltType.get((byte)catColumn.getType()),
                                            catColumn.getSize()),
                                            catColumn.getNullable())));
        }}.build();
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
                if (m_catTable.getIsreplicated()) {
                    return RelDistributions.SINGLETON;
                } else {
                    Column partitionColumn = m_catTable.getPartitioncolumn();
                    List<Integer> partitionColumnIds = Collections.list(partitionColumn.getIndex());
                    RelDistribution hashDist = RelDistributions.hash(partitionColumnIds);
                    return hashDist;
                }
            }
            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                /** TODO: Returns the collection of referential constraints (foreign-keys)
                 * for this table. */
                return null;
            }
        };
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        // Start conservatively with a Logical Scan
        return new VoltDBLTableScan(cluster,
                cluster.traitSet(),
                relOptTable,
                this);
    }

    public org.voltdb.catalog.Table getCatTable() {
        return m_catTable;
    }

    @Override
    public boolean isRolledUp(String column) { // VoltDB does not support RollUp
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            SqlNode parent, CalciteConnectionConfig config) { // VoltDB does not support RollUp
        return false;
    }
}
