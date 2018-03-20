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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.VoltType;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalTableScan;
import org.voltdb.catalog.Column;
import org.voltdb.utils.CatalogUtil;

public class VoltDBTable implements TranslatableTable {

    final private org.voltdb.catalog.Table m_catTable;

    public VoltDBTable(org.voltdb.catalog.Table table) {
        m_catTable = table;
    }

    public static RelDataType toRelDataType(RelDataTypeFactory typeFactory, VoltType vt, int prec) {
        SqlTypeName sqlTypeName = SqlTypeName.get(vt.toSQLString().toUpperCase());
        RelDataType rdt;
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
        return Schema.TableType.TABLE;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        FieldInfoBuilder builder = typeFactory.builder();
        List<Column> columns = CatalogUtil.getSortedCatalogItems(getCatTable().getColumns(), "index");
        for (Column catColumn : columns) {
            VoltType vt = VoltType.get((byte)catColumn.getType());
            RelDataType rdt = toRelDataType(typeFactory, vt, catColumn.getSize());
            rdt = typeFactory.createTypeWithNullability(rdt, catColumn.getNullable());
            builder.add(catColumn.getName(), rdt);
        }
        return builder.build();
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
                return null;
            }

            @Override
            public List<RelReferentialConstraint> getReferentialConstraints() {
                // TODO Auto-generated method stub
                return null;
            }

        };
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        RelOptCluster cluster = context.getCluster();
        // Start conservatively with a Logical Scan
        RelNode node = new VoltDBLogicalTableScan(cluster,
                cluster.traitSet(),
                relOptTable,
                this);
        return node;
    }

    public org.voltdb.catalog.Table getCatTable() {
        return m_catTable;
    }

    //@Override
    public boolean isRolledUp(String column) {
        // TODO Auto-generated method stub
        return false;
    }

    //@Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            SqlNode parent, CalciteConnectionConfig config) {
        // TODO Auto-generated method stub
        return false;
    }
}
