package org.voltdb.calciteadapter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ToRelContext;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.VoltType;
import org.voltdb.calciteadapter.rel.VoltDBPartitionedTableScan;
import org.voltdb.calciteadapter.rel.VoltDBTableScan;
import org.voltdb.calciteadapter.rel.VoltDBTableScan;
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

        };
    }

    @Override
    public RelNode toRel(ToRelContext context, RelOptTable relOptTable) {
        if (getCatTable().getIsreplicated()) {
            return new VoltDBTableScan(context.getCluster(), relOptTable, this);
        }

        return new VoltDBPartitionedTableScan(context.getCluster(), relOptTable, this);
    }

    public org.voltdb.catalog.Table getCatTable() {
        return m_catTable;
    }
}
