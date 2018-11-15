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

package org.voltdb.newplanner;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.ArrayList;

/**
 * VoltDB SqlToRelConverter which Converts a SQL parse tree (consisting of
 * {@link org.apache.calcite.sql.SqlNode} objects) into a relational algebra
 * expression (consisting of {@link org.apache.calcite.rel.RelNode} objects).
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class VoltSqlToRelConverter extends SqlToRelConverter {

    /**
     * Creates a VoltDB Converter.
     *
     * @param viewExpander
     * @param validator
     * @param catalogReader
     * @param cluster
     * @param convertletTable
     * @param config
     */
    public VoltSqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
    }

    /**
     * Creates a VoltDB Converter.
     *
     * @param validator
     * @param schemaPlus
     * @return
     */
    public static VoltSqlToRelConverter create(SqlValidator validator, SchemaPlus schemaPlus) {
        final VolcanoPlanner planner = new VolcanoPlanner();
        // We add ConventionTraitDef.INSTANCE and RelCollationTraitDef.INSTANCE.
        // RelDistributionTraitDef will be added in the later stage.
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        final RexBuilder rexBuilder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        RelMetadataProvider pv = cluster.getMetadataProvider();
        final Prepare.CatalogReader reader = new CalciteCatalogReader(
                CalciteSchema.from(schemaPlus),
                new ArrayList<>(),
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                null
        );
        return new VoltSqlToRelConverter(null, validator, reader, cluster,
                StandardConvertletTable.INSTANCE, SqlToRelConverter.Config.DEFAULT);
    }
}
