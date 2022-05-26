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

import java.util.Objects;

import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Program;
import org.voltdb.plannerv2.rules.PlannerRules;

import com.google.common.collect.ImmutableList;

/**
 * Implementation of {@link FrameworkConfig}
 * describing how to configure VoltDB planning sessions with Calcite.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class VoltFrameworkConfig implements FrameworkConfig {

    /**
     * The list of trait definitions used by the planner.
     */
    @SuppressWarnings("rawtypes")
    private static final ImmutableList<RelTraitDef> TRAIT_DEFS = ImmutableList.of(
            ConventionTraitDef.INSTANCE,
            RelCollationTraitDef.INSTANCE);

    static final Config PARSER_CONFIG = SqlParser.configBuilder()
                         .setParserFactory(SqlDdlParserImpl.FACTORY)
                         .setQuoting(Lex.VOLTDB.quoting)
                         .setUnquotedCasing(Lex.VOLTDB.unquotedCasing)
                         .setQuotedCasing(Lex.VOLTDB.quotedCasing)
                         .setConformance(VoltSqlConformance.INSTANCE).build();
    static final String DEFAULT_SCHEMA_NAME = "public";

    // -- Additional states that do not belong to the original FrameworkConfig interface.
    private final SchemaPlus m_schema;
    private final RelDataTypeFactory m_typeFactory;
    private final Prepare.CatalogReader m_catalogReader;

    @Override public RelDataTypeSystem getTypeSystem() {
        return VoltRelDataTypeSystem.VOLT_REL_DATATYPE_SYSTEM;
    }

    /**
     * Build a {@link VoltFrameworkConfig}.
     *
     * @param schema the converted {@code SchemaPlus} from VoltDB catalog.
     */
    public VoltFrameworkConfig(SchemaPlus schema) {
        m_schema = Objects.requireNonNull(schema, "SchemaPlus is null");
        m_typeFactory = new SqlTypeFactoryImpl(getTypeSystem());
        CalciteSchema calciteSchema = CalciteSchema.from(m_schema);
        m_catalogReader = new CalciteCatalogReader(
                calciteSchema,
                calciteSchema.path(null) /*default schema*/,
                m_typeFactory,
                null /*connection configuration*/
        );
    }

    @Override public SqlParser.Config getParserConfig() {
        return PARSER_CONFIG;
    }

    @Override public SqlToRelConverter.Config getSqlToRelConverterConfig() {
        return VoltSqlToRelConverterConfig.INSTANCE;
    }

    @Override public SchemaPlus getDefaultSchema() {
        return m_schema;
    }

    @Override public RexExecutor getExecutor() {
        // Reduces expressions, and writes their results into {@code reducedValues}.
        return null;
    }

    @Override public ImmutableList<Program> getPrograms() {
        return PlannerRules.getPrograms();
    }

    @Override public SqlOperatorTable getOperatorTable() {
        // use m_catalogReader+SqlStdOperatorTable instead of SqlStdOperatorTable.instance,
        // otherwise the operators/functions we added cannot be found
        return ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance(), getCatalogReader());
    }

    @Override public RelOptCostFactory getCostFactory() {
        // Return null here then the Volcano planner will enable its own
        // default cost factory (VolcanoCost.FACTORY).
        return null;
    }

    @SuppressWarnings("rawtypes")
    @Override public ImmutableList<RelTraitDef> getTraitDefs() {
        return TRAIT_DEFS;
    }

    @Override public SqlRexConvertletTable getConvertletTable() {
        return StandardConvertletTable.INSTANCE;
    }

    @Override public Context getContext() {
        return null;
    }

    /**
     * @return the {@link RelDataTypeFactory} that is used by the VoltlDB planner.
     */
    public RelDataTypeFactory getTypeFactory() {
        return m_typeFactory;
    }

    /**
     * @return the {@link SqlConformance} that is used by the VoltlDB planner.
     */
    public SqlConformance getSqlConformance() {
        return VoltSqlConformance.INSTANCE;
    }

    /**
     * @return the {@link Prepare.CatalogReader} that is used by the VoltlDB planner.
     */
    public Prepare.CatalogReader getCatalogReader() {
        return m_catalogReader;
    }
}
