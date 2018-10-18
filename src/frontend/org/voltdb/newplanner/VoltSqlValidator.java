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
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.ArrayList;


/**
 * VoltDB SQL validator.
 * @author Yiqun Zhang
 * @since 8.4
 */
public class VoltSqlValidator extends SqlValidatorImpl {

    /**
     * Build a VoltDB SQL validator.
     * @param opTab
     * @param catalogReader
     * @param typeFactory
     * @param conformance
     */
    public VoltSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory, SqlConformance conformance) {
        super(opTab, catalogReader, typeFactory, conformance);
    }

    /**
     * Build a VoltDB SQL validator from {@link SchemaPlus}.
     *
     * @param schemaPlus
     */
    public VoltSqlValidator(SchemaPlus schemaPlus) {
        // TODO: currently we are using the default implementation of SqlOperatorTable, RelDataTypeFactory
        // and SqlConformance. May replace them with our own versions in the future.
        this(
                SqlStdOperatorTable.instance(),
                new CalciteCatalogReader(
                        CalciteSchema.from(schemaPlus),
                        new ArrayList<>(),
                        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                        null
                ),
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                SqlConformanceEnum.DEFAULT
        );
    }
}
