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

import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilderFactory;

public class VoltSqlToRelConverterConfig implements SqlToRelConverter.Config {
    public static final VoltSqlToRelConverterConfig INSTANCE = new VoltSqlToRelConverterConfig();

    private VoltSqlToRelConverterConfig() {
        super();
    }

    @Override
    public boolean isConvertTableAccess() {
        return this.DEFAULT.isConvertTableAccess();
    }

    @Override
    public boolean isDecorrelationEnabled() {
        return this.DEFAULT.isDecorrelationEnabled();
    }

    @Override
    public boolean isTrimUnusedFields() {
        return true;
    }

    @Override
    public boolean isCreateValuesRel() {
        return this.DEFAULT.isCreateValuesRel();
    }

    @Override
    public boolean isExplain() {
        return this.DEFAULT.isExplain();
    }

    @Override
    public boolean isExpand() {
        return this.DEFAULT.isCreateValuesRel();
    }

    @Override
    public int getInSubQueryThreshold() {
        // forces usage of OR rather than join against an inline table in all cases.
        return Integer.MAX_VALUE;
    }

    @Override
    public RelBuilderFactory getRelBuilderFactory() {
        return this.DEFAULT.getRelBuilderFactory();
    }
}
