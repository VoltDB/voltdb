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
package org.voltdb.stats.procedure;

import java.util.Map;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Procedure;

public class ReadOnlyProcedureInformation {

    private final Supplier<Map<String, Boolean>> m_procedureInfo;

    private static Supplier<Map<String, Boolean>> getProcedureInformation() {
        return Suppliers.memoize(() -> {
            CatalogContext ctx = VoltDB.instance().getCatalogContext();

            ImmutableMap.Builder<String, Boolean> builder = ImmutableMap.builder();
            for (Procedure p : ctx.procedures) {
                builder.put(p.getClassname(), p.getReadonly());
            }

            return builder.build();
        });
    }

    @VisibleForTesting
    public ReadOnlyProcedureInformation(Supplier<Map<String, Boolean>> m_procedureInfo) {
        this.m_procedureInfo = m_procedureInfo;
    }

    public ReadOnlyProcedureInformation() {
        this(getProcedureInformation());
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isReadOnlyProcedure(String name) {
        return m_procedureInfo.get().getOrDefault(name, false);
    }
}
