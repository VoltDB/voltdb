package org.voltdb.stats.procedure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Procedure;
import java.util.Map;

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
