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

package org.voltdb.sysprocs;

import java.util.concurrent.ExecutionException;

import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

/**
 * Run system garbage collection and return how long it took to run in nanos.
 *
 */
public class GC extends VoltNTSystemProcedure {
    public VoltTable run() throws InterruptedException, ExecutionException {
        final long start = System.nanoTime();
        System.gc();
        final long duration = System.nanoTime() - start;

        VoltTable vt = new VoltTable(
                new ColumnInfo[] { new ColumnInfo("SYSTEM_GC_DURATION_NANOS", VoltType.BIGINT) });
        vt.addRow(duration);

        return vt;
    }
}
