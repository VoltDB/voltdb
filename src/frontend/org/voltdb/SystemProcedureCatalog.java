/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.catalog.Procedure;

import com.google.common.collect.ImmutableMap;


/**
 * Lists built-in system stored procedures with metadata
 */
public class SystemProcedureCatalog {

    // Historical note:

    // We used to list syprocs in the catalog (inserting them in
    // VoltCompiler). That adds unnecessary content to catalogs,
    // couples catalogs to versions (an old catalog wouldn't be able
    // to invoke a new sysprocs), and complicates the idea of
    // commercial-only sysprocs.

    // Now we maintain this list here in code. This is also not
    // roses - as ProcedureWrapper really wants catalog.Procedures
    // and ClientInterface has to check two lists at dispatch
    // time.

    /* Data about each registered procedure */
    public static class Config {
        public final String className;
        public final boolean readOnly;
        public final boolean singlePartition;
        public final boolean everySite;
        public final boolean commercial;
        // whether this procedure will terminate replication
        public final boolean terminatesReplication;
        // whether this procedure should be skipped in replication
        public final boolean skipReplication;
        // whether normal clients can call this sysproc in secondary
        public final boolean allowedInReplica;

        public Config(String className,
                boolean singlePartition,
                boolean readOnly,
                boolean everySite,
                boolean commercial,
                boolean terminatesReplication,
                boolean skipReplication,
                boolean allowedInReplica)
        {
            this.className = className;
            this.singlePartition = singlePartition;
            this.readOnly = readOnly;
            this.everySite = everySite;
            this.commercial = commercial;
            this.terminatesReplication = terminatesReplication;
            this.skipReplication = skipReplication;
            this.allowedInReplica = allowedInReplica;
        }

        public boolean getEverysite() {
            return everySite;
        }

        public boolean getReadonly() {
            return readOnly;
        }

        public boolean getSinglepartition() {
            return singlePartition;
        }

        public String getClassname() {
            return className;
        }

        Procedure asCatalogProcedure() {
            Procedure p = new Procedure();
            p.setClassname(className);
            p.setSinglepartition(singlePartition);
            p.setReadonly(readOnly);
            p.setEverysite(everySite);
            p.setSystemproc(true);
            p.setDefaultproc(false);
            p.setHasjava(true);
            p.setPartitiontable(null);
            p.setPartitioncolumn(null);
            p.setPartitionparameter(0);
            return p;
        }
    }

    public static final ImmutableMap<String, Config> listing;

    static {                                                                                            // SP     RO     Every  Pro  (DR: kill, skip, replica-ok)
        ImmutableMap.Builder<String, Config> builder = ImmutableMap.builder();
        builder.put("@AdHoc_RW_MP",             new Config("org.voltdb.sysprocs.AdHoc_RW_MP",              false, false, false, false, false, false, true));
        builder.put("@AdHoc_RW_SP",             new Config("org.voltdb.sysprocs.AdHoc_RW_SP",              true,  false, false, false, false, false, true));
        builder.put("@AdHoc_RO_MP",             new Config("org.voltdb.sysprocs.AdHoc_RO_MP",              false, true,  false, false, false, false, true));
        builder.put("@AdHoc_RO_SP",             new Config("org.voltdb.sysprocs.AdHoc_RO_SP",              true,  true,  false, false, false, false, true));
        builder.put("@Pause",                   new Config("org.voltdb.sysprocs.Pause",                    false, false, true,  false, false, true, true));
        builder.put("@Resume",                  new Config("org.voltdb.sysprocs.Resume",                   false, false, true,  false, false, true, true));
        builder.put("@Quiesce",                 new Config("org.voltdb.sysprocs.Quiesce",                  false, false, false, false, false, true, true));
        builder.put("@SnapshotSave",            new Config("org.voltdb.sysprocs.SnapshotSave",             false, false, false, false, false, true, true));
        builder.put("@SnapshotRestore",         new Config("org.voltdb.sysprocs.SnapshotRestore",          false, false, false, false, true, true, false));
        builder.put("@SnapshotStatus",          new Config("org.voltdb.sysprocs.SnapshotStatus",           false, false, false, false, false, true, true));
        builder.put("@SnapshotScan",            new Config("org.voltdb.sysprocs.SnapshotScan",             false, false, false, false, false, true, true));
        builder.put("@SnapshotDelete",          new Config("org.voltdb.sysprocs.SnapshotDelete",           false, false, false, false, false, true, true));
        builder.put("@Shutdown",                new Config("org.voltdb.sysprocs.Shutdown",                 false, false, false, false, false, true, true));
        builder.put("@ProfCtl",                 new Config("org.voltdb.sysprocs.ProfCtl",                  false, false, true, false, false, true, true));
        builder.put("@Statistics",              new Config("org.voltdb.sysprocs.Statistics",               false, true,  false, false, false, true, true));
        builder.put("@SystemCatalog",           new Config("org.voltdb.sysprocs.SystemCatalog",            true,  true,  false, false, false, true, true));
        builder.put("@SystemInformation",       new Config("org.voltdb.sysprocs.SystemInformation",        false, true,  false, false, false, true, true));
        builder.put("@UpdateLogging",           new Config("org.voltdb.sysprocs.UpdateLogging",            false, false, true,  false, false, true, true));
        builder.put("@UpdateTopology",          new Config("org.voltdb.sysprocs.UpdateTopology",           false, false, false, true, true, true, false));
        builder.put("@BalancePartitions",       new Config("org.voltdb.sysprocs.BalancePartitions",        false, false, false, true, true, true, false));
        builder.put("@UpdateApplicationCatalog",new Config("org.voltdb.sysprocs.UpdateApplicationCatalog", false, false, false, true, false, false, false));
        builder.put("@LoadMultipartitionTable", new Config("org.voltdb.sysprocs.LoadMultipartitionTable",  false, false, false, false, false, false, false));
        builder.put("@LoadSinglepartitionTable",new Config("org.voltdb.sysprocs.LoadSinglepartitionTable", true,  false, false, false, false, false, false));
        builder.put("@Promote",                 new Config("org.voltdb.sysprocs.Promote",                  false, false, true, false, false, true, true));
        listing = builder.build();
    }
}
