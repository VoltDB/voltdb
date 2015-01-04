/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;

import com.google_voltpatches.common.collect.ImmutableMap;


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
        // partitioning parameter for SP sysprocs, 0 by default.
        public final int partitionParam;
        // type of partitioning parameter for SP sysprocs
        public final VoltType partitionParamType;
        public final boolean commercial;
        // whether this procedure will terminate replication
        public final boolean terminatesReplication;
        // whether this procedure should be skipped in replication
        public final boolean skipReplication;
        // whether normal clients can call this sysproc in secondary
        public final boolean allowedInReplica;
        //Durable?
        public final boolean durable;

        public Config(String className,
                      boolean singlePartition,
                      boolean readOnly,
                      boolean everySite,
                      int partitionParam,
                      VoltType partitionParamType,
                      boolean commercial,
                      boolean terminatesReplication,
                      boolean skipReplication,
                      boolean allowedInReplica,
                      boolean durable)
        {
            this.className = className;
            this.singlePartition = singlePartition;
            this.readOnly = readOnly;
            this.everySite = everySite;
            this.partitionParam = partitionParam;
            this.partitionParamType = partitionParamType;
            this.commercial = commercial;
            this.terminatesReplication = terminatesReplication;
            this.skipReplication = skipReplication;
            this.allowedInReplica = allowedInReplica;
            this.durable = durable;
        }

        public boolean isDurable() {
            return durable;
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
            Column partitionCol = new Column();
            partitionCol.setType(partitionParamType.getValue());

            Procedure p = new Procedure();
            p.setClassname(className);
            p.setSinglepartition(singlePartition);
            p.setReadonly(readOnly);
            p.setEverysite(everySite);
            p.setSystemproc(true);
            p.setDefaultproc(false);
            p.setHasjava(true);
            p.setPartitiontable(null);
            p.setPartitioncolumn(partitionCol);
            p.setPartitionparameter(partitionParam);
            p.setAttachment(new ProcedurePartitionInfo( partitionParamType, partitionParam ));
            return p;
        }
    }

    public static final ImmutableMap<String, Config> listing;

    static {                                                                                            // SP     RO     Every  Param ParamType       Pro  (DR: kill, skip, replica-ok) durable?)
        ImmutableMap.Builder<String, Config> builder = ImmutableMap.builder();
        builder.put("@AdHoc_RW_MP",             new Config("org.voltdb.sysprocs.AdHoc_RW_MP",              false, false, false, 0, VoltType.INVALID,   false, false, false, true, true));
        builder.put("@AdHoc_RW_SP",             new Config("org.voltdb.sysprocs.AdHoc_RW_SP",              true,  false, false, 0, VoltType.VARBINARY, false, false, false, true, true));
        builder.put("@AdHoc_RO_MP",             new Config("org.voltdb.sysprocs.AdHoc_RO_MP",              false, true,  false, 0, VoltType.INVALID,   false, false, false, true, false));
        builder.put("@AdHoc_RO_SP",             new Config("org.voltdb.sysprocs.AdHoc_RO_SP",              true,  true,  false, 0, VoltType.VARBINARY, false, false, false, true, false));
        builder.put("@Pause",                   new Config("org.voltdb.sysprocs.Pause",                    false, false, true,  0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@Resume",                  new Config("org.voltdb.sysprocs.Resume",                   false, false, true,  0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@Quiesce",                 new Config("org.voltdb.sysprocs.Quiesce",                  false, false, false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@SnapshotSave",            new Config("org.voltdb.sysprocs.SnapshotSave",             false, false, false, 0, VoltType.INVALID,   true,  false, true,  true, false));
        builder.put("@SnapshotRestore",         new Config("org.voltdb.sysprocs.SnapshotRestore",          false, false, false, 0, VoltType.INVALID,   true,  true,  true,  false, false));
        builder.put("@SnapshotStatus",          new Config("org.voltdb.sysprocs.SnapshotStatus",           false, false, false, 0, VoltType.INVALID,   true,  false, true,  true, false));
        builder.put("@SnapshotScan",            new Config("org.voltdb.sysprocs.SnapshotScan",             false, false, false, 0, VoltType.INVALID,   true,  false, true,  true, false));
        builder.put("@SnapshotDelete",          new Config("org.voltdb.sysprocs.SnapshotDelete",           false, false, false, 0, VoltType.INVALID,   true,  false, true,  true, false));
        builder.put("@Shutdown",                new Config("org.voltdb.sysprocs.Shutdown",                 false, false, false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@ProfCtl",                 new Config("org.voltdb.sysprocs.ProfCtl",                  false, false, true,  0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@Statistics",              new Config("org.voltdb.sysprocs.Statistics",               false, true,  false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@SystemCatalog",           new Config("org.voltdb.sysprocs.SystemCatalog",            true,  true,  false, 0, VoltType.STRING,    false, false, true,  true, false));
        builder.put("@SystemInformation",       new Config("org.voltdb.sysprocs.SystemInformation",        false, true,  false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@UpdateLogging",           new Config("org.voltdb.sysprocs.UpdateLogging",            false, false, true,  0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@BalancePartitions",       new Config("org.voltdb.sysprocs.BalancePartitions",        false, false, false, 0, VoltType.INVALID,   true,  true,  true,  false, true));
        builder.put("@UpdateApplicationCatalog",new Config("org.voltdb.sysprocs.UpdateApplicationCatalog", false, false, false, 0, VoltType.INVALID,   false, false, false, false, true));
        builder.put("@LoadMultipartitionTable", new Config("org.voltdb.sysprocs.LoadMultipartitionTable",  false, false, false, 0, VoltType.INVALID,   false, false, false, false, true));
        builder.put("@LoadSinglepartitionTable",new Config("org.voltdb.sysprocs.LoadSinglepartitionTable", true,  false, false, 0, VoltType.VARBINARY, false, false, false, false, true));
        builder.put("@Promote",                 new Config("org.voltdb.sysprocs.Promote",                  false, false, true,  0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@ValidatePartitioning",    new Config("org.voltdb.sysprocs.ValidatePartitioning",     false, false, false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@GetHashinatorConfig",     new Config("org.voltdb.sysprocs.GetHashinatorConfig",      false, true,  false, 0, VoltType.INVALID,   true,  false, true,  true, false));
        builder.put("@ApplyBinaryLogSP",        new Config("org.voltdb.sysprocs.ApplyBinaryLogSP",         true,  false, false, 0, VoltType.VARBINARY, true,  false, true,  true, true));
        builder.put("@Ping",                    new Config(null,                                           true,  true,  false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@GetPartitionKeys",        new Config(null,                                           false, true,  true,  0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@Subscribe",               new Config(null,                                           false, true,  false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@GC",                      new Config(null,                                           true,  false, false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@StopNode",                new Config(null,                                           true,  false, false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@Explain",                 new Config(null,                                           true,  true,  false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@ExplainProc",             new Config(null,                                           true,  true,  false, 0, VoltType.INVALID,   false, false, true,  true, false));
        builder.put("@SendSentinel",            new Config(null,                                           true,  false, false, 0, VoltType.INVALID,   true,  false, false, true, false));
        listing = builder.build();
    }
}
