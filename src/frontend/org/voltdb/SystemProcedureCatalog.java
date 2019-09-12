/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.List;

import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FragmentTaskMessage;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;


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

    /**
     * Builder class to simplify the construction of {@link Config} instances
     */
    static class Builder {
        private final String className;
        private boolean readOnly = false;
        private final boolean singlePartition;
        private final boolean everySite;
        private final int partitionParam;
        private final VoltType partitionParamType;
        private boolean commercial = false;
        private boolean terminatesReplication = false;
        private boolean allowedInReplica = false;
        private boolean durable = true;
        private boolean allowedInShutdown = false;
        private final boolean transactional;
        private boolean restartable;

        static Builder createSp(String className, VoltType partitionParamType) {
            return createSp(className, 0, partitionParamType);
        }

        static Builder createSp(String className, int partitionParam, VoltType partitionParamType) {
            return new Builder(className, true, false, partitionParam, partitionParamType, true, false);
        }

        static Builder createMp(String className) {
            return new Builder(className, false, false, 0, VoltType.INVALID, true, true);
        }

        static Builder createNp(String className) {
            return new Builder(className, false, false, 0, VoltType.INVALID, false, false).notDurable()
                    .notRestartable();
        }

        static Builder createEverySite(String className, int partitionParam, VoltType partitionParamType) {
            return new Builder(className, false, true, partitionParam, partitionParamType, true, false);
        }

        private Builder(String className, boolean singlePartition, boolean everySite, int partitionParam,
                VoltType partitionParamType, boolean transactional, boolean restartable) {
            this.className = className;
            this.singlePartition = singlePartition;
            this.everySite = everySite;
            this.partitionParam = partitionParam;
            this.partitionParamType = partitionParamType;
            this.transactional = transactional;
            this.restartable = restartable;
        }

        /**
         * Mark this procedure as read only
         *
         * @return {@code this}
         */
        Builder readOnly() {
            if (!singlePartition) {
                throw new IllegalArgumentException("RO not supported for MP transactions");
            }
            readOnly = true;
            return this;
        }

        /**
         * Mark this procedure as part of the commercial product and not the community edition
         *
         * @return {@code this}
         */
        Builder commercial() {
            commercial = true;
            return this;
        }

        /**
         * Mark this procedure as one that terminates replication
         *
         * @return {@code this}
         */
        Builder terminatesReplication() {
            terminatesReplication = true;
            return this;
        }

        /**
         * Mark this procedure as allowed to be run on a read only replica system
         *
         * @return {@code this}
         */
        Builder allowedInReplica() {
            allowedInReplica = true;
            return this;
        }

        /**
         * Mark this procedure to not be included in command logging
         *
         * @return {@code this}
         */
        Builder notDurable() {
            durable = false;
            return this;
        }

        /**
         * Mark this procedure as being able to be executed while the system is shutting down
         *
         * @return {@code this}
         */
        Builder allowedInShutdown() {
            allowedInShutdown = true;
            return this;
        }

        /**
         * Mark this MP procedure as not restartable if a node failure or leader change occurs while it is executing
         *
         * @return {@code this}
         */
        Builder notRestartable() {
            restartable = false;
            return this;
        }

        Config build() {
            return new Config(className, singlePartition, readOnly, everySite, partitionParam, partitionParamType,
                    commercial, terminatesReplication, allowedInReplica, durable, allowedInShutdown, transactional,
                    restartable);
        }
    }

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
        // whether normal clients can call this sysproc in secondary
        public final boolean allowedInReplica;
        //Durable?
        public final boolean durable;
        // whether this procedure is allowed in cluster shutdown
        public final boolean allowedInShutdown;
        // whether transactional or not
        public final boolean transactional;
        // whether restartable
        public final boolean restartable;
        // MP Durable and restartable yes
        // MP not durable and restartable yes
        // MP durable and not restartable no
        // MP not durable and not restartable yes

        Config(String className, boolean singlePartition, boolean readOnly, boolean everySite, int partitionParam,
                VoltType partitionParamType, boolean commercial, boolean terminatesReplication, boolean allowedInReplica,
                boolean durable, boolean allowedInShutdown, boolean transactional, boolean restartable) {
            Preconditions.checkArgument(!transactional || singlePartition || restartable || !durable,
                    "Restartable but not durable MP System procedure %s has a risk of "
                    + "corrupting commandlog therefore is disallowed", className);
            this.className = className;
            this.singlePartition = singlePartition;
            this.readOnly = readOnly;
            this.everySite = everySite;
            this.partitionParam = partitionParam;
            this.partitionParamType = partitionParamType;
            this.commercial = commercial;
            this.terminatesReplication = terminatesReplication;
            this.allowedInReplica = allowedInReplica;
            this.durable = durable;
            this.allowedInShutdown = allowedInShutdown;
            this.transactional = transactional;
            this.restartable = restartable;
        }

        public boolean isDurable() {
            return durable;
        }

        public boolean isRestartable() {
            return restartable;
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
            p.setAllowedinshutdown(allowedInShutdown);
            p.setTransactional(transactional);
            p.setRestartable(restartable);
            return p;
        }
    }

    public static final ImmutableMap<String, Config> listing;

    // Cache the fragments of VoltSysemProcedure which should be processed
    // when TaskLogs are replayed during rejoining.
    static ImmutableSet<Long> s_allowableSysprocFragsInTaskLog;

    // Cache VoltSysemProcedure by name which should be processed
    // when TaskLogs are replayed during rejoining.
    static ImmutableSet<String> s_allowableSysprocsInTaskLog;
    static {                                                                                            // SP     RO     Every  Param ParamType           PRO    killDR replica-ok durable allowedInShutdown transactional restartable
        // special-case replica acceptability by DR version
        final ImmutableMap.Builder<String, Config> builder = ImmutableMap.builder();
        builder.put("@AdHoc_RW_MP",
                new Config("org.voltdb.sysprocs.AdHoc_RW_MP",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, false, true,
                        false, true, true));
        builder.put("@AdHoc_RW_SP",
                new Config("org.voltdb.sysprocs.AdHoc_RW_SP",
                        true,  false, false, 0, VoltType.VARBINARY,
                        false, false, false,true,
                        false, true, true));
        builder.put("@AdHoc_RO_MP",
                new Config("org.voltdb.sysprocs.AdHoc_RO_MP",
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, true));
        builder.put("@MigratePartitionLeader",
                new Config("org.voltdb.sysprocs.MigratePartitionLeader",
                        true, true, false, 0, VoltType.BIGINT,
                        false, true,  false,false,
                        false, true, false));
        builder.put("@AdHoc_RO_SP",
                new Config("org.voltdb.sysprocs.AdHoc_RO_SP",
                        true,  true,  false, 0, VoltType.VARBINARY,
                        false, false, true,false,
                        false, true, true));
        builder.put("@JStack",
                new Config(null,
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@Pause",
                new Config("org.voltdb.sysprocs.Pause",
                        false, false, true,  0, VoltType.INVALID,
                        false, false, true,false,
                        false, true, false));
        builder.put("@QueryStats",
                new Config("org.voltdb.sysprocs.QueryStats",
                        false, true, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@Resume",
                new Config("org.voltdb.sysprocs.Resume",
                        false, false, true,  0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@Quiesce",
                new Config("org.voltdb.sysprocs.Quiesce",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@SnapshotSave",
                new Config("org.voltdb.sysprocs.SnapshotSave",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@SnapshotRestore",
                new Config("org.voltdb.sysprocs.SnapshotRestore",
                        false, false, false, 0, VoltType.INVALID,
                        false, true,false, false,
                        false, true, false ));
        builder.put("@SnapshotStatus",
                new Config("org.voltdb.sysprocs.SnapshotStatus",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@SnapshotScan",
                new Config("org.voltdb.sysprocs.SnapshotScan",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@SnapshotDelete",
                new Config("org.voltdb.sysprocs.SnapshotDelete",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@Shutdown",
                new Config("org.voltdb.sysprocs.Shutdown",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@ProfCtl",
                new Config("org.voltdb.sysprocs.ProfCtl",
                        false, false, true, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@Statistics",
                new Config("org.voltdb.sysprocs.Statistics",
                        false, true, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@SystemCatalog",
                new Config("org.voltdb.sysprocs.SystemCatalog",
                        true, true, false, 0, VoltType.STRING,
                        false, false, true, false,
                        false, true, false));
        builder.put("@SystemInformation",
                new Config("org.voltdb.sysprocs.SystemInformation",
                        false, true, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@UpdateLogging",
                new Config("org.voltdb.sysprocs.UpdateLogging",
                        false, false, true, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@BalancePartitions",
                new Config("org.voltdb.sysprocs.BalancePartitions",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, false, true,
                        false, true, true));
        builder.put("@UpdateCore",
                new Config("org.voltdb.sysprocs.UpdateCore",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, true, true));
        builder.put("@VerifyCatalogAndWriteJar",
                new Config("org.voltdb.sysprocs.VerifyCatalogAndWriteJar",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, false, false));
        builder.put("@UpdateApplicationCatalog",
                new Config("org.voltdb.sysprocs.UpdateApplicationCatalog",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, false, false));
        builder.put("@UpdateClasses",
                new Config("org.voltdb.sysprocs.UpdateClasses",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, false, false));
        builder.put("@LoadMultipartitionTable",
                new Config("org.voltdb.sysprocs.LoadMultipartitionTable",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, false, true,
                        false, true, true));
        builder.put("@LoadSinglepartitionTable",
                new Config("org.voltdb.sysprocs.LoadSinglepartitionTable",
                        true,  false, false, 0, VoltType.VARBINARY,
                        false, false, false, true,
                        false, true, false ));
        builder.put("@Promote",
                new Config("org.voltdb.sysprocs.Promote",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@ValidatePartitioning",
                new Config("org.voltdb.sysprocs.ValidatePartitioning",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@GetHashinatorConfig",
                new Config("org.voltdb.sysprocs.GetHashinatorConfig",
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@ApplyBinaryLogSP",
                new Config("org.voltdb.sysprocs.ApplyBinaryLogSP",
                        true,  false, false, 0, VoltType.VARBINARY,
                        true, false, true, true,
                        false, true, false));
        builder.put("@ApplyBinaryLogMP",
                new Config("org.voltdb.sysprocs.ApplyBinaryLogMP",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, true,
                        false, true, true));
        builder.put("@LoadVoltTableSP",
                new Config("org.voltdb.sysprocs.LoadVoltTableSP",
                        true,  false, false, 0, VoltType.VARBINARY,
                        true, false, true, true,
                        false, true, false));
        builder.put("@LoadVoltTableMP",
                new Config("org.voltdb.sysprocs.LoadVoltTableMP",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, true,
                        false, true, true));
        builder.put("@ResetDR",
                new Config("org.voltdb.sysprocs.ResetDR",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, true, false));
        /* @ExecuteTask is a all-in-one system store procedure and should be ONLY used for internal purpose */
        builder.put("@ExecuteTask",
                new Config("org.voltdb.sysprocs.ExecuteTask",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, true, true));
        builder.put("@ExecuteTask_SP",
                new Config("org.voltdb.sysprocs.ExecuteTask_SP",
                        true, false, false, 0, VoltType.VARBINARY,
                        false, false, true, true,
                        false, true, false));
        builder.put("@UpdateSettings",
                new Config("org.voltdb.sysprocs.UpdateSettings",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, true, true));
        builder.put("@Ping",
                new Config(null,
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@PingPartitions",
                new Config("org.voltdb.sysprocs.PingPartitions",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, true));
        builder.put("@GetPartitionKeys",
                new Config(null,
                        false, true,  true,  0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@Subscribe",
                new Config(null,
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@GC",
                new Config("org.voltdb.sysprocs.GC",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@AdHoc",
                new Config("org.voltdb.sysprocs.AdHoc",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, true));
        builder.put("@AdHocSpForTest",
                new Config("org.voltdb.sysprocs.AdHocSpForTest",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, true));
        builder.put("@AdHocLarge",
                new Config("org.voltdb.sysprocs.AdHocLarge",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, true));
        builder.put("@StopNode",
                new Config(null,
                        true,  false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@PrepareStopNode",
                new Config(null,
                        true,  false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@Explain",
                new Config("org.voltdb.sysprocs.Explain",
                        false, true, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@ExplainProc",
                new Config("org.voltdb.sysprocs.ExplainProc",
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@ExplainView",
                new Config("org.voltdb.sysprocs.ExplainView",
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@ExplainJSON",
                new Config("org.voltdb.sysprocs.ExplainJSON",
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@ExplainCatalog",
                new Config("org.voltdb.sysprocs.ExplainCatalog",
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, false, false));
        builder.put("@SendSentinel",
                new Config(null,
                        true,  false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, true, false));
        builder.put("@PrepareShutdown",
                new Config("org.voltdb.sysprocs.PrepareShutdown",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@CancelShutdown",
                new Config("org.voltdb.sysprocs.CancelShutdown",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        true, true, false));
        builder.put("@SwapTables",
                new Config("org.voltdb.sysprocs.SwapTables",
                        false, false, false, 0,    VoltType.INVALID,
                        false, false, true, true,
                        false, false, false));
        builder.put("@SwapTablesCore",
                new Config("org.voltdb.sysprocs.SwapTablesCore",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, true, true));
        builder.put("@Trace",
                new Config(null,
                        false, true,  false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@CheckUpgradePlanNT",
                new Config("org.voltdb.sysprocs.CheckUpgradePlanNT",
                        true,  false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, false, false));
        builder.put("@PrerequisitesCheckNT",
                new Config("org.voltdb.sysprocs.CheckUpgradePlanNT$PrerequisitesCheckNT",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, false, false));
        builder.put("@RestartDRConsumerNT",
                new Config("org.voltdb.sysprocs.RestartDRConsumerNT",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, false, false));
        builder.put("@ShutdownNodeDRConsumerNT",
                new Config("org.voltdb.sysprocs.RestartDRConsumerNT$ShutdownNodeDRConsumerNT",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, false, false));
        builder.put("@StartNodeDRConsumerNT",
                new Config("org.voltdb.sysprocs.RestartDRConsumerNT$StartNodeDRConsumerNT",
                        false, false, false, 0, VoltType.INVALID,
                        true, false, true, false,
                        false, false, false));
        builder.put("@NibbleDeleteSP",
                new Config("org.voltdb.sysprocs.NibbleDeleteSP",
                        true, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, true, true));
        builder.put("@NibbleDeleteMP",
                new Config("org.voltdb.sysprocs.NibbleDeleteMP",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, true,
                        false, true, true));
        builder.put("@LowImpactDeleteNT",
                new Config("org.voltdb.sysprocs.LowImpactDeleteNT",
                        true, false, false, 0, VoltType.INVALID,
                        false, false, false, false,
                        false, false, false));
        builder.put("@ExportControl",
                new Config("org.voltdb.sysprocs.ExportControl",
                        false, false, false, 0, VoltType.INVALID,
                        false, false, true, false,
                        false, true, false));
        builder.put("@MigrateRowsAcked_SP",
                new Config("org.voltdb.sysprocs.MigrateRowsAcked_SP",
                        true, false, false, 0, VoltType.INVALID,
                        false, false, false, true,
                        true, true, true));
        builder.put("@MigrateRowsAcked_MP",
                new Config("org.voltdb.sysprocs.MigrateRowsAcked_MP",
                        false, false, false, 0, VoltType.VARBINARY,
                        false, false, false, true,
                        true, true, true));
        builder.put("@MigrateRowsSP",
                new Config("org.voltdb.sysprocs.MigrateRowsSP",
                        true, false, false, 0, VoltType.INVALID,
                        false, false, false, true,
                        false, true, true));
        builder.put("@MigrateRowsMP",
                new Config("org.voltdb.sysprocs.MigrateRowsMP",
                        false, false, false, 0, VoltType.VARBINARY,
                        false, false, false, true,
                        false, true, true));
        builder.put("@MigrateRowsNT",
                new Config("org.voltdb.sysprocs.MigrateRowsNT",
                        true, false, false, 0, VoltType.INVALID,
                        false, false, false, true,
                        false, false, false));
        builder.put("@MigrateRowsDeleterNT",
                new Config("org.voltdb.sysprocs.MigrateRowsDeleterNT",
                        true, false, false, 0, VoltType.INVALID,
                        false, false, false, true,
                        true, false, false));
        builder.put("@ElasticRemoveNT",
                Builder.createNp("org.voltdb.sysprocs.ElasticRemoveNT").commercial().allowedInReplica().build());
        builder.put("@ElasticRemove", Builder.createMp("org.voltdb.sysprocs.ElasticRemove").commercial().build());
        builder.put("@CollectDrSiteTrackers",
                Builder.createMp("org.voltdb.sysprocs.CollectDrSiteTrackers").commercial().notDurable()
                        .allowedInReplica().build());
        listing = builder.build();
    }

    // Set up the cache when system procedures are loaded on execution sites.
    public static void setupAllowableSysprocFragsInTaskLog(List<Long> fragments, List<String> procs) {
        if (s_allowableSysprocFragsInTaskLog == null && s_allowableSysprocsInTaskLog == null){
            synchronized(SystemProcedureCatalog.class) {
                if (s_allowableSysprocFragsInTaskLog == null && s_allowableSysprocsInTaskLog == null) {
                    s_allowableSysprocFragsInTaskLog = ImmutableSet.<Long>builder().addAll(fragments).build();
                    s_allowableSysprocsInTaskLog = ImmutableSet.<String>builder().addAll(procs).build();
                }
            }
        }
    }

    // return true if the fragment or the system procedure is allowed to be replayed in TaskLog
    public static boolean isAllowableInTaskLog(Long fragId, FragmentTaskMessage msg) {
        if(s_allowableSysprocFragsInTaskLog == null || s_allowableSysprocsInTaskLog == null) {
            return true;
        }

        // Check specified fragment IDs
        if (s_allowableSysprocFragsInTaskLog.contains(fragId)) {
            return true;
        }

        // If fragId is not in the allowed list, check proc name.
        String procName = msg.getProcedureName();
        if (procName != null) {
            return s_allowableSysprocsInTaskLog.contains(procName);
        }
        return false;
    }
}
