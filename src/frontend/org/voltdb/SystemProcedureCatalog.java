/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

    static enum Restartability {
        NOT_APPLICABLE, /* Only MP can be restarted, SP or NT procedures aren't affected by it. */
        RESTARTABLE, /* When MP failed before completion, MPI will re-initiate the procedure. */
        NOT_RESTARTABLE, /* When MP failed before completion, MPI will NOT re-initiate the procedure. */
    }

    static enum Durability {
        NOT_APPLICABLE, /* NT procedures aren't affected by durability flag. */
        DURABLE, /* SP or MP procedures that need to write into command logs. */
        NOT_DURABLE, /* SP or MP procedures that DON'T need to write into command logs. */
    }

    static enum Initiator {
        SINGLE_PARTITION, /* Executes on one target partition. */
        MULTI_PARTITION, /* Executes on MPI and generates FragmentTasks for each SP Partition. */
        RUN_EVERYWHERE, /* ROUTED TO MPI who scatter gather the procedure call to all SPIs */
    }

    static enum Mutable {
        READ_ONLY,
        READ_WRITE
    }
    /**
     * Builder class to simplify the construction of {@link Config} instances
     */
    static class Builder {
        private final String className;
        private Mutable mutable = Mutable.READ_WRITE;
        private final Initiator initiator;
        private final int partitionParam;
        private final VoltType partitionParamType;
        private boolean commercial = false;
        private boolean terminatesReplication = false;
        private boolean allowedInReplica = false;
        private Durability durable = Durability.DURABLE;
        private boolean allowedInShutdown = false;
        private final boolean transactional;
        private final Restartability restartable;

        static Builder createSp(String className, VoltType partitionParamType) {
            return createSp(className, 0, partitionParamType);
        }

        static Builder createSp(String className, int partitionParam, VoltType partitionParamType) {
            return new Builder(className, Initiator.SINGLE_PARTITION, partitionParam, partitionParamType, true, Restartability.NOT_APPLICABLE);
        }

        static Builder createMp(String className) {
            return new Builder(className, Initiator.MULTI_PARTITION, 0, VoltType.INVALID, true, Restartability.RESTARTABLE);
        }

        static Builder createNp(String className) {
            return createNp(className, 0, VoltType.INVALID);
        }

        static Builder createNp(String className, VoltType partitionParamType) {
            return createNp(className, 0, partitionParamType);
        }
        static Builder createNp(String className, int partitionParam, VoltType partitionParamType) {
            return new Builder(className, Initiator.MULTI_PARTITION, partitionParam, partitionParamType, false, Restartability.NOT_APPLICABLE).notDurable();
        }

        static Builder createEverySite(String className, int partitionParam, VoltType partitionParamType) {
            return new Builder(className, Initiator.RUN_EVERYWHERE, partitionParam, partitionParamType, true, Restartability.NOT_RESTARTABLE);
        }

        private Builder(String className, Initiator initiator, int partitionParam,
                VoltType partitionParamType, boolean transactional, Restartability restartable) {
            this.className = className;
            this.initiator = initiator;
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
            if (initiator == Initiator.MULTI_PARTITION) {
                throw new IllegalArgumentException("RO not supported for MP transactions");
            }
            mutable = Mutable.READ_ONLY;
            durable = Durability.NOT_APPLICABLE;
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
            durable = Durability.NOT_DURABLE;
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


        Config build() {
            return new Config(className, initiator, mutable, partitionParam, partitionParamType,
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
        public final Durability durable;
        // whether this procedure is allowed in cluster shutdown
        public final boolean allowedInShutdown;
        // whether transactional or not
        public final boolean transactional;
        // whether restartable
        public final Restartability restartable;
        // MP Durable and restartable yes
        // MP not durable and restartable yes
        // MP durable and not restartable no
        // MP not durable and not restartable yes

        Config(String className, Initiator initiator, Mutable mutable, int partitionParam, VoltType partitionParamType,
                boolean commercial, boolean terminatesReplication, boolean allowedInReplica, Durability durable,
                boolean allowedInShutdown, boolean transactional, Restartability restartable) {
            Preconditions.checkArgument(!transactional || initiator == Initiator.SINGLE_PARTITION ||
                    durable != Durability.DURABLE || restartable == Restartability.RESTARTABLE ,
                    "Restartable but not durable MP System procedure %s has a risk of "
                    + "corrupting commandlog therefore is disallowed", className);
            this.className = className;
            this.singlePartition = initiator == Initiator.SINGLE_PARTITION;
            this.readOnly = mutable == Mutable.READ_ONLY;
            this.everySite = initiator == Initiator.RUN_EVERYWHERE;
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
            return !readOnly && durable == Durability.DURABLE;
        }

        public boolean isRestartable() {
            return restartable == Restartability.RESTARTABLE;
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
            p.setRestartable(restartable == Restartability.RESTARTABLE);
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
    static {
        final ImmutableMap.Builder<String, Config> builder = ImmutableMap.builder();
        //              SP/MP   RO/RW    ParamNum    ParamType
        //              PRO     killDR   replica-ok  durable
        //              allowedInShutdown  transactional  restartable
        // Special case: replica acceptability by DR version
        builder.put("@AdHoc_RW_MP",
                new Config("org.voltdb.sysprocs.AdHoc_RW_MP",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@AdHoc_RW_SP",
                new Config("org.voltdb.sysprocs.AdHoc_RW_SP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        false, false, false,Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@AdHoc_RO_MP",
                new Config("org.voltdb.sysprocs.AdHoc_RO_MP",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@MigratePartitionLeader",
                new Config("org.voltdb.sysprocs.MigratePartitionLeader",
                        Initiator.SINGLE_PARTITION, Mutable.READ_ONLY, 0, VoltType.BIGINT,
                        false, true,  false, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@AdHoc_RO_SP",
                new Config("org.voltdb.sysprocs.AdHoc_RO_SP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_ONLY, 0, VoltType.VARBINARY,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@JStack",
                new Config(null,
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        true, true, Restartability.NOT_APPLICABLE));
        builder.put("@Pause",
                new Config("org.voltdb.sysprocs.Pause",
                        Initiator.RUN_EVERYWHERE, Mutable.READ_WRITE,  0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@QueryStats",
                new Config("org.voltdb.sysprocs.QueryStats",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@Resume",
                new Config("org.voltdb.sysprocs.Resume",
                        Initiator.RUN_EVERYWHERE, Mutable.READ_WRITE,  0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@Quiesce",
                new Config("org.voltdb.sysprocs.Quiesce",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        true, true, Restartability.NOT_RESTARTABLE));
        builder.put("@SnapshotSave",
                new Config("org.voltdb.sysprocs.SnapshotSave",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        true, true, Restartability.NOT_RESTARTABLE));
        builder.put("@SnapshotRestore",
                new Config("org.voltdb.sysprocs.SnapshotRestore",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, true,false, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_APPLICABLE ));
        builder.put("@SnapshotScan",
                new Config("org.voltdb.sysprocs.SnapshotScan",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@SnapshotDelete",
                new Config("org.voltdb.sysprocs.SnapshotDelete",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@Shutdown",
                new Config("org.voltdb.sysprocs.Shutdown",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        true, true, Restartability.NOT_RESTARTABLE));
        builder.put("@ProfCtl",
                new Config("org.voltdb.sysprocs.ProfCtl",
                        Initiator.RUN_EVERYWHERE, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@Statistics",
                new Config("org.voltdb.sysprocs.Statistics",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        true, true, Restartability.NOT_APPLICABLE));
        builder.put("@SystemCatalog",
                new Config("org.voltdb.sysprocs.SystemCatalog",
                        Initiator.SINGLE_PARTITION, Mutable.READ_ONLY, 0, VoltType.STRING,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put(ClockSkewCollectorAgent.PROCEDURE,
                new Config(null,
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@SystemInformation",
                new Config("org.voltdb.sysprocs.SystemInformation",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@UpdateLogging",
                new Config("org.voltdb.sysprocs.UpdateLogging",
                        Initiator.RUN_EVERYWHERE, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@BalancePartitions",
                new Config("org.voltdb.sysprocs.BalancePartitions",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, false, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@UpdateCore",
                new Config("org.voltdb.sysprocs.UpdateCore",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@VerifyCatalogAndWriteJar",
                new Config("org.voltdb.sysprocs.VerifyCatalogAndWriteJar",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@UpdateApplicationCatalog",
                new Config("org.voltdb.sysprocs.UpdateApplicationCatalog",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@UpdateClasses",
                new Config("org.voltdb.sysprocs.UpdateClasses",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@LoadMultipartitionTable",
                new Config("org.voltdb.sysprocs.LoadMultipartitionTable",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@LoadSinglepartitionTable",
                new Config("org.voltdb.sysprocs.LoadSinglepartitionTable",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        false, false, false, Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE ));
        builder.put("@Promote",
                new Config("org.voltdb.sysprocs.Promote",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ValidatePartitioning",
                new Config("org.voltdb.sysprocs.ValidatePartitioning",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@GetHashinatorConfig",
                new Config("org.voltdb.sysprocs.GetHashinatorConfig",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@ApplyBinaryLogSP",
                new Config("org.voltdb.sysprocs.ApplyBinaryLogSP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        true, false, true, Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@ApplyBinaryLogMP",
                new Config("org.voltdb.sysprocs.ApplyBinaryLogMP",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@LoadVoltTableSP",
                new Config("org.voltdb.sysprocs.LoadVoltTableSP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        true, false, true, Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@LoadVoltTableMP",
                new Config("org.voltdb.sysprocs.LoadVoltTableMP",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        // DR state is determined by state machine and pbd files, that's the reason why ResetDR
        // neither commandlogged or restartable.
        builder.put("@ResetDR",
                new Config("org.voltdb.sysprocs.ResetDR",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        /* @ExecuteTask is a all-in-one system store procedure and should be ONLY used for internal purpose */
        builder.put("@ExecuteTask",
                new Config("org.voltdb.sysprocs.ExecuteTask",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@ExecuteTask_SP",
                new Config("org.voltdb.sysprocs.ExecuteTask_SP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@UpdateSettings",
                new Config("org.voltdb.sysprocs.UpdateSettings",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@Ping",
                new Config(null,
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        true, true, Restartability.NOT_APPLICABLE));
        builder.put("@PingPartitions",
                new Config("org.voltdb.sysprocs.PingPartitions",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@GetPartitionKeys",
                new Config(null,
                        Initiator.RUN_EVERYWHERE, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@Subscribe",
                new Config(null,
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.NOT_RESTARTABLE));
        builder.put("@GC",
                new Config("org.voltdb.sysprocs.GC",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@AdHoc",
                new Config("org.voltdb.sysprocs.AdHoc",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@AdHocSpForTest",
                new Config("org.voltdb.sysprocs.AdHocSpForTest",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@AdHocLarge",
                new Config("org.voltdb.sysprocs.AdHocLarge",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@StopNode",
                new Config(null,
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@PrepareStopNode",
                new Config(null,
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@Explain",
                new Config("org.voltdb.sysprocs.Explain",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ExplainProc",
                new Config("org.voltdb.sysprocs.ExplainProc",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ExplainView",
                new Config("org.voltdb.sysprocs.ExplainView",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ExplainJSON",
                new Config("org.voltdb.sysprocs.ExplainJSON",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ExplainCatalog",
                new Config("org.voltdb.sysprocs.ExplainCatalog",
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@PrepareShutdown",
                new Config("org.voltdb.sysprocs.PrepareShutdown",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        true, true, Restartability.NOT_RESTARTABLE));
        builder.put("@CancelShutdown",
                new Config("org.voltdb.sysprocs.CancelShutdown",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        true, true, Restartability.NOT_RESTARTABLE));
        builder.put("@SwapTables",
                new Config("org.voltdb.sysprocs.SwapTables",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0,    VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@SwapTablesCore",
                new Config("org.voltdb.sysprocs.SwapTablesCore",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@Trace",
                new Config(null,
                        Initiator.MULTI_PARTITION, Mutable.READ_ONLY, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_APPLICABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@CheckUpgradePlanNT",
                new Config("org.voltdb.sysprocs.CheckUpgradePlanNT",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@PrerequisitesCheckNT",
                new Config("org.voltdb.sysprocs.CheckUpgradePlanNT$PrerequisitesCheckNT",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@RestartDRConsumerNT",
                new Config("org.voltdb.sysprocs.RestartDRConsumerNT",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ShutdownNodeDRConsumerNT",
                new Config("org.voltdb.sysprocs.RestartDRConsumerNT$ShutdownNodeDRConsumerNT",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@StartNodeDRConsumerNT",
                new Config("org.voltdb.sysprocs.RestartDRConsumerNT$StartNodeDRConsumerNT",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@NibbleDeleteSP",
                new Config("org.voltdb.sysprocs.NibbleDeleteSP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@NibbleDeleteMP",
                new Config("org.voltdb.sysprocs.NibbleDeleteMP",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@LowImpactDeleteNT",
                new Config("org.voltdb.sysprocs.LowImpactDeleteNT",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@ExportControl",
                new Config("org.voltdb.sysprocs.ExportControl",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.RESTARTABLE));
        // @TopicControl is like @ExportControl but for topic streams in PRO
        builder.put("@TopicControl",
                new Config("org.voltdb.sysprocs.TopicControl",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@MigrateRowsAcked_SP",
                new Config("org.voltdb.sysprocs.MigrateRowsAcked_SP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.DURABLE,
                        true, true, Restartability.NOT_APPLICABLE));
        builder.put("@MigrateRowsAcked_MP",
                new Config("org.voltdb.sysprocs.MigrateRowsAcked_MP",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        false, false, false, Durability.DURABLE,
                        true, true, Restartability.RESTARTABLE));
        builder.put("@MigrateRowsSP",
                new Config("org.voltdb.sysprocs.MigrateRowsSP",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.DURABLE,
                        false, true, Restartability.NOT_APPLICABLE));
        builder.put("@MigrateRowsMP",
                new Config("org.voltdb.sysprocs.MigrateRowsMP",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.VARBINARY,
                        false, false, false, Durability.DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@MigrateRowsNT",
                new Config("org.voltdb.sysprocs.MigrateRowsNT",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.NOT_APPLICABLE,
                        false, false, Restartability.NOT_APPLICABLE));
        builder.put("@MigrateRowsDeleterNT",
                new Config("org.voltdb.sysprocs.MigrateRowsDeleterNT",
                        Initiator.SINGLE_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        false, false, false, Durability.NOT_APPLICABLE,
                        true, false, Restartability.NOT_APPLICABLE));
        builder.put("@ElasticRemoveNT",
                Builder.createNp("org.voltdb.sysprocs.ElasticRemoveNT").commercial().allowedInReplica().build());
        builder.put("@ElasticRemove", Builder.createMp("org.voltdb.sysprocs.ElasticRemove").commercial().build());
        builder.put("@CollectDrSiteTrackers",
                Builder.createMp("org.voltdb.sysprocs.CollectDrSiteTrackers").commercial().notDurable()
                        .allowedInReplica().build());
        builder.put("@StopReplicas",
                new Config("org.voltdb.sysprocs.StopReplicas",
                        Initiator.MULTI_PARTITION, Mutable.READ_WRITE, 0, VoltType.INVALID,
                        true, false, true, Durability.NOT_DURABLE,
                        false, true, Restartability.RESTARTABLE));
        builder.put("@StoreTopicsGroup", Builder
                .createSp("org.voltdb.sysprocs.TopicsProcedures$StoreGroup", VoltType.STRING).commercial().build());
        builder.put("@DeleteTopicsGroup", Builder
                .createSp("org.voltdb.sysprocs.TopicsProcedures$DeleteGroup", VoltType.STRING).commercial().build());
        builder.put("@FetchTopicsGroups",
                Builder.createSp("org.voltdb.sysprocs.TopicsProcedures$FetchGroups", -1, VoltType.INVALID).commercial()
                        .readOnly().notDurable().build());
        builder.put("@CommitTopicsGroupOffsets",
                Builder.createSp("org.voltdb.sysprocs.TopicsProcedures$CommitGroupOffsets", VoltType.STRING)
                        .commercial().build());
        builder.put("@FetchTopicsGroupOffsets",
                Builder.createSp("org.voltdb.sysprocs.TopicsProcedures$FetchGroupOffsets", VoltType.STRING)
                        .commercial().readOnly().build());
        builder.put("@DeleteExpiredTopicsOffsets",
                Builder.createSp("org.voltdb.sysprocs.TopicsProcedures$DeleteExpiredOffsets", -1, VoltType.INVALID)
                        .commercial().build());
        builder.put("@UpdateLicense",
                Builder.createNp("org.voltdb.sysprocs.UpdateLicense").commercial().allowedInReplica().build());
        builder.put("@LicenseValidation",
                Builder.createNp("org.voltdb.sysprocs.UpdateLicense$LicenseValidation").commercial().allowedInReplica().build());
        builder.put("@LiveLicenseUpdate",
                Builder.createNp("org.voltdb.sysprocs.UpdateLicense$LiveLicenseUpdate").commercial().allowedInReplica().build());

        builder.put("@TopicDirectInsertSP",
                Builder.createSp("org.voltdb.sysprocs.TopicDirectInsertSP", -1, VoltType.INVALID)
                        .commercial().build());

        builder.put("@Note",
                Builder.createNp("org.voltdb.sysprocs.LogNote", VoltType.STRING).allowedInReplica().build());
        builder.put("@LogNoteOnHost",
                Builder.createNp("org.voltdb.sysprocs.LogNote$LogNoteOnHost", VoltType.STRING).allowedInReplica().build());

        builder.put("@ValidateDeployment",
                new Config("org.voltdb.sysprocs.ValidateDeployment",
                        Initiator.SINGLE_PARTITION, Mutable.READ_ONLY, 0, VoltType.VARBINARY,
                        false, false, true, Durability.NOT_DURABLE,
                        false, false, Restartability.NOT_APPLICABLE));

        builder.put("@SetReplicableTables",
                Builder.createMp("org.voltdb.sysprocs.SetReplicableTables").commercial().allowedInReplica().build());

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
