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

package org.voltdb.utils;
/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN voltdb_logstrings.properties GENERATOR */
/**
 * Keys for internationalized log strings in the voltdb_logstrings resource bundle
 *
 */
public enum LogKeys {
    //Contains log strings that can be replaced with internationalized versions
    //in more specialized bundles
    //HOST log strings
    host_ExecutionSite_DependencyNotFound,
    host_ExecutionSite_DependencyContainedNull,
    host_ExecutionSite_DependencyNotVoltTable,
    host_ExecutionSite_Initializing,
    host_ExecutionSite_FailedConstruction,
    host_ExecutionSite_GenericException,
    host_ExecutionSite_RuntimeException,
    host_ExecutionSite_UnexpectedProcedureException,
    host_ExecutionSite_FailedDeserializingParamsForFragmentTask,
    host_ExecutionSite_ExceptionExecutingPF,
    host_ExecutionSite_UnhandledVoltMessage,
    host_Backend_RunDDLFailed,
    host_Backend_ErrorOnShutdown,
    host_ClientInterface_checkForAdhocSQL_SerializationException,
    host_VoltDB_ErrorStartAcceptingConnections,
    host_VoltDB_ErrorStartHTTPListener,
    host_VoltDB_ServerCompletedInitialization,
    host_VoltDB_StartingNetwork,
    host_VoltDB_InternalProfilingDisabledOnMultipartitionHosts,
    host_VoltDB_ProfileLevelIs,
    host_VoltDB_InvalidHostCount,
    host_VoltDB_CouldNotRetrieveLeaderAddress,
    host_VoltDB_ExportInitFailure,
    host_VoltDB_CatalogReadFailure,
    host_VoltDB_StartupString,
    host_VoltDB_StayTunedForLogging,
    host_VoltDB_StayTunedForNoLogging,
    host_TheHashinator_ExceptionHashingString,
    host_TheHashinator_AttemptedToHashinateNonLongOrString,
    host_Initialiazion_InvalidDDL,

    //AUTH
    auth_ClientInterface_ProcedureNotFound,
    auth_ClientInterface_LackingPermissionForProcedure,
    auth_ClientInterface_LackingPermissionForSql,
    auth_ClientInterface_LackingPermissionForSysproc,
    auth_ClientInterface_LackingPermissionForDefaultproc,
    auth_AuthSystem_NoSuchAlgorithm,
    auth_AuthSystem_NoSuchUser,
    auth_AuthSystem_AuthFailedPasswordMistmatch,

    //SQL
    sql_Backend_DmlError,
    sql_Backend_ExecutingDML,
    sql_Backend_ConvertingHSQLExtoCFEx,

    //Benchmark
    benchmark_BenchmarkController_ProcessReturnedMalformedLine,
    benchmark_BenchmarkController_GotReadyMessage,
    benchmark_BenchmarkController_ReturnedErrorMessage,
    benchmark_BenchmarkController_ErrorDuringReflectionForClient,
    benchmark_BenchmarkController_UnableToInstantiateProjectBuilder,
    benchmark_BenchmarkController_UnableToRunRemoteKill,
    benchmark_BenchmarkController_NotEnoughClients,
    benchmark_BenchmarkController_NotEnoughHosts,

    //EXPORT
    export_ExportManager_NoLoaderExtensions,
    export_ExportManager_DataDroppedLoaderDead,

    //COMPILER
    compiler_VoltCompiler_LeaderAndHostCountAndSitesPerHost,
    compiler_VoltCompiler_CatalogPath,
    compiler_VoltCompiler_NoSuchAlgorithm,


    org_voltdb_ExecutionSite_ImportingDependency,
    org_voltdb_ExecutionSite_Watchdog_possibleHang,
    org_voltdb_ExecutionSite_ExpectedProcedureException,
    org_voltdb_ExecutionSite_GotDtxnWU,
    org_voltdb_ExecutionSite_StackFrameDrop,
    org_voltdb_ExecutionSite_SendingCompletedWUToDtxn,
    org_voltdb_ExecutionSite_SendingDependency,

    org_voltdb_VoltDB_CreatingThreadForSite,
    org_voltdb_VoltDB_CreatingLocalSite,
    org_voltdb_VoltDB_FailedToRetrieveBuildString,

    org_voltdb_dtxn_SimpleDtxnConnection_UnkownMessageClass,
    NOT_USED;
}