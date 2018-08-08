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

/*
 Implement the Java ExecutionEngine interface using IPC to a standalone EE
 process. This allows the backend to run without a JVM - useful for many
 debugging tasks.  Represents a single EE in a single process. Accepts
 and executes commands from Java synchronously.
 */
#pragma once

#include "common/Topend.h"
#include "execution/VoltDBEngine.h"
#include "storage/table.h"
#include "logging/StdoutLogProxy.h"

// Please don't make this different from the JNI result buffer size.
// This determines the size of the EE results buffer and it's nice
// if IPC and JNI are matched.
#define MAX_MSG_SZ (1024*1024*10)

namespace voltdb {
   class Pool;
   class StreamBlock;
   class Table;
   struct ipc_command;

   class VoltDBIPC : public voltdb::Topend {
      public:
         // must match ERRORCODE_SUCCESS|ERROR in ExecutionEngine.java
         enum {
            kErrorCode_None = -1, // not in the java
            kErrorCode_Success = 0,
            kErrorCode_Error = 1,
            /*
             * The following are not error codes but requests for information or functionality
             * from Java. These do not exist in ExecutionEngine.java since they are IPC specific.
             * These constants are mirrored in ExecutionEngine.java.
             */
            kErrorCode_RetrieveDependency = 100,           // Request for dependency
            kErrorCode_DependencyFound = 101,              // Response to 100
            kErrorCode_DependencyNotFound = 102,           // Also response to 100
            kErrorCode_pushExportBuffer = 103,             // Indication that export buffer is next
            kErrorCode_CrashVoltDB = 104,                  // Crash with reason string
            kErrorCode_getQueuedExportBytes = 105,         // Retrieve value for stats
            kErrorCode_pushPerFragmentStatsBuffer = 106,   // Indication that per-fragment statistics buffer is next
            kErrorCode_callJavaUserDefinedFunction = 107,  // Notify the frontend to call a Java user-defined function.
            kErrorCode_needPlan = 110,                     // fetch a plan from java for a fragment
            kErrorCode_progressUpdate = 111,               // Update Java on execution progress
            kErrorCode_decodeBase64AndDecompress = 112,    // Decode base64, compressed data
            kErrorCode_pushEndOfStream = 113               // Push EOF for dropped stream.
         };

         VoltDBIPC(int fd);

         ~VoltDBIPC();

         const voltdb::VoltDBEngine* getEngine() const {
            return m_engine.get();
         }

         int loadNextDependency(int32_t dependencyId, voltdb::Pool *stringPool, voltdb::Table* destination);
         void fallbackToEEAllocatedBuffer(char *buffer, size_t length) { }

         /**
          * Retrieve a dependency from Java via the IPC connection.
          * This method returns null if there are no more dependency tables. Otherwise
          * it returns a pointer to a buffer containing the dependency. The first four bytes
          * of the buffer is an int32_t length prefix.
          *
          * The returned allocated memory must be freed by the caller.
          * Returns dependency size with out parameter.
          */
         char *retrieveDependency(int32_t dependencyId, size_t *dependencySz);

         int64_t fragmentProgressUpdate(
               int32_t batchIndex,
               voltdb::PlanNodeType planNodeType,
               int64_t tuplesProcessed,
               int64_t currMemoryInBytes,
               int64_t peakMemoryInBytes);

         std::string decodeBase64AndDecompress(const std::string& base64Data);

         /**
          * Retrieve a plan from Java via the IPC connection for a fragment id.
          * Plan is JSON. Returns the empty string on failure, but failure is
          * probably going to be detected somewhere else.
          */
         std::string planForFragmentId(int64_t fragmentId);

         bool execute(ipc_command *cmd);

         int64_t pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block);

         void pushPoisonPill(int32_t partitionId, std::string& reason, voltdb::StreamBlock *block);

         /**
          * Log a statement on behalf of the IPC log proxy at the specified log level
          * @param LoggerId ID of the logger that received this statement
          * @param level Log level of the statement
          * @param statement null terminated UTF-8 string containing the statement to log
          */
         void log(voltdb::LoggerId loggerId, voltdb::LogLevel level, const char *statement) const;

         void crashVoltDB(voltdb::FatalException e);

         /*
          * Cause the engine to terminate gracefully after finishing execution of the current command.
          * Useful when running Valgrind because you can terminate at the point where you think memory has leaked
          * and this method will make sure that the VoltDBEngine is deleted and that the program will attempt
          * to free all memory allocated on the heap.
          */
         void terminate();

         int64_t getQueuedExportBytes(int32_t partitionId, std::string signature);
         void pushExportBuffer(int32_t partitionId, std::string signature, voltdb::StreamBlock *block, bool sync);
         void pushEndOfStream(int32_t partitionId, std::string signature);

         int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, voltdb::DRRecordType action,
               voltdb::DRConflictType deleteConflict, voltdb::Table *existingMetaTableForDelete, voltdb::Table *existingTupleTableForDelete,
               voltdb::Table *expectedMetaTableForDelete, voltdb::Table *expectedTupleTableForDelete,
               voltdb::DRConflictType insertConflict, voltdb::Table *existingMetaTableForInsert, voltdb::Table *existingTupleTableForInsert,
               voltdb::Table *newMetaTableForInsert, voltdb::Table *newTupleTableForInsert);

         bool storeLargeTempTableBlock(voltdb::LargeTempTableBlock* block);

         bool loadLargeTempTableBlock(voltdb::LargeTempTableBlock* block);

         bool releaseLargeTempTableBlock(voltdb::LargeTempTableBlockId blockId);


      private:

         int8_t stub(ipc_command *cmd);

         int8_t loadCatalog(ipc_command *cmd);

         int8_t updateCatalog(ipc_command *cmd);

         int8_t initialize(ipc_command *cmd);

         int8_t toggleProfiler(ipc_command *cmd);

         int8_t releaseUndoToken(ipc_command *cmd);

         int8_t undoUndoToken(ipc_command *cmd);

         int8_t tick(ipc_command *cmd);

         int8_t quiesce(ipc_command *cmd);

         int8_t shutDown();

         int8_t setLogLevels(ipc_command *cmd);

         void executePlanFragments(ipc_command *cmd);

         void getStats(ipc_command *cmd);

         int8_t loadTable(ipc_command *cmd);

         int8_t processRecoveryMessage( ipc_command *cmd);

         void tableHashCode( ipc_command *cmd);

         void hashinate(ipc_command* cmd);

         void updateHashinator(ipc_command *cmd);

         void threadLocalPoolAllocations();

         void applyBinaryLog(ipc_command*);

         void executeTask(ipc_command*);

         void sendPerFragmentStatsBuffer();

         int callJavaUserDefinedFunction();

         void setViewsEnabled(ipc_command*);

         // We do not adjust the UDF buffer size in the IPC mode.
         // The buffer sizes are always MAX_MSG_SZ (10M)
         void resizeUDFBuffer(int32_t size) {
            return;
         }

         void sendException( int8_t errorCode);

         int8_t activateTableStream(ipc_command *cmd);
         void tableStreamSerializeMore(ipc_command *cmd);
         void exportAction(ipc_command *cmd);
         void getUSOForExportTable(ipc_command *cmd);

         void signalHandler(int signum, siginfo_t *info, void *context);
         static void signalDispatcher(int signum, siginfo_t *info, void *context);
         void setupSigHandler(void) const;

         std::unique_ptr<voltdb::VoltDBEngine> m_engine{};
         long int m_counter = 0;

         int m_fd;
         std::unique_ptr<char[]> m_perFragmentStatsBuffer{new char[MAX_MSG_SZ]},
            m_reusedResultBuffer{new char[MAX_MSG_SZ]},
            m_exceptionBuffer{new char[MAX_MSG_SZ]},
            m_udfBuffer{new char[MAX_MSG_SZ]},
            m_tupleBuffer{new char[MAX_MSG_SZ]};
         bool m_terminate = false;

         // The tuple buffer gets expanded (doubled) as needed, but never compacted.
         size_t m_tupleBufferSize = 0;
         StdoutLogProxy m_logProxy{};
   };

   /* java sends all data with this header */
   struct ipc_command {
      int32_t msgsize;
      int32_t command;
      char data[0];
   }__attribute__((packed));

   /*
    * Structure describing an executePlanFragments message header.
    */
   struct querypfs {
      ipc_command cmd;
      int64_t txnId;
      int64_t spHandle;
      int64_t lastCommittedSpHandle;
      int64_t uniqueId;
      int64_t undoToken;
      int8_t perFragmentTimingEnabled;
      int32_t numFragmentIds;
      char data[0];
   }__attribute__((packed));

   struct loadfrag {
      ipc_command cmd;
      int32_t planFragLength;
      char data[0];
   }__attribute__((packed));

   /*
    * Header for a load table request.
    */
   struct load_table_cmd {
      ipc_command cmd;
      int32_t tableId;
      int64_t txnId;
      int64_t spHandle;
      int64_t lastCommittedSpHandle;
      int64_t uniqueId;
      int64_t undoToken;
      int32_t returnUniqueViolations;
      int32_t shouldDRStream;
      char data[0];
   }__attribute__((packed));

   /*
    * Header for a stats table request.
    */
   struct get_stats_cmd {
      ipc_command cmd;
      int32_t selector;
      int8_t  interval;
      int64_t now;
      int32_t num_locators;
      int32_t locators[0];
   }__attribute__((packed));

   struct undo_token {
      ipc_command cmd;
      int64_t token;
      char isEmptyDRTxn;
   }__attribute__((packed));

   /*
    * Header for a ActivateCopyOnWrite request
    */
   struct activate_tablestream {
      ipc_command cmd;
      voltdb::CatalogId tableId;
      voltdb::TableStreamType streamType;
      int64_t undoToken;
      char data[0];
   }__attribute__((packed));

   /*
    * Header for a Copy On Write Serialize More request
    */
   struct tablestream_serialize_more {
      ipc_command cmd;
      voltdb::CatalogId tableId;
      voltdb::TableStreamType streamType;
      int bufferCount;
      char data[0];
   }__attribute__((packed));

   /*
    * Header for an incoming recovery message
    */
   struct recovery_message {
      ipc_command cmd;
      int32_t messageLength;
      char message[0];
   }__attribute__((packed));

   /*
    * Header for a request for a table hash code
    */
   struct table_hash_code {
      ipc_command cmd;
      int32_t tableId;
   }__attribute__((packed));

   struct hashinate_msg {
      ipc_command cmd;
      int32_t configLength;
      char data[0];
   }__attribute__((packed));

   /*
    * Header for an Export action.
    */
   struct export_action {
      ipc_command cmd;
      int32_t isSync;
      int64_t offset;
      int64_t seqNo;
      int32_t tableSignatureLength;
      char tableSignature[0];
   }__attribute__((packed));

   struct get_uso {
      ipc_command cmd;
      int32_t tableSignatureLength;
      char tableSignature[0];
   }__attribute__((packed));

   struct catalog_load {
      ipc_command cmd;
      int64_t timestamp;
      char data[0];
   }__attribute__((packed));

   struct execute_task {
      ipc_command cmd;
      int64_t taskId;
      char task[0];
   }__attribute__((packed));

   struct apply_binary_log {
      ipc_command cmd;
      int64_t txnId;
      int64_t spHandle;
      int64_t lastCommittedSpHandle;
      int64_t uniqueId;
      int32_t remoteClusterId;
      int32_t remotePartitionId;
      int64_t undoToken;
      char log[0];
   }__attribute__((packed));

   struct update_catalog_cmd {
      ipc_command cmd;
      int64_t timestamp;
      int32_t isStreamChange;
      char data[0];
   }__attribute__((packed));

   struct set_views_enabled {
      ipc_command cmd;
      char enabled;
      char viewNameBytes[0];
   }__attribute__((packed));

   /**
    * Utility used for deserializing ParameterSet passed from Java.
    */
   void deserializeParameterSetCommon(int cnt, ReferenceSerializeInputBE &serialize_in,
         NValueArray &params, Pool *stringPool);

   void *eethread(void *ptr);
   void checkBytesRead(ssize_t byteCountExpected, ssize_t byteCountRead, std::string description);
}
