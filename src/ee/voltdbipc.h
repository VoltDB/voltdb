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

#ifndef VOLTDBIPC_H_
#define VOLTDBIPC_H_

#include <signal.h>
#include <vector>
#include "common/ids.h"
#include "logging/LogDefs.h"
#include "logging/LogProxy.h"
#include "common/FatalException.hpp"
#include "common/Topend.h"

namespace voltdb {
class Pool;
class StreamBlock;
class Table;
class VoltDBEngine;
}

class VoltDBIPC : public voltdb::Topend {
public:

    // must match ERRORCODE_SUCCESS|ERROR in ExecutionEngine.java
    enum {
        kErrorCode_None = -1, kErrorCode_Success = 0, kErrorCode_Error = 1,
        /*
         * The following are not error codes but requests for information or functionality
         * from Java. These do not exist in ExecutionEngine.java since they are IPC specific.
         * These constants are mirrored in ExecutionEngine.java.
         */
        kErrorCode_RetrieveDependency = 100,       // Request for dependency
        kErrorCode_DependencyFound = 101,          // Response to 100
        kErrorCode_DependencyNotFound = 102,       // Also response to 100
        kErrorCode_pushExportBuffer = 103,         // Indication that el buffer is next
        kErrorCode_CrashVoltDB = 104,              // Crash with reason string
        kErrorCode_getQueuedExportBytes = 105,     // Retrieve value for stats
        kErrorCode_needPlan = 110,                 // fetch a plan from java for a fragment
        kErrorCode_progressUpdate = 111,           // Update Java on execution progress
        kErrorCode_decodeBase64AndDecompress = 112 // Decode base64, compressed data
    };

    VoltDBIPC(int fd);

    ~VoltDBIPC();

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

    int64_t fragmentProgressUpdate(int32_t batchIndex, std::string planNodeName,
            std::string lastAccessedTable, int64_t lastAccessedTableSize, int64_t tuplesProcessed,
            int64_t currMemoryInBytes, int64_t peakMemoryInBytes);

    std::string decodeBase64AndDecompress(const std::string& base64Data);

    /**
     * Retrieve a plan from Java via the IPC connection for a fragment id.
     * Plan is JSON. Returns the empty string on failure, but failure is
     * probably going to be detected somewhere else.
     */
    std::string planForFragmentId(int64_t fragmentId);

    bool execute(struct ipc_command *cmd);

    void pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block);

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
    void pushExportBuffer(int64_t exportGeneration, int32_t partitionId, std::string signature, voltdb::StreamBlock *block, bool sync, bool endOfStream);
private:
    voltdb::VoltDBEngine *m_engine;
    long int m_counter;

    int8_t stub(struct ipc_command *cmd);

    int8_t loadCatalog(struct ipc_command *cmd);

    int8_t updateCatalog(struct ipc_command *cmd);

    int8_t initialize(struct ipc_command *cmd);

    int8_t toggleProfiler(struct ipc_command *cmd);

    int8_t releaseUndoToken(struct ipc_command *cmd);

    int8_t undoUndoToken(struct ipc_command *cmd);

    int8_t tick(struct ipc_command *cmd);

    int8_t quiesce(struct ipc_command *cmd);

    int8_t setLogLevels(struct ipc_command *cmd);

    void executePlanFragments(struct ipc_command *cmd);

    void getStats(struct ipc_command *cmd);

    int8_t loadTable(struct ipc_command *cmd);

    int8_t processRecoveryMessage( struct ipc_command *cmd);

    void tableHashCode( struct ipc_command *cmd);

    void hashinate(struct ipc_command* cmd);

    void updateHashinator(struct ipc_command *cmd);

    void threadLocalPoolAllocations();

    void executeTask(struct ipc_command*);

    void sendException( int8_t errorCode);

    int8_t activateTableStream(struct ipc_command *cmd);
    void tableStreamSerializeMore(struct ipc_command *cmd);
    void exportAction(struct ipc_command *cmd);
    void getUSOForExportTable(struct ipc_command *cmd);

    void signalHandler(int signum, siginfo_t *info, void *context);
    static void signalDispatcher(int signum, siginfo_t *info, void *context);
    void setupSigHandler(void) const;

    int m_fd;
    char *m_reusedResultBuffer;
    char *m_exceptionBuffer;
    bool m_terminate;

    // The tuple buffer gets expanded (doubled) as needed, but never compacted.
    char *m_tupleBuffer;
    size_t m_tupleBufferSize;
};

#endif /* VOLTDBIPC_H_ */
