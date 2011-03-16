/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef VOLTDBIPC_H_
#define VOLTDBIPC_H_

#include <signal.h>
#include <vector>
#include "common/ids.h"
#include "logging/LogDefs.h"
#include "logging/LogProxy.h"
#include "execution/VoltDBEngine.h"
#include "common/FatalException.hpp"
#include "storage/StreamBlock.h"

class VoltDBIPC {
public:

    // must match ERRORCODE_SUCCESS|ERROR in ExecutionEngine.java
    enum {
        kErrorCode_None = -1, kErrorCode_Success = 0, kErrorCode_Error = 1,
        /*
         * The following are not error codes but requests for information or functionality
         * from Java. These do not exist in ExecutionEngine.java since they are IPC specific.
         * These constants are mirrored in ExecutionEngine.java.
         */
        kErrorCode_RetrieveDependency = 100, //Request for dependency
        kErrorCode_DependencyFound = 101,    //Response to 100
        kErrorCode_DependencyNotFound = 102, //Also response to 100
        kErrorCode_pushExportBuffer = 103, //Indication that el buffer is next
        kErrorCode_CrashVoltDB = 104, //Crash with reason string
        kErrorCode_getQueuedExportBytes = 105 //Retrieve value for stats
    };

    VoltDBIPC(int fd);

    ~VoltDBIPC();

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

    bool execute(struct ipc_command *cmd);

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

    void executeQueryPlanFragmentsAndGetResults(struct ipc_command *cmd);

    void executePlanFragmentAndGetResults(struct ipc_command *cmd);

    void executeCustomPlanFragmentAndGetResults(struct ipc_command *cmd);

    void getStats(struct ipc_command *cmd);

    int8_t loadTable(struct ipc_command *cmd);

    int8_t processRecoveryMessage( struct ipc_command *cmd);

    void tableHashCode( struct ipc_command *cmd);

    void hashinate(struct ipc_command* cmd);

    void threadLocalPoolAllocations();

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
};

#endif /* VOLTDBIPC_H_ */
