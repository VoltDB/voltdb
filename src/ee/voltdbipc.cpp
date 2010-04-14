/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

/*
 Implement the Java ExecutionEngine interface using IPC to a standalone EE
 process. This allows the backend to run without a JVM - useful for many
 debugging tasks.  Represents a single EE in a single process. Accepts
 and executes commands from Java synchronously.
 */

#include "voltdbipc.h"
#include "logging/StdoutLogProxy.h"

#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/Pool.hpp"
#include "common/FatalException.hpp"
#include "execution/IPCTopend.h"
#include "execution/VoltDBEngine.h"

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <dlfcn.h>

#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>


// Please don't make this different from the JNI result buffer size.
// This determines the size of the EE results buffer and it's nice
// if IPC and JNI are matched.
#define MAX_MSG_SZ (1024*1024*10)

using namespace std;

/* java sends all data with this header */
struct ipc_command {
    int32_t msgsize;
    int32_t command;
    char data[0];
}__attribute__((packed));

/*
 * Structure describing an executeQueryPlanFragment message header.
 */
typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    int64_t lastCommittedTxnId;
    int64_t undoToken;
    int32_t numFragmentIds;
    int32_t numParameterSets;
    char data[0];
}__attribute__((packed)) querypfs;

/*
 * Header of an execute plan fragment request. Contains the single fragmentId followed by the parameter set.
 */
typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    int64_t lastCommittedTxnId;
    int64_t undoToken;
    int64_t fragmentId;
    int32_t outputDepId;
    int32_t inputDepId;
    char data[0];
}__attribute__((packed)) planfrag;

/*
 * Header of an execute custom plan fragment request. Contains no fragmentId, just the custom plan string.
 */
typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    int64_t lastCommittedTxnId;
    int64_t undoToken;
    int32_t outputDepId;
    int32_t inputDepId;
    int32_t length;
    char data[0];
}__attribute__((packed)) customplanfrag;

/*
 * Header for a load table request.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t tableId;
    int64_t txnId;
    int64_t lastCommittedTxnId;
    int64_t undoToken;
    int16_t  allowELT;
    char data[0];
}__attribute__((packed)) load_table_cmd;

/*
 * Header for a stats table request.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t selector;
    int8_t  interval;
    int64_t now;
    int32_t num_locators;
    int32_t locators[0];
}__attribute__((packed)) get_stats_cmd;

/*
 * Header for a saveTableToDisk request
 */
typedef struct {
    struct ipc_command cmd;
    int32_t clusterId;
    int32_t databaseId;
    int32_t tableId;
    char data[0];
}__attribute__((packed)) save_table_to_disk_cmd;

struct undo_token {
    struct ipc_command cmd;
    int64_t token;
}__attribute__((packed));

/*
 * Header for a ActivateCopyOnWrite request
 */
typedef struct {
    struct ipc_command cmd;
    voltdb::CatalogId tableId;
}__attribute__((packed)) activate_copy_on_write;

/*
 * Header for a Copy On Write Serialize More request
 */
typedef struct {
    struct ipc_command cmd;
    voltdb::CatalogId tableId;
    int bufferSize;
}__attribute__((packed)) cow_serialize_more;

using namespace voltdb;

/*
 * This is used by the signal dispatcher
 */
static VoltDBIPC *currentVolt = NULL;

// defined in voltdbjni.cpp
extern void deserializeParameterSetCommon(int, voltdb::ReferenceSerializeInput&, voltdb::GenericValueArray<voltdb::NValue>&, Pool *stringPool);

VoltDBIPC::VoltDBIPC(int fd) : m_fd(fd) {
    currentVolt = this;
    m_engine = NULL;
    m_counter = 0;
    m_reusedResultBuffer = NULL;
    m_terminate = false;

    setupSigHandler();
}

VoltDBIPC::~VoltDBIPC() {
    delete m_engine;
    delete [] m_reusedResultBuffer;
    delete [] m_exceptionBuffer;
}

bool VoltDBIPC::execute(struct ipc_command *cmd) {
    int8_t result = kErrorCode_None;

    if (0)
        std::cout << "IPC client command: " << ntohl(cmd->command) << std::endl;

    // commands must match java's ExecutionEngineIPC.Command
    // could enumerate but they're only used in this one place.
    switch (ntohl(cmd->command)) {
      case 0:
        result = initialize(cmd);
        break;
      case 2:
        result = loadCatalog(cmd);
        break;
      case 3:
        result = toggleProfiler(cmd);
        break;
      case 4:
        result = tick(cmd);
        break;
      case 5:
        getStats(cmd);
        break;
      case 6:
        // also writes results directly
        executeQueryPlanFragmentsAndGetResults(cmd);
        result = kErrorCode_None;
        break;
      case 7:
        // also writes results (if any) directly
        executePlanFragmentAndGetResults(cmd);
        result = kErrorCode_None;
        break;
      case 9:
        result = loadTable(cmd);
        break;
      case 10:
        result = releaseUndoToken(cmd);
        break;
      case 11:
        result = undoUndoToken(cmd);
        break;
      case 12:
        executeCustomPlanFragmentAndGetResults(cmd);
        result = kErrorCode_None;
        break;
      case 13:
        result = setLogLevels(cmd);
        break;
      case 16:
        result = quiesce(cmd);
        break;
      case 17:
        result = activateCopyOnWrite(cmd);
        break;
      case 18:
        cowSerializeMore(cmd);
        result = kErrorCode_None;
        break;
      case 19:
        result = updateCatalog(cmd);
        break;
      default:
        result = stub(cmd);
    }

    // write results for the simple commands. more
    // complex commands write directly in the command
    // implementation.
    if (result != kErrorCode_None) {
        ssize_t bytes = 0;
        if (result == kErrorCode_Error) {
            char msg[3];
            msg[0] = result;
            *reinterpret_cast<int16_t*>(&msg[1]) = 0;//exception length 0
            bytes = write(m_fd, msg, sizeof(int8_t) + sizeof(int16_t));
        } else {
            bytes = write(m_fd, &result, sizeof(int8_t));
        }
    }
    return m_terminate;
}

int8_t VoltDBIPC::stub(struct ipc_command *cmd) {
    printf("IPC command %d not implemented.\n", ntohl(cmd->command));
    fflush(stdout);
    return kErrorCode_Error;
}

int8_t VoltDBIPC::loadCatalog(struct ipc_command *cmd) {
    printf("loadCatalog\n");
    assert(m_engine);
    if (!m_engine)
        return kErrorCode_Error;

    try {
        if (m_engine->loadCatalog(std::string(cmd->data)) == true) {
            return kErrorCode_Success;
        }
    } catch (SerializableEEException &e) {}

    return kErrorCode_Error;
}

int8_t VoltDBIPC::updateCatalog(struct ipc_command *cmd) {
    printf("updateCatalog\n");
    assert(m_engine);
    if (!m_engine)
        return kErrorCode_Error;

    try {
        if (m_engine->updateCatalog(std::string(cmd->data)) == true) {
            return kErrorCode_Success;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

int8_t VoltDBIPC::initialize(struct ipc_command *cmd) {
    // expect a single initialization.
    assert(!m_engine);
    delete m_engine;

    // voltdbengine::initialize expects catalogids.
    assert(sizeof(CatalogId) == sizeof(int));

    struct initialize {
        struct ipc_command cmd;
        int clusterId;
        int siteId;
        int partitionId;
        int hostId;
        int64_t logLevels;
        int16_t hostnameLength;
        char hostname[0];
    };
    struct initialize * cs = (struct initialize*) cmd;

    printf("initialize: cluster=%d, site=%d\n",
           ntohl(cs->clusterId), ntohl(cs->siteId));
    cs->clusterId = ntohl(cs->clusterId);
    cs->siteId = ntohl(cs->siteId);
    cs->partitionId = ntohl(cs->partitionId);
    cs->hostId = ntohl(cs->hostId);
    cs->hostnameLength = ntohs(cs->hostnameLength);
    std::string hostname(cs->hostname, cs->hostnameLength);
    try {
        m_engine = new VoltDBEngine(new voltdb::IPCTopend(this), new voltdb::StdoutLogProxy());
        m_engine->getLogManager()->setLogLevels(cs->logLevels);
        m_reusedResultBuffer = new char[MAX_MSG_SZ];
        m_exceptionBuffer = new char[MAX_MSG_SZ];
        m_engine->setBuffers( NULL, 0, m_reusedResultBuffer, MAX_MSG_SZ, m_exceptionBuffer, MAX_MSG_SZ);
        if (m_engine->initialize( cs->clusterId, cs->siteId, cs->partitionId, cs->hostId, hostname) == true) {
            return kErrorCode_Success;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

int8_t VoltDBIPC::toggleProfiler(struct ipc_command *cmd) {
    assert(m_engine);
    if (!m_engine)
        return kErrorCode_Error;

    struct toggle {
        struct ipc_command cmd;
        int toggle;
    }__attribute__((packed));
    struct toggle * cs = (struct toggle*) cmd;

    printf("toggleProfiler: toggle=%d\n", ntohl(cs->toggle));

    // actually, the engine doesn't implement this now.
    // m_engine->ProfilerStart();
    return kErrorCode_Success;
}

int8_t VoltDBIPC::releaseUndoToken(struct ipc_command *cmd) {
    assert(m_engine);
    if (!m_engine)
        return kErrorCode_Error;


    struct undo_token * cs = (struct undo_token*) cmd;

    try {
        m_engine->releaseUndoToken(ntohll(cs->token));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

int8_t VoltDBIPC::undoUndoToken(struct ipc_command *cmd) {
    assert(m_engine);
    if (!m_engine)
        return kErrorCode_Error;


    struct undo_token * cs = (struct undo_token*) cmd;

    try {
        m_engine->undoUndoToken(ntohll(cs->token));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

int8_t VoltDBIPC::tick(struct ipc_command *cmd) {
    assert (m_engine);
    if (!m_engine)
        return kErrorCode_Error;

    struct tick {
        struct ipc_command cmd;
        int64_t time;
        int64_t lastTxnId;
    }__attribute__((packed));

    struct tick * cs = (struct tick*) cmd;
    //std::cout << "tick: time=" << cs->time << " txn=" << cs->lastTxnId << std::endl;

    try {
        // no return code. can't fail!
        m_engine->tick(ntohll(cs->time), ntohll(cs->lastTxnId));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}

int8_t VoltDBIPC::quiesce(struct ipc_command *cmd) {
    struct quiesce {
        struct ipc_command cmd;
        int64_t lastTxnId;
    }__attribute__((packed));

    struct quiesce *cs = (struct quiesce*)cmd;

    try {
        m_engine->quiesce(ntohll(cs->lastTxnId));
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    return kErrorCode_Success;
}


void VoltDBIPC::executeQueryPlanFragmentsAndGetResults(struct ipc_command *cmd) {
    int errors = 0;
    NValueArray &params = m_engine->getParameterContainer();

    querypfs *queryCommand = (querypfs*) cmd;

    if (0)
        std::cout << "querypfs:" << " txnId=" << ntohll(queryCommand->txnId)
                  << " lastCommitted=" << ntohll(queryCommand->lastCommittedTxnId)
                  << " numFragIds=" << ntohl(queryCommand->numFragmentIds)
                  << " numParamSets=" << ntohl(queryCommand->numParameterSets) << std::endl;

    // data has binary packed fragmentIds first
    int64_t *fragmentId = (int64_t*) (&(queryCommand->data));

    // ...and fast serialized parameter sets last.
    void* offset = queryCommand->data + (sizeof(int64_t) * ntohl(queryCommand->numFragmentIds));
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(querypfs) - sizeof(int32_t) * ntohl(queryCommand->numFragmentIds));
    ReferenceSerializeInput serialize_in(offset, sz);

    try {
        // and reset to space for the results output
        m_engine->resetReusedResultOutputBuffer(1);//1 byte to add status code
        m_engine->setUndoToken(ntohll(queryCommand->undoToken));
        int numFrags = ntohl(queryCommand->numFragmentIds);
        for (int i = 0; i < numFrags; ++i) {
            int cnt = serialize_in.readShort();
            assert(cnt> -1);
            Pool *pool = m_engine->getStringPool();
            deserializeParameterSetCommon(cnt, serialize_in, params, pool);
            m_engine->setUsedParamcnt(cnt);
            if (m_engine->executeQuery(ntohll(fragmentId[i]), 1, -1,
                                       params, ntohll(queryCommand->txnId),
                                       ntohll(queryCommand->lastCommittedTxnId),
                                       i == 0 ? true : false, //first
                                       i == numFrags - 1 ? true : false)) { //last
                ++errors;
            }
            pool->purge();
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    // write the results array back across the wire
    if (errors == 0) {
        // write the results array back across the wire
        const int32_t size = m_engine->getResultsSize();
        char *resultBuffer = m_engine->getReusedResultBuffer();
        resultBuffer[0] = kErrorCode_Success;
        ssize_t bytes = write(m_fd, resultBuffer, size);
        if (bytes != size) {
            printf("Error - blocking write failed. %ld", bytes);
            assert(false);
            exit(-1);
        }
    } else {
        sendException(kErrorCode_Error);
    }
}

void VoltDBIPC::executePlanFragmentAndGetResults(struct ipc_command *cmd) {
    int errors = 0;
    NValueArray &params = m_engine->getParameterContainer();

    planfrag *planfragCommand = (planfrag*) cmd;

    if (0)
        std::cout << "planfrag:" << " txnId=" << ntohll(planfragCommand->txnId)
                  << " lastCommitted=" << ntohll(planfragCommand->lastCommittedTxnId)
                  << " fragmentId=" << ntohll(planfragCommand->fragmentId) << std::endl;

    // data has binary packed fragmentIds/deps first
    int64_t fragmentId = ntohll(planfragCommand->fragmentId);
    int32_t outputDepId = ntohl(planfragCommand->outputDepId);
    int32_t inputDepId = ntohl(planfragCommand->inputDepId);

    // ...and fast serialized parameter set last.
    void* offset = planfragCommand->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(planfrag));
    ReferenceSerializeInput serialize_in(offset, sz);

    try {
        // and reset to space for the results output
        m_engine->resetReusedResultOutputBuffer(1);

        int cnt = serialize_in.readShort();
        assert(cnt> -1);
        Pool *pool = m_engine->getStringPool();
        deserializeParameterSetCommon(cnt, serialize_in, params, pool);
        m_engine->setUsedParamcnt(cnt);
        m_engine->setUndoToken(ntohll(planfragCommand->undoToken));
        if (m_engine->executeQuery(fragmentId, outputDepId, inputDepId, params,
                                   ntohll(planfragCommand->txnId),
                                   ntohll(planfragCommand->lastCommittedTxnId),
                                   true, true)) {
    //        assert(!"Do not expect errors executing Query");
            ++errors;
        }
        pool->purge();
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    // write the results array back across the wire
    if (errors == 0) {
        // write the dependency tables back across the wire
        // the result set includes the total serialization size
        const int32_t size = m_engine->getResultsSize();
        char *resultBuffer = m_engine->getReusedResultBuffer();
        resultBuffer[0] = kErrorCode_Success;
        ssize_t bytes = write(m_fd, resultBuffer, size);
        if (bytes != size) {
            printf("Error - blocking write failed. %ld", bytes);
            assert(false);
            exit(-1);
        }
    } else {
        sendException(kErrorCode_Error);
    }
}

void VoltDBIPC::sendException(int8_t errorCode) {
    ssize_t bytes = write(m_fd, &errorCode, sizeof(int8_t));
    if (bytes != sizeof(int8_t)) {
        printf("Error - blocking write failed. %ld", (uintmax_t)bytes);
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    const void* exceptionData = m_engine->getExceptionOutputSerializer()->data();
    int16_t exceptionLength = static_cast<int16_t>(ntohs(*reinterpret_cast<const int16_t*>(exceptionData)));
    printf("Sending exception length %d\n", exceptionLength);
    fflush(stdout);

    const std::size_t expectedSize = exceptionLength + sizeof(int16_t);
    bytes = write(m_fd, exceptionData, expectedSize);


    if (bytes != expectedSize) {
        printf("Error - blocking write failed. %jd written %jd attempted", (intmax_t)bytes, (intmax_t)expectedSize);
        fflush(stdout);
        assert(false);
        exit(-1);
    }
}

void VoltDBIPC::executeCustomPlanFragmentAndGetResults(struct ipc_command *cmd) {
    int errors = 0;

    customplanfrag *plan = (customplanfrag*)cmd;

    // setup
    m_engine->resetReusedResultOutputBuffer();
    m_engine->setUsedParamcnt(0);
    m_engine->setUndoToken(ntohll(plan->undoToken));

    // data as fast serialized string
    int32_t len = ntohl(plan->length);
    string plan_str = string(plan->data, len);

    // deps info
    int32_t outputDepId = ntohl(plan->outputDepId);
    int32_t inputDepId = ntohl(plan->inputDepId);

    // execute
    if (m_engine->executePlanFragment(plan_str, outputDepId, inputDepId,
                                      ntohll(plan->txnId),
                                      ntohll(plan->lastCommittedTxnId))) {
        ++errors;
    }

    // write the results array back across the wire
    const int8_t successResult = kErrorCode_Success;
    if (errors == 0) {
        size_t bytes;
        bytes = write(m_fd, &successResult, sizeof(int8_t));
        assert(bytes == sizeof(int8_t));
        const int32_t size = m_engine->getResultsSize();
        // write the dependency tables back across the wire
        bytes = write(m_fd, m_engine->getReusedResultBuffer(), size);
        assert(bytes == size);
    } else {
        sendException(kErrorCode_Error);
    }
}

int8_t VoltDBIPC::loadTable(struct ipc_command *cmd) {
    load_table_cmd *loadTableCommand = (load_table_cmd*) cmd;

    if (0) {
        std::cout << "loadTable:" << " tableId=" << ntohl(loadTableCommand->tableId)
                  << " txnId=" << ntohll(loadTableCommand->txnId) << " lastCommitted="
                  << ntohll(loadTableCommand->lastCommittedTxnId) << std::endl;
    }

    const int32_t tableId = ntohl(loadTableCommand->tableId);
    const int64_t txnId = ntohll(loadTableCommand->txnId);
    const int64_t lastCommittedTxnId = ntohll(loadTableCommand->lastCommittedTxnId);
    const int64_t undoToken = ntohll(loadTableCommand->undoToken);
    const bool    allowELT = (loadTableCommand->allowELT != 0);
    // ...and fast serialized table last.
    void* offset = loadTableCommand->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(load_table_cmd));
    try {
        ReferenceSerializeInput serialize_in(offset, sz);

        m_engine->setUndoToken(undoToken);
        bool success = m_engine->loadTable(allowELT, tableId, serialize_in, txnId, lastCommittedTxnId);
        if (success) {
            return kErrorCode_Success;
        } else {
            return kErrorCode_Error;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

int8_t VoltDBIPC::setLogLevels(struct ipc_command *cmd) {
    int64_t logLevels = *((int64_t*)&cmd->data[0]);
    try {
        m_engine->getLogManager()->setLogLevels(logLevels);
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Success;
}

void VoltDBIPC::terminate() {
    m_terminate = true;
}

/**
 * Retrieve a dependency from Java via the IPC connection.
 * This method returns null if there are no more dependency tables. Otherwise
 * it returns a pointer to a buffer containing the dependency. The first four bytes
 * of the buffer is an int32_t length prefix.
 *
 * The returned allocated memory must be freed by the caller.
 */
char *VoltDBIPC::retrieveDependency(int32_t dependencyId, size_t *dependencySz) {
    ssize_t bytes;
    char message[5];
    *dependencySz = 0;

    // tell java to send the dependency over the socket
    message[0] = static_cast<int8_t>(kErrorCode_RetrieveDependency);
    *reinterpret_cast<int32_t*>(&message[1]) = htonl(dependencyId);
    bytes = write(m_fd, message, sizeof(int8_t) + sizeof(int32_t));
    if (bytes != sizeof(int8_t) + sizeof(int32_t)) {
        printf("Error - blocking write failed. %jd written %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int8_t) + sizeof(int32_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    // read java's response code
    int8_t responseCode;
    bytes = read(m_fd, &responseCode, sizeof(int8_t));
    if (bytes != sizeof(int8_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int8_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    // deal with error response codes
    if (kErrorCode_DependencyNotFound == responseCode) {
        return NULL;
    } else if (kErrorCode_DependencyFound != responseCode) {
        printf("Received unexpected response code %d to retrieve dependency request\n",
                (int)responseCode);
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    // start reading the dependency. its length is first
    int32_t dependencyLength;
    bytes = read(m_fd, &dependencyLength, sizeof(int32_t));
    if (bytes != sizeof(int32_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int32_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    bytes = 0;
    dependencyLength = ntohl(dependencyLength);
    *dependencySz = (size_t)dependencyLength;
    char *dependencyData = new char[dependencyLength];
    while (bytes != dependencyLength) {
        ssize_t oldBytes = bytes;
        bytes += read(m_fd, dependencyData + bytes, dependencyLength - bytes);
        if (oldBytes == bytes) {
            break;
        }
        if (oldBytes > bytes) {
            bytes++;
            break;
        }
    }

    if (bytes != dependencyLength) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)dependencyLength);
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    return dependencyData;
}

void VoltDBIPC::crashVoltDB(voltdb::FatalException e) {
    const char *reasonBytes = e.m_reason.c_str();
    int32_t reasonLength = static_cast<int32_t>(strlen(reasonBytes));
    int32_t lineno = static_cast<int32_t>(e.m_lineno);
    int32_t filenameLength = static_cast<int32_t>(strlen(e.m_filename));
    int32_t numTraces = static_cast<int32_t>(e.m_traces.size());
    int32_t totalTracesLength = 0;
    for (int ii = 0; ii < static_cast<int>(e.m_traces.size()); ii++) {
        totalTracesLength += static_cast<int32_t>(strlen(e.m_traces[ii].c_str()));
    }
    //sizeof traces text + length prefix per trace, length prefix of reason string, number of traces count,
    //filename length, lineno
    int32_t messageLength =
            static_cast<int32_t>(
                    totalTracesLength +
                    (sizeof(int32_t) * numTraces) +
                    (sizeof(int32_t) * 4) +
                    reasonLength +
                    filenameLength);

    //status code
    m_reusedResultBuffer[0] = static_cast<char>(kErrorCode_CrashVoltDB);
    size_t position = 1;

    //overall message length, not included in messageLength
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[position]) = htonl(messageLength);
    position += sizeof(int32_t);

    //reason string
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[position]) = htonl(reasonLength);
    position += sizeof(int32_t);
    memcpy( &m_reusedResultBuffer[position], reasonBytes, reasonLength);
    position += reasonLength;

    //filename string
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[position]) = htonl(filenameLength);
    position += sizeof(int32_t);
    memcpy( &m_reusedResultBuffer[position], e.m_filename, filenameLength);
    position += filenameLength;

    //lineno
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[position]) = htonl(lineno);
    position += sizeof(int32_t);

    //number of traces
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[position]) = htonl(numTraces);
    position += sizeof(int32_t);

    for (int ii = 0; ii < static_cast<int>(e.m_traces.size()); ii++) {
        int32_t traceLength = static_cast<int32_t>(strlen(e.m_traces[ii].c_str()));
        *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[position]) = htonl(traceLength);
        position += sizeof(int32_t);
        memcpy( &m_reusedResultBuffer[position], e.m_traces[ii].c_str(), traceLength);
        position += traceLength;
    }

    ssize_t toWrite = 5 + messageLength;
    ssize_t written = 0;
    while (written < toWrite) {
        ssize_t bytes = write(m_fd,  m_reusedResultBuffer + written, toWrite - written);
        if (bytes == -1) {
            printf("Error - blocking write failed. %jd read %jd attempted",
                    (intmax_t)bytes, (intmax_t)(toWrite - written));
            fflush(stdout);
            assert(false);
            exit(-1);
        }
    }
    exit(-1);
}

void VoltDBIPC::getStats(struct ipc_command *cmd) {
    get_stats_cmd *getStatsCommand = (get_stats_cmd*) cmd;

    const int32_t selector = ntohl(getStatsCommand->selector);
    const int32_t numLocators = ntohl(getStatsCommand->num_locators);
    bool interval = false;
    if (getStatsCommand->interval != 0) {
        interval = true;
    }
    const int64_t now = ntohll(getStatsCommand->now);
    int32_t *locators = new int32_t[numLocators];
    for (int ii = 0; ii < numLocators; ii++) {
        locators[ii] = ntohl(getStatsCommand->locators[ii]);
    }

    m_engine->resetReusedResultOutputBuffer();

    try {
        int result = m_engine->getStats(
                static_cast<int>(selector),
                locators,
                numLocators,
                interval,
                now);

        delete [] locators;

        // write the results array back across the wire
        const int8_t successResult = kErrorCode_Success;
        if (result == 1) {
            size_t bytes = write(m_fd, &successResult, sizeof(int8_t));
            assert(bytes == sizeof(int8_t));

            // write the dependency tables back across the wire
            // the result set includes the total serialization size
            const int32_t size = m_engine->getResultsSize();
            bytes = write(m_fd, m_engine->getReusedResultBuffer(), size);
            assert(bytes == size);
        } else {
            sendException(kErrorCode_Error);
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
}

void
VoltDBIPC::handoffReadyELBuffer(char* bufferPtr, int32_t bytesUsed, int32_t tableId) {
    size_t bytes = 0;

    // serialized in network order.
    // serialize as {int8_t indicator,
    //              int32_t tableId,
    //              int32_t bytes,
    //              buffer}

    char message[1 + 4 + 4];
    message[0] = static_cast<int8_t>(kErrorCode_HandoffReadELBuffer);
    *reinterpret_cast<int32_t*>(&message[1]) = htonl(tableId);
    *reinterpret_cast<int32_t*>(&message[5]) = htonl(bytesUsed);

    bytes = write(m_fd, message, 9);
    if (bytes != 9) {
        printf("Error. Blocking write of handoffReadyElBuffer indicator failed");
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    bytes = write(m_fd, bufferPtr, bytesUsed);
    if (bytes != bytesUsed) {
        printf("Error. Blocking write of handoffReadyElBuffer data failed.");
        fflush(stdout);
        assert(false);
        exit(-1);
    }
}

int8_t VoltDBIPC::activateCopyOnWrite(struct ipc_command *cmd) {
    activate_copy_on_write *activateCopyOnWriteCommand = (activate_copy_on_write*) cmd;
    const voltdb::CatalogId tableId = ntohl(activateCopyOnWriteCommand->tableId);
    try {
        if (m_engine->activateCopyOnWrite(tableId)) {
            return kErrorCode_Success;
        } else {
            return kErrorCode_Error;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

void VoltDBIPC::cowSerializeMore(struct ipc_command *cmd) {
    cow_serialize_more *cowSerializeMore = (cow_serialize_more*) cmd;
    const voltdb::CatalogId tableId = ntohl(cowSerializeMore->tableId);
    const int bufferLength = ntohl(cowSerializeMore->bufferSize);
    assert(bufferLength < MAX_MSG_SZ - 5);

    if (bufferLength >= MAX_MSG_SZ - 5) {
        char msg[3];
        msg[0] = kErrorCode_Error;
        *reinterpret_cast<int16_t*>(&msg[1]) = 0;//exception length 0
        ssize_t bytes = write(m_fd, msg, sizeof(int8_t) + sizeof(int16_t));

        if (bytes != sizeof(int8_t)) {
            printf("Error - blocking write failed. %ld", bytes);
            assert(false);
            exit(-1);
        }
    }

    try {
        ReferenceSerializeOutput out(m_reusedResultBuffer + 5, bufferLength);
        int serialized = m_engine->cowSerializeMore( &out, tableId);
        m_reusedResultBuffer[0] = kErrorCode_Success;
        *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[1]) = htonl(serialized);
        ssize_t bytesWritten = 0;

        /*
         * Already put the -1 code into the message.
         * Set it 0 so toWrite has the correct number of bytes
         */
        if (serialized == -1) {
            serialized = 0;
        }
        const ssize_t toWrite = serialized + 5;
        while (bytesWritten < toWrite) {
            ssize_t thisTime = write(m_fd, m_reusedResultBuffer + bytesWritten, toWrite - bytesWritten);
            if (thisTime == -1) {
                printf("Error - blocking write failed. %ld", toWrite);
                assert(false);
                exit(-1);
            }
            bytesWritten += thisTime;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
}

/**
 * The following code is for handling signals. A stack trace will be printed
 * when a SIGSEGV is caught.
 *
 * The code is modified based on the original code found at
 * http://tlug.up.ac.za/wiki/index.php/Obtaining_a_stack_trace_in_C_upon_SIGSEGV
 *
 * This source file is used to print out a stack-trace when your program
 * segfaults. It is relatively reliable and spot-on accurate.
 *
 * This code is in the public domain. Use it as you see fit, some credit
 * would be appreciated, but is not a prerequisite for usage. Feedback
 * on it's use would encourage further development and maintenance.
 *
 * Due to a bug in gcc-4.x.x you currently have to compile as C++ if you want
 * demangling to work.
 *
 * Please note that it's been ported into my ULS library, thus the check for
 * HAS_ULSLIB and the use of the sigsegv_outp macro based on that define.
 *
 * Author: Jaco Kroon <jaco@kroon.co.za>
 *
 * Copyright (C) 2005 - 2010 Jaco Kroon
 */
#if defined(REG_RIP)
# define SIGSEGV_STACK_IA64
#elif defined(REG_EIP)
# define SIGSEGV_STACK_X86
#else
# define SIGSEGV_STACK_GENERIC
#endif

void VoltDBIPC::signalHandler(int signum, siginfo_t *info, void *context) {
    char err_msg[128];
    sprintf(err_msg, "SIGSEGV caught: signal number %d, error value %d, signal"
            " code %d\n\n", info->si_signo, info->si_errno, info->si_code);
    std::string message = err_msg;
    message.append(m_engine->debug());
    FatalException e = FatalException(message.c_str(), __FILE__, __LINE__);

#ifndef SIGSEGV_NOSTACK
#if defined(SIGSEGV_STACK_IA64) || defined(SIGSEGV_STACK_X86)

    ucontext_t *ucontext = (ucontext_t *)context;
    int f = 0;
    Dl_info dlinfo;
    void **bp = 0;
    void *ip = 0;
    std::vector<std::string> traces;

#if defined(SIGSEGV_STACK_IA64)
    ip = (void*)ucontext->uc_mcontext.gregs[REG_RIP];
    bp = (void**)ucontext->uc_mcontext.gregs[REG_RBP];
#elif defined(SIGSEGV_STACK_X86)
    ip = (void*)ucontext->uc_mcontext.gregs[REG_EIP];
    bp = (void**)ucontext->uc_mcontext.gregs[REG_EBP];
#endif

    if (!bp || !ip) {
        e.m_traces = traces;
    }

    while(bp && ip) {
        if(!dladdr(ip, &dlinfo))
            break;

        const char *symname = dlinfo.dli_sname;
        char trace[1024];

#ifndef NO_CPP_DEMANGLE
        int status;
        char * tmp = abi::__cxa_demangle(symname, NULL, 0, &status);

        if (status == 0 && tmp)
            symname = tmp;
#endif

        sprintf(trace, "% 2d: %p <%s+%lu> (%s)\n",
                ++f,
                ip,
                symname,
                (unsigned long)ip - (unsigned long)dlinfo.dli_saddr,
                dlinfo.dli_fname);
        traces.push_back(std::string(trace));

#ifndef NO_CPP_DEMANGLE
        if (tmp)
            free(tmp);
#endif

        if(dlinfo.dli_sname && !strcmp(dlinfo.dli_sname, "main"))
            break;

        ip = bp[1];
        bp = (void**)bp[0];
    }
#endif
#endif

    crashVoltDB(e);
}

void VoltDBIPC::signalDispatcher(int signum, siginfo_t *info, void *context) {
    if (currentVolt != NULL)
        currentVolt->signalHandler(signum, info, context);
}

void VoltDBIPC::setupSigHandler(void) const {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = VoltDBIPC::signalDispatcher;
    action.sa_flags = SA_SIGINFO;
    if(sigaction(SIGSEGV, &action, NULL) < 0)
        perror("Failed to setup signal handler for SIGSEGV");
}

int main(int argc, char **argv) {
    const int pid = getpid();
    printf("==%d==", pid);
    fflush(stdout);
    int sock = -1;
    int fd = -1;
    /* max message size that can be read from java */
    int max_ipc_message_size = (1024 * 1024 * 2);

    int port = 21214;
    if (argc == 2)
        port = atoi(argv[1]);

    if (argc == 2)
        printf("Attempting to bind to port %d which was passed in as %s\n", port, argv[1]);

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;

    // read args which presumably configure VoltDBIPC

    // and set up an accept socket.
    if ((sock = socket(AF_INET,SOCK_STREAM, 0)) < 0) {
        printf("Failed to create socket.\n");
        exit(-1);
    }

    if ((bind(sock, (struct sockaddr*) (&address), sizeof(struct sockaddr_in))) != 0) {
        printf("Failed to bind socket.\n");
        exit(-2);
    }

    if ((listen(sock, 1)) != 0) {
        printf("Failed to listen on socket.\n");
        exit(-3);
    }
    printf("listening\nPort %d\n", port);
    fflush(stdout);

    struct sockaddr_in client_addr;
    socklen_t addr_size = sizeof(struct sockaddr_in);
    fd = accept(sock, (struct sockaddr*) (&client_addr), &addr_size);
    if (fd < 0) {
        printf("Failed to accept socket.\n");
        exit(-4);
    }

    int flag = 1;
    int ret = setsockopt( fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(flag) );
    if (ret == -1) {
      printf("Couldn't setsockopt(TCP_NODELAY)\n");
      exit( EXIT_FAILURE );
    }

    // requests larger than this will cause havoc.
    // cry havoc and let loose the dogs of war
    char* data = (char*) malloc(max_ipc_message_size);
    memset(data, 0, max_ipc_message_size);

    // instantiate voltdbipc to interface to EE.
    VoltDBIPC *voltipc = new VoltDBIPC(fd);

    int more = 1;
    while (more) {
        size_t bytesread = 0;

        // read the header
        while (bytesread < 4) {
            std::size_t b = read(fd, data + bytesread, 4 - bytesread);
            if (b == 0) {
                printf("client eof\n");
                goto done;
            } else if (b == -1) {
                printf("client error\n");
                goto done;
            }
            bytesread += b;
        }

        // read the message body in to the same data buffer
        int msg_size = ntohl(((struct ipc_command*) data)->msgsize);
        //printf("Received message size %d\n", msg_size);
        if (msg_size > max_ipc_message_size) {
            max_ipc_message_size = msg_size;
            char* newdata = (char*) malloc(max_ipc_message_size);
            memset(newdata, 0, max_ipc_message_size);
            memcpy(newdata, data, 4);
            free(data);
            data = newdata;
        }

        while (bytesread < msg_size) {
            std::size_t b = read(fd, data + bytesread, msg_size - bytesread);
            if (b == 0) {
                printf("client eof\n");
                goto done;
            } else if (b == -1) {
                printf("client error\n");
                goto done;
            }
            bytesread += b;
        }

        // dispatch the request
        struct ipc_command *cmd = (struct ipc_command*) data;
        // size at least length + command
        if (ntohl(cmd->msgsize) < sizeof(struct ipc_command)) {
            printf("bytesread=%zx cmd=%d msgsize=%d\n",
                   bytesread, cmd->command, ntohl(cmd->msgsize));
            for (int ii = 0; ii < bytesread; ++ii) {
                printf("%x ", data[ii]);
            }
            assert(ntohl(cmd->msgsize) >= sizeof(struct ipc_command));
        }
        bool terminate = voltipc->execute(cmd);
        if (terminate) {
            goto done;
        }
    }

    done: close(sock);
    close(fd);
    delete voltipc;
    free(data);
    fflush(stdout);
    return 0;
}
