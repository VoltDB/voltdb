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
#include "common/SegvException.hpp"
#include "common/RecoveryProtoMessage.h"
#include "common/TheHashinator.h"
#include "execution/IPCTopend.h"
#include "execution/VoltDBEngine.h"
#include "common/ThreadLocalPool.h"

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
    voltdb::TableStreamType streamType;
}__attribute__((packed)) activate_tablestream;

/*
 * Header for a Copy On Write Serialize More request
 */
typedef struct {
    struct ipc_command cmd;
    voltdb::CatalogId tableId;
    voltdb::TableStreamType streamType;
    int bufferSize;
}__attribute__((packed)) tablestream_serialize_more;

/*
 * Header for an incoming recovery message
 */
typedef struct {
    struct ipc_command cmd;
    int32_t messageLength;
    char message[0];
}__attribute__((packed)) recovery_message;

/*
 * Header for a request for a table hash code
 */
typedef struct {
    struct ipc_command cmd;
    int32_t tableId;
}__attribute__((packed)) table_hash_code;

typedef struct {
    struct ipc_command cmd;
    int32_t partitionCount;
    char data[0];
}__attribute__((packed)) hashinate_msg;

/*
 * Header for an Export action.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t isSync;
    int64_t offset;
    int64_t seqNo;
    int32_t tableSignatureLength;
    char tableSignature[0];
}__attribute__((packed)) export_action;

typedef struct {
    struct ipc_command cmd;
    int32_t tableSignatureLength;
    char tableSignature[0];
}__attribute__((packed)) get_uso;

typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    char data[0];
}__attribute__((packed)) catalog_load;


using namespace voltdb;

// file static help function to do a blocking write.
// exit on a -1.. otherwise return when all bytes
// written.
static void writeOrDie(int fd, unsigned char *data, ssize_t sz) {
    ssize_t written = 0;
    ssize_t last = 0;
    if (sz == 0) {
        return;
    }
    do {
        last = write(fd, data + written, sz - written);
        if (last < 0) {
            printf("\n\nIPC write to JNI returned -1. Exiting\n\n");
            fflush(stdout);
            exit(-1);
        }
        written += last;
    } while (written < sz);
}


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
        result = kErrorCode_None;
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
        result = activateTableStream(cmd);
        break;
      case 18:
        tableStreamSerializeMore(cmd);
        result = kErrorCode_None;
        break;
      case 19:
        result = updateCatalog(cmd);
        break;
      case 20:
        exportAction(cmd);
        result = kErrorCode_None;
        break;
      case 21:
          result = processRecoveryMessage(cmd);
        break;
      case 22:
          tableHashCode(cmd);
          result = kErrorCode_None;
          break;
      case 23:
          hashinate(cmd);
          result = kErrorCode_None;
          break;
      case 24:
          threadLocalPoolAllocations();
          result = kErrorCode_None;
          break;
      default:
        result = stub(cmd);
    }

    // write results for the simple commands. more
    // complex commands write directly in the command
    // implementation.
    if (result != kErrorCode_None) {
        if (result == kErrorCode_Error) {
            char msg[5];
            msg[0] = result;
            *reinterpret_cast<int32_t*>(&msg[1]) = 0;//exception length 0
            writeOrDie(m_fd, (unsigned char*)msg, sizeof(int8_t) + sizeof(int32_t));
        } else {
            writeOrDie(m_fd, (unsigned char*)&result, sizeof(int8_t));
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

    catalog_load *msg = reinterpret_cast<catalog_load*>(cmd);
    try {
        if (m_engine->loadCatalog(ntohll(msg->txnId), std::string(msg->data)) == true) {
            return kErrorCode_Success;
        }
    } catch (SerializableEEException &e) {}

    return kErrorCode_Error;
}

int8_t VoltDBIPC::updateCatalog(struct ipc_command *cmd) {
    assert(m_engine);
    if (!m_engine) {
        return kErrorCode_Error;
    }

    struct updatecatalog {
        struct ipc_command cmd;
        int64_t txnId;
        char data[];
    };
    struct updatecatalog *uc = (struct updatecatalog*)cmd;
    try {
        if (m_engine->updateCatalog(ntohll(uc->txnId), std::string(uc->data)) == true) {
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
        int64_t tempTableMemory;
    };
    struct initialize * cs = (struct initialize*) cmd;

    printf("initialize: cluster=%d, site=%d\n",
           ntohl(cs->clusterId), ntohl(cs->siteId));
    cs->clusterId = ntohl(cs->clusterId);
    cs->siteId = ntohl(cs->siteId);
    cs->partitionId = ntohl(cs->partitionId);
    cs->hostId = ntohl(cs->hostId);
    cs->hostnameLength = ntohs(cs->hostnameLength);
    cs->tempTableMemory = ntohll(cs->tempTableMemory);
    std::string hostname(cs->hostname, cs->hostnameLength);
    try {
        m_engine = new VoltDBEngine(new voltdb::IPCTopend(this), new voltdb::StdoutLogProxy());
        m_engine->getLogManager()->setLogLevels(cs->logLevels);
        m_reusedResultBuffer = new char[MAX_MSG_SZ];
        m_exceptionBuffer = new char[MAX_MSG_SZ];
        m_engine->setBuffers( NULL, 0, m_reusedResultBuffer, MAX_MSG_SZ, m_exceptionBuffer, MAX_MSG_SZ);
        if (m_engine->initialize(cs->clusterId,
                                 cs->siteId,
                                 cs->partitionId,
                                 cs->hostId,
                                 hostname,
                                 cs->tempTableMemory) == true) {
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
        writeOrDie(m_fd, (unsigned char*)resultBuffer, size);
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
        writeOrDie(m_fd, (unsigned char*)resultBuffer, size);
    } else {
        sendException(kErrorCode_Error);
    }
}

void VoltDBIPC::sendException(int8_t errorCode) {
    writeOrDie(m_fd, (unsigned char*)&errorCode, sizeof(int8_t));

    const void* exceptionData =
      m_engine->getExceptionOutputSerializer()->data();
    int32_t exceptionLength =
      static_cast<int32_t>(ntohl(*reinterpret_cast<const int32_t*>(exceptionData)));
    printf("Sending exception length %d\n", exceptionLength);
    fflush(stdout);

    const std::size_t expectedSize = exceptionLength + sizeof(int32_t);
    writeOrDie(m_fd, (unsigned char*)exceptionData, expectedSize);
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
        writeOrDie(m_fd, (unsigned char*)&successResult, sizeof(int8_t));
        const int32_t size = m_engine->getResultsSize();

        // write the dependency tables back across the wire
        writeOrDie(m_fd, (unsigned char*)(m_engine->getReusedResultBuffer()), size);
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
    // ...and fast serialized table last.
    void* offset = loadTableCommand->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(load_table_cmd));
    try {
        ReferenceSerializeInput serialize_in(offset, sz);

        m_engine->setUndoToken(undoToken);
        bool success = m_engine->loadTable(tableId, serialize_in, txnId, lastCommittedTxnId);
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
    char message[5];
    *dependencySz = 0;

    // tell java to send the dependency over the socket
    message[0] = static_cast<int8_t>(kErrorCode_RetrieveDependency);
    *reinterpret_cast<int32_t*>(&message[1]) = htonl(dependencyId);
    writeOrDie(m_fd, (unsigned char*)message, sizeof(int8_t) + sizeof(int32_t));

    // read java's response code
    int8_t responseCode;
    ssize_t bytes = read(m_fd, &responseCode, sizeof(int8_t));
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

    writeOrDie(m_fd,  (unsigned char*)m_reusedResultBuffer, 5 + messageLength);
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
            writeOrDie(m_fd, (unsigned char*)&successResult, sizeof(int8_t));

            // write the dependency tables back across the wire
            // the result set includes the total serialization size
            const int32_t size = m_engine->getResultsSize();
            writeOrDie(m_fd, (unsigned char*)(m_engine->getReusedResultBuffer()), size);
        } else {
            sendException(kErrorCode_Error);
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
}

int8_t VoltDBIPC::activateTableStream(struct ipc_command *cmd) {
    activate_tablestream *activateTableStreamCommand = (activate_tablestream*) cmd;
    const voltdb::CatalogId tableId = ntohl(activateTableStreamCommand->tableId);
    const voltdb::TableStreamType streamType =
            static_cast<voltdb::TableStreamType>(ntohl(activateTableStreamCommand->streamType));
    try {
        if (m_engine->activateTableStream(tableId, streamType)) {
            return kErrorCode_Success;
        } else {
            return kErrorCode_Error;
        }
    } catch (FatalException e) {
        crashVoltDB(e);
    }
    return kErrorCode_Error;
}

void VoltDBIPC::tableStreamSerializeMore(struct ipc_command *cmd) {
    tablestream_serialize_more *tableStreamSerializeMore = (tablestream_serialize_more*) cmd;
    const voltdb::CatalogId tableId = ntohl(tableStreamSerializeMore->tableId);
    const voltdb::TableStreamType streamType =
            static_cast<voltdb::TableStreamType>(ntohl(tableStreamSerializeMore->streamType));
    const int bufferLength = ntohl(tableStreamSerializeMore->bufferSize);
    assert(bufferLength < MAX_MSG_SZ - 5);

    if (bufferLength >= MAX_MSG_SZ - 5) {
        char msg[3];
        msg[0] = kErrorCode_Error;
        *reinterpret_cast<int16_t*>(&msg[1]) = 0;//exception length 0
        writeOrDie(m_fd, (unsigned char*)msg, sizeof(int8_t) + sizeof(int16_t));
    }

    try {
        ReferenceSerializeOutput out(m_reusedResultBuffer + 5, bufferLength);
        int serialized = m_engine->tableStreamSerializeMore( &out, tableId, streamType);
        m_reusedResultBuffer[0] = kErrorCode_Success;
        *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[1]) = htonl(serialized);

        /*
         * Already put the -1 code into the message.
         * Set it 0 so toWrite has the correct number of bytes
         */
        if (serialized == -1) {
            serialized = 0;
        }
        const ssize_t toWrite = serialized + 5;
        writeOrDie(m_fd, (unsigned char*)m_reusedResultBuffer, toWrite);
    } catch (FatalException e) {
        crashVoltDB(e);
    }
}

int8_t VoltDBIPC::processRecoveryMessage( struct ipc_command *cmd) {
    recovery_message *recoveryMessage = (recovery_message*) cmd;
    const int32_t messageLength = ntohl(recoveryMessage->messageLength);
    ReferenceSerializeInput input(recoveryMessage->message, messageLength);
    RecoveryProtoMsg message(&input);
    m_engine->processRecoveryMessage(&message);
    return kErrorCode_Success;
}

void VoltDBIPC::tableHashCode( struct ipc_command *cmd) {
    table_hash_code *hashCodeRequest = (table_hash_code*) cmd;
    const int32_t tableId = ntohl(hashCodeRequest->tableId);
    int64_t tableHashCode = m_engine->tableHashCode(tableId);
    char response[9];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<int64_t*>(&response[1]) = htonll(tableHashCode);
    writeOrDie(m_fd, (unsigned char*)response, 9);
}

void VoltDBIPC::exportAction(struct ipc_command *cmd) {
    export_action *action = (export_action*)cmd;

    m_engine->resetReusedResultOutputBuffer();
    int32_t tableSignatureLength = ntohl(action->tableSignatureLength);
    std::string tableSignature(action->tableSignature, tableSignatureLength);
    int64_t result = m_engine->exportAction(action->isSync,
                                         static_cast<int64_t>(ntohll(action->offset)),
                                         static_cast<int64_t>(ntohll(action->seqNo)),
                                         tableSignature);

    // write offset across bigendian.
    result = htonll(result);
    writeOrDie(m_fd, (unsigned char*)&result, sizeof(result));
}

void VoltDBIPC::getUSOForExportTable(struct ipc_command *cmd) {
    get_uso *get = (get_uso*)cmd;

    m_engine->resetReusedResultOutputBuffer();
    int32_t tableSignatureLength = ntohl(get->tableSignatureLength);
    std::string tableSignature(get->tableSignature, tableSignatureLength);

    size_t ackOffset;
    int64_t seqNo;
    m_engine->getUSOForExportTable(ackOffset, seqNo, tableSignature);

    // write offset across bigendian.
    int64_t ackOffsetI64 = static_cast<int64_t>(ackOffset);
    ackOffsetI64 = htonll(ackOffsetI64);
    writeOrDie(m_fd, (unsigned char*)&ackOffsetI64, sizeof(ackOffsetI64));

    // write the poll data. It is at least 4 bytes of length prefix.
    seqNo = htonll(seqNo);
    writeOrDie(m_fd, (unsigned char*)&seqNo, sizeof(seqNo));
}

void VoltDBIPC::hashinate(struct ipc_command* cmd) {
    hashinate_msg* hash = (hashinate_msg*)cmd;
    NValueArray& params = m_engine->getParameterContainer();

    int32_t partCount = ntohl(hash->partitionCount);
    void* offset = hash->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(hash));
    ReferenceSerializeInput serialize_in(offset, sz);

    int retval = -1;
    try {
        int cnt = serialize_in.readShort();
        assert(cnt> -1);
        Pool *pool = m_engine->getStringPool();
        deserializeParameterSetCommon(cnt, serialize_in, params, pool);
        retval =
            voltdb::TheHashinator::hashinate(params[0], partCount);
        pool->purge();
    } catch (FatalException e) {
        crashVoltDB(e);
    }

    char response[5];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<int32_t*>(&response[1]) = htonl(retval);
    writeOrDie(m_fd, (unsigned char*)response, 5);
}

void VoltDBIPC::signalHandler(int signum, siginfo_t *info, void *context) {
    char err_msg[128];
    snprintf(err_msg, 128, "SIGSEGV caught: signal number %d, error value %d,"
             " signal code %d\n\n", info->si_signo, info->si_errno,
             info->si_code);
    std::string message = err_msg;
    message.append(m_engine->debug());
    crashVoltDB(SegvException(message.c_str(), context, __FILE__, __LINE__));
}

void VoltDBIPC::signalDispatcher(int signum, siginfo_t *info, void *context) {
    if (currentVolt != NULL)
        currentVolt->signalHandler(signum, info, context);
}

void VoltDBIPC::setupSigHandler(void) const {
#if !defined(MEMCHECK)
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = VoltDBIPC::signalDispatcher;
    action.sa_flags = SA_SIGINFO;
    if(sigaction(SIGSEGV, &action, NULL) < 0)
        perror("Failed to setup signal handler for SIGSEGV");
#endif
}

void VoltDBIPC::threadLocalPoolAllocations() {
    std::size_t poolAllocations = ThreadLocalPool::getPoolAllocationSize();
    char response[9];
    response[0] = kErrorCode_Success;
    *reinterpret_cast<std::size_t*>(&response[1]) = htonll(poolAllocations);
    writeOrDie(m_fd, (unsigned char*)response, 9);
}

int64_t VoltDBIPC::getQueuedExportBytes(int32_t partitionId, std::string signature) {
    m_reusedResultBuffer[0] = kErrorCode_getQueuedExportBytes;
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[1]) = htonl(partitionId);
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[5]) = htonl(static_cast<int32_t>(signature.size()));
    ::memcpy( &m_reusedResultBuffer[9], signature.c_str(), signature.size());
    writeOrDie(m_fd, (unsigned char*)m_reusedResultBuffer, 9 + signature.size());

    int64_t netval;
    ssize_t bytes = read(m_fd, &netval, sizeof(int64_t));
    if (bytes != sizeof(int64_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(int64_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    int64_t retval = ntohll(netval);
    return retval;
}

void VoltDBIPC::pushExportBuffer(
        int64_t exportGeneration,
        int32_t partitionId,
        std::string signature,
        voltdb::StreamBlock *block,
        bool sync,
        bool endOfStream) {
    int32_t index = 0;
    m_reusedResultBuffer[index++] = kErrorCode_pushExportBuffer;
    *reinterpret_cast<int64_t*>(&m_reusedResultBuffer[index]) = htonll(exportGeneration);
    index += 8;
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[index]) = htonl(partitionId);
    index += 4;
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[index]) = htonl(static_cast<int32_t>(signature.size()));
    index += 4;
    ::memcpy( &m_reusedResultBuffer[index], signature.c_str(), signature.size());
    index += static_cast<int32_t>(signature.size());
    if (block != NULL) {
        *reinterpret_cast<int64_t*>(&m_reusedResultBuffer[index]) = htonll(block->uso());
    } else {
        *reinterpret_cast<int64_t*>(&m_reusedResultBuffer[index]) = 0;
    }
    index += 8;
    *reinterpret_cast<int8_t*>(&m_reusedResultBuffer[index++]) =
        sync ?
            static_cast<int8_t>(1) : static_cast<int8_t>(0);
    *reinterpret_cast<int8_t*>(&m_reusedResultBuffer[index++]) =
        endOfStream ?
            static_cast<int8_t>(1) : static_cast<int8_t>(0);
    if (block != NULL) {
        *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[index]) = htonl(block->rawLength());
        writeOrDie(m_fd, (unsigned char*)m_reusedResultBuffer, index + 4);
        writeOrDie(m_fd, (unsigned char*)block->rawPtr(), block->rawLength());
    } else {
        *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[index]) = htonl(0);
        writeOrDie(m_fd, (unsigned char*)m_reusedResultBuffer, index + 4);
    }
    delete [] block->rawPtr();
}

int main(int argc, char **argv) {
    //Create a pool ref to init the thread local in case a poll message comes early
    voltdb::ThreadLocalPool poolRef;
    const int pid = getpid();
    printf("==%d==\n", pid);
    fflush(stdout);
    int sock = -1;
    int fd = -1;
    /* max message size that can be read from java */
    int max_ipc_message_size = (1024 * 1024 * 2);

    int port = 0;

    if (argc == 2) {
        printf("Binding to a specific socket is no longer supported\n");
        exit(-1);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = INADDR_ANY;


    // read args which presumably configure VoltDBIPC

    // and set up an accept socket.
    if ((sock = socket(AF_INET,SOCK_STREAM, 0)) < 0) {
        printf("Failed to create socket.\n");
        exit(-2);
    }

    if ((bind(sock, (struct sockaddr*) (&address), sizeof(struct sockaddr_in))) != 0) {
        printf("Failed to bind socket.\n");
        exit(-3);
    }

    socklen_t address_len = sizeof(struct sockaddr_in);
    if (getsockname( sock, reinterpret_cast<sockaddr*>(&address), &address_len)) {
        printf("Failed to find socket address\n");
        exit(-4);
    }

    port = ntohs(address.sin_port);
    printf("==%d==\n", port);
    fflush(stdout);

    if ((listen(sock, 1)) != 0) {
        printf("Failed to listen on socket.\n");
        exit(-5);
    }
    printf("listening\n");
    fflush(stdout);

    struct sockaddr_in client_addr;
    socklen_t addr_size = sizeof(struct sockaddr_in);
    fd = accept(sock, (struct sockaddr*) (&client_addr), &addr_size);
    if (fd < 0) {
        printf("Failed to accept socket.\n");
        exit(-6);
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

  done:
    close(sock);
    close(fd);
    delete voltipc;
    free(data);
    fflush(stdout);
    return 0;
}
