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

/*
 Implement the Java ExecutionEngine interface using IPC to a standalone EE
 process. This allows the backend to run without a JVM - useful for many
 debugging tasks.  Represents a single EE in a single process. Accepts
 and executes commands from Java synchronously.
 */

#include "common/Topend.h"

#include "execution/VoltDBEngine.h"
#include "logging/StdoutLogProxy.h"
#include "storage/table.h"

#include "common/ElasticHashinator.h"
#include "common/LegacyHashinator.h"
#include "common/RecoveryProtoMessage.h"
#include "common/serializeio.h"
#include "common/SegvException.hpp"
#include "common/types.h"

#include <signal.h>
#include <sys/socket.h>
#include <netinet/tcp.h> // for TCP_NODELAY

// Please don't make this different from the JNI result buffer size
// defined in ExecutionEngineJNI.java .
// This determines the size of the EE results buffer and it's nice
// if IPC and JNI are matched.
#define MAX_MSG_SZ (1024*1024*10)

using namespace std;

namespace voltdb {
class Pool;
class StreamBlock;
}

using namespace voltdb;

static const int8_t
    // These two definitions must match the definitions in ExecutionEngine.java
    ERRORCODE_SUCCESS = 0,
    ERRORCODE_ERROR = 1,

    // These "error codes" are actually command codes for upstream commands to the java process,
    // but they need to be disjoint from other error codes so that the java code can determine
    // when it is getting an upstream command instead of a direct response to its last command.
    // The voltdbipc processing for the responses to these upstream commands does not need
    // special reponse codes because the upstream messages are responded to synchronously
    // and are never interrupted by any other downstream traffic.

    // These definitions MUST MATCH the definitions in ExecutionEngineIPC.java
    ERRORCODE_NEED_PLAN = 90,                         // Request for uncached plan bytes
    ERRORCODE_NEED_PROGRESS_UPDATE = 91,              // Request for "long run" reporting or timeout
    ERRORCODE_NEED_DECODE_BASE64_AND_DECOMPRESS = 92, // Request for decoding work
    ERRORCODE_NEED_DEPENDENCY = 93,                   // Request for dependent data
    ERRORCODE_NEED_BUFFER_EXPORT = 94,                // Indication that export buffer is next
    ERRORCODE_NEED_QUEUED_EXPORT_BYTES_STAT = 95,     // Retrieve value for stats

    ERRORCODE_CRASH_VOLTDB = 99,                      // Crash with reason string
    // These definitions MUST MATCH the definitions in ExecutionEngineIPC.java

    ERRORCODE_UNSET = -1,        // not in the java
    ERRORCODE_ALREADY_SENT = -1; // not in the java

// Set this to true to indicate to the ee threads to terminate gracefully
// after finishing execution of their current/next command.
// Normally threads only terminate when disconnected from the java process.
// This is theoretically useful when running Valgrind because you can terminate
// at the point where you think memory has leaked and this setting will try to
// do an orderly shutdown of the VoltDBEngines and other allocated resources.
// Timing MAY be an issue in a multi-eethread-ed configuration since termination
// depends on all eethreads being active to detect this setting after a command
// execution. It might be smarter to try to trigger this on the java side
// with a legitimate disconnection of each ExecutionEngineIPC which would have
// the natural side effect of causing the same kind of shutdown.
// For now, this flag can only be set within this module.
// If it became useful to set it from deep in the volt library code, the abstract
// Topend should define the no-op "virtual void terminateEarlyForProfiling() {}"
// to avoid dependencies on this module.
// This method is defined as needlessly virtual on VoltDBIPC to be ready for that.
volatile static bool terminateEarlyForProfiling = false;

class VoltDBIPC : public Topend {
public:
    VoltDBIPC(int fd);

    ~VoltDBIPC();

    //
    // Required Topend methods.
    // These are cut and paste from Topend.h then just slightly tweaked
    // to remove any abstract "= 0" annotations.
    // Avoid other kinds of reformatting in this section to make it easier to
    // track changes from there to here.
    // Comments in this section are from Topend.h and reflect the INTENT of the
    // API call. IPC Implementation-specific comments can/should be added where
    // the override is defined outside the class body.
    //

    virtual int loadNextDependency(
        int32_t dependencyId, voltdb::Pool *pool, Table* destination);

    // Update the topend on query progress and give the topend a chance to tell the
    // query to stop.
    // Return 0 if the Topend wants the EE to stop processing the current fragment
    // or the number of tuples the EE should process before repeating this call.
    virtual int64_t fragmentProgressUpdate(int32_t batchIndex, std::string planNodeName,
                std::string targetTableName, int64_t targetTableSize, int64_t tuplesProcessed,
                int64_t currMemoryInBytes, int64_t peakMemoryInBytes);

    virtual std::string planForFragmentId(int64_t fragmentId);

    virtual void crashVoltDB(voltdb::FatalException e);

    virtual int64_t getQueuedExportBytes(int32_t partitionId, std::string signature);
    virtual void pushExportBuffer(
            int64_t exportGeneration,
            int32_t partitionId,
            std::string signature,
            StreamBlock *block,
            bool sync,
            bool endOfStream);

    virtual void pushDRBuffer(int32_t partitionId, StreamBlock *block);

    virtual void fallbackToEEAllocatedBuffer(char *buffer, size_t length);

    /** Calls the java method in org.voltdb.utils.Encoder */
    virtual std::string decodeBase64AndDecompress(const std::string& buffer);

    //
    // End of copy-paste-tweaked section from Topend.h
    //

    //
    // IPC interface support methods.
    //

    // This may become a Topend virtual function some day.
    // See the comment on the static terminateEarlyForProfiling variable above.
    virtual void terminateEarlyForProfiling()
    { ::terminateEarlyForProfiling = true; }

    /// Dispatch a received command.
    /// There MAY be an advantage to making the eethread function a static memeber function
    /// and making execute private -- but not a huge one.
    void execute(struct ipc_command *cmd);

    /// helper for loadNextDependency
    char *retrieveDependency(int32_t dependencyId, size_t *dependencySz);

private:
    // The IPC command implementations in no particular order.
    // Most of these have parallel JNI entry points in voltdbjni.cpp.
    // Many delegate to a supporting VoltDBEngine call.
    int8_t loadCatalog(struct ipc_command *cmd);
    int8_t updateCatalog(struct ipc_command *cmd);
    void initialize(struct ipc_command *cmd);
    int8_t toggleProfiler(struct ipc_command *cmd);
    int8_t releaseUndoToken(struct ipc_command *cmd);
    int8_t undoUndoToken(struct ipc_command *cmd);
    int8_t tick(struct ipc_command *cmd);
    void quiesce(struct ipc_command *cmd);
    int8_t setLogLevels(struct ipc_command *cmd);
    void executePlanFragments(struct ipc_command *cmd);
    void getStats(struct ipc_command *cmd);
    int8_t loadTable(struct ipc_command *cmd);
    void processRecoveryMessage(struct ipc_command *cmd);
    void tableHashCode(struct ipc_command *cmd);
    void hashinate(struct ipc_command* cmd);
    void updateHashinator(struct ipc_command *cmd);
    void threadLocalPoolAllocations();
    void applyBinaryLog(struct ipc_command*);
    void executeTask(struct ipc_command*);
    void activateTableStream(struct ipc_command *cmd);
    void tableStreamSerializeMore(struct ipc_command *cmd);
    void exportAction(struct ipc_command *cmd);
    void getUSOForExportTable(struct ipc_command *cmd);
    int8_t stub(struct ipc_command *cmd);


    void sendSimpleSuccess();
    template<typename T> void sendPrimitiveResult(const T& result);
    void sendSerializedResult();
    void sendSerializedException(int8_t errorCode);
    void sendDummyError();

    void signalHandler(int signum, siginfo_t *info, void *context);
    static void signalDispatcher(int signum, siginfo_t *info, void *context);
    void setupSigHandler(void) const;

    VoltDBEngine *m_engine;
    long m_counter;
    int m_fd;
    char *m_reusedResultBuffer;
    char *m_exceptionBuffer;

    // The tuple buffer gets expanded (doubled) as needed, but never compacted.
    char *m_tupleBuffer;
    size_t m_tupleBufferSize;
};

/* java sends all data with this header */
struct ipc_command {
    int32_t msgsize;
    int32_t command;
    char data[0];
}__attribute__((packed));

// FIXME: All of these per-command overlay structures should go away.
// They SEEMED useful to avoid procedural code to get the raw bytes into
// usable typed fields, EXCEPT that most fields would then need to be
// procedurally "reserialized" via ntohXX calls.
// Since this procedural step is required anyway, it is easier AND safer
// to sequentially deserialize directly from the raw byte buffer into
// properly typed local variables using a smart implicitly
// template-driven ntohXX-enabled deserializer.
// This would completely hide the raw values that these overlay classes
// expose to the unwary programmer who may forget to apply ntohXX before
// using the value.
/*
 * Structure describing an executePlanFragments message header.
 */
typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    int64_t spHandle;
    int64_t lastCommittedSpHandle;
    int64_t uniqueId;
    int64_t undoToken;
    int32_t numFragmentIds;
    char data[0];
}__attribute__((packed)) querypfs;

typedef struct {
    struct ipc_command cmd;
    int32_t planFragLength;
    char data[0];
}__attribute__((packed)) loadfrag;

/*
 * Header for a load table request.
 */
typedef struct {
    struct ipc_command cmd;
    int32_t tableId;
    int64_t txnId;
    int64_t spHandle;
    int64_t lastCommittedSpHandle;
    int64_t uniqueId;
    int64_t undoToken;
    int32_t undo;
    int32_t shouldDRStream;
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
    int64_t undoToken;
    char data[0];
}__attribute__((packed)) activate_tablestream;

/*
 * Header for a Copy On Write Serialize More request
 */
typedef struct {
    struct ipc_command cmd;
    voltdb::CatalogId tableId;
    voltdb::TableStreamType streamType;
    int bufferCount;
    char data[0];
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
    int32_t hashinatorType;
    int32_t configLength;
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
    int64_t timestamp;
    char data[0];
}__attribute__((packed)) catalog_load;

typedef struct {
    struct ipc_command cmd;
    int64_t taskId;
    char task[0];
}__attribute__((packed)) execute_task;

typedef struct {
    struct ipc_command cmd;
    int64_t txnId;
    int64_t spHandle;
    int64_t lastCommittedSpHandle;
    int64_t uniqueId;
    int64_t undoToken;
    char log[0];
}__attribute__((packed)) apply_binary_log;

/// htont, a type-driven generic replacement for htons, htonl, htonll.
/// Rationale: driving the selection of htonX function from the argument type
/// removes the opportunity for a mistake that the compiler MAY cover up
/// via type promotion.
/// If the argument types don't match the intended htonX units or buffer layouts,
/// there is always going to be some subtle problem.
template<typename T>
inline T htont(const T& raw);

inline signed char htont(const signed char& raw) { return raw; }
inline short htont(const short& raw) { return htons(raw); }
inline int htont(const int& raw) { return htonl(raw); }
inline long htont(const long& raw) { return htonll(raw); }
inline unsigned long htont(const unsigned long& raw) { return htonll(raw); }

// file static help function to do a blocking write.
// exit on a -1.. otherwise return when all bytes
// written.
static void writeOrDie(int fd, const char *data, ssize_t sz) {
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

template<int8_t TAG, size_t SZ>
class IpcResponseBuilder {
public:
    IpcResponseBuilder() : m_position(1)
    { m_content[0] = static_cast<char>(TAG); }

    template<typename T>
    IpcResponseBuilder& append(const T& raw) {
        *reinterpret_cast<T*>(&(m_content[m_position])) = htont(raw);
        m_position += static_cast<int>(sizeof(T));
        return *this;
    }

    IpcResponseBuilder& append(const bool& raw) {
        m_content[m_position++] = (raw ? static_cast<char>(1) : static_cast<char>(0));
        return *this;
    }

    // It's not allowed to override valid tags.
    IpcResponseBuilder& overrideResponseCode(int8_t validTag) {
        assert(static_cast<char>(ERRORCODE_UNSET) == m_content[0]);
        assert(ERRORCODE_UNSET != validTag);
        assert(static_cast<char>(TAG) == m_content[0]);

        m_content[0] = static_cast<char>(validTag);
        return *this;
    }

    void writeOrDie(int fd)
    {
        assert(static_cast<char>(ERRORCODE_UNSET) != m_content[0]);
        assert(SZ == m_position);
        ::writeOrDie(fd, m_content, SZ);
    }

private:
    int m_position;
    char m_content[SZ];
};

/*
 * This is used by the signal dispatcher
 */
static VoltDBIPC *currentVolt = NULL;

/**
 * Utility used for deserializing ParameterSet passed from Java.
 */
void deserializeParameterSetCommon(int cnt, ReferenceSerializeInputBE &serialize_in,
                                   NValueArray &params, Pool *stringPool)
{
    for (int i = 0; i < cnt; ++i) {
        params[i].deserializeFromAllocateForStorage(serialize_in, stringPool);
    }
}

VoltDBIPC::VoltDBIPC(int fd) : m_fd(fd) {
    currentVolt = this;
    m_engine = NULL;
    m_counter = 0;
    m_reusedResultBuffer = NULL;
    m_tupleBuffer = NULL;
    m_tupleBufferSize = 0;

    setupSigHandler();
}

VoltDBIPC::~VoltDBIPC() {
    delete m_engine;
    delete [] m_reusedResultBuffer;
    delete [] m_tupleBuffer;
    delete [] m_exceptionBuffer;
    close(m_fd);
}

inline void VoltDBIPC::sendSimpleSuccess() {
    IpcResponseBuilder<ERRORCODE_SUCCESS, 1> genericSuccess;
    genericSuccess.writeOrDie(m_fd);
}

template<typename T>
inline void VoltDBIPC::sendPrimitiveResult(const T& result) {
    IpcResponseBuilder<ERRORCODE_SUCCESS, 1 + sizeof(T)> genericResult;
    genericResult.append(result).writeOrDie(m_fd);
}

inline void VoltDBIPC::sendSerializedResult() {
    m_reusedResultBuffer[0] = static_cast<char>(ERRORCODE_SUCCESS);
    const int32_t size = m_engine->getResultsSize();
    ::writeOrDie(m_fd, m_reusedResultBuffer, size);
}

inline void VoltDBIPC::sendSerializedException(int8_t errorCode) {
    int32_t size = static_cast<int32_t>(m_engine->getExceptionOutputSerializer()->size());
    // Callers should have reset the reusable buffers with an initial position
    // of 1 to save room for the response code set here and should have added
    // at least a 4-byte length for whatever exception detail follows.
    // Even without exception detail -- which is weak -- there should still
    // be a zero-valued length serialized to keep ExecutionEngineIPC from
    // over-reading.
    assert(size >= 5);
    m_exceptionBuffer[0] = static_cast<char>(errorCode);
    writeOrDie(m_fd, m_exceptionBuffer, size);
}

inline void VoltDBIPC::sendDummyError() {
    IpcResponseBuilder<ERRORCODE_ERROR, 5> genericError;
    int32_t zero = 0;
    genericError.append(zero).writeOrDie(m_fd);
}

void VoltDBIPC::execute(struct ipc_command *cmd) {
    int8_t result = ERRORCODE_ALREADY_SENT;

    if (0)
        std::cout << "IPC client command: " << ntohl(cmd->command) << std::endl;

    // commands must match java's ExecutionEngineIPC.Command
    // could enumerate but they're only used in this one place.
    switch (ntohl(cmd->command)) {
    case 0:
        initialize(cmd);
        result = ERRORCODE_ALREADY_SENT;
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
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 6:
        // also writes results directly
        executePlanFragments(cmd);
        result = ERRORCODE_ALREADY_SENT;
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
    case 13:
        result = setLogLevels(cmd);
        break;
    case 16:
        quiesce(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 17:
        activateTableStream(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 18:
        tableStreamSerializeMore(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 19:
        result = updateCatalog(cmd);
        break;
    case 20:
        exportAction(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 21:
        processRecoveryMessage(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 22:
        tableHashCode(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 23:
        hashinate(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 24:
        threadLocalPoolAllocations();
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 27:
        updateHashinator(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 28:
        executeTask(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    case 29:
        applyBinaryLog(cmd);
        result = ERRORCODE_ALREADY_SENT;
        break;
    default:
        result = stub(cmd);
    }

    // Send pass/fail results for some simple commands.
    // More complex commands send directly from the command
    // implementation and leave result == ERRORCODE_ALREADY_SENT.
    // FIXME: This code should be phased out.
    // The signature of the command functions could be standardized
    // to return void. Then ALL commands would be responsible for
    // sending their results.
    // Helper functions like sendSimpleSuccess() and possibly some
    // variations on sendSerializedException could be provided to ease their burden.
    if (result == ERRORCODE_ERROR) {
        // The ERRORCODE_ERROR return requires an explanatory message -- even if it's 0 length.
        sendDummyError();
    } else if (result == ERRORCODE_SUCCESS) {
        sendSimpleSuccess();
    } else {
        // Alternative error codes must be sent from within
        // specific command functions and result left/set ==
        // ERRORCODE_ALREADY_SENT.
        assert(result == ERRORCODE_ALREADY_SENT);
    }
}

int8_t VoltDBIPC::stub(struct ipc_command *cmd) {
    printf("IPC command %d not implemented.\n", ntohl(cmd->command));
    fflush(stdout);
    return ERRORCODE_ERROR;
}

int8_t VoltDBIPC::loadCatalog(struct ipc_command *cmd) {
    printf("loadCatalog\n");
    assert(m_engine);
    if ( ! m_engine) {
        return ERRORCODE_ERROR;
    }

    catalog_load *msg = reinterpret_cast<catalog_load*>(cmd);
    try {
        if (m_engine->loadCatalog(ntohll(msg->timestamp), std::string(msg->data)) == true) {
            return ERRORCODE_SUCCESS;
        }
    //TODO: FatalException and SerializableException should be universally caught and handled in "execute",
    // rather than in hard-to-maintain "execute method" boilerplate code like this.
    } catch (const FatalException& e) {
        crashVoltDB(e);
    } catch (const SerializableEEException &e) {} //TODO: We don't really want to quietly SQUASH non-fatal exceptions.

    return ERRORCODE_ERROR;
}

int8_t VoltDBIPC::updateCatalog(struct ipc_command *cmd) {
    assert(m_engine);
    if ( ! m_engine) {
        return ERRORCODE_ERROR;
    }

    struct updatecatalog {
        struct ipc_command cmd;
        int64_t timestamp;
        char data[];
    };
    struct updatecatalog *uc = (struct updatecatalog*)cmd;
    try {
        if (m_engine->updateCatalog(ntohll(uc->timestamp), std::string(uc->data)) == true) {
            return ERRORCODE_SUCCESS;
        }
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
    return ERRORCODE_ERROR;
}

void VoltDBIPC::initialize(struct ipc_command *cmd) {
    // expect a single initialization.
    assert( ! m_engine);
    delete m_engine;

    // voltdbengine::initialize expects catalogids.
    assert(sizeof(CatalogId) == sizeof(int));

    struct initialize {
        struct ipc_command cmd;
        int clusterId;
        long siteId;
        int partitionId;
        int hostId;
        int64_t logLevels;
        int64_t tempTableMemory;
        int32_t createDrReplicatedStream;
        int32_t hostnameLength;
        char data[0];
    }__attribute__((packed));
    struct initialize * cs = (struct initialize*) cmd;

    printf("initialize: cluster=%d, site=%jd\n",
           ntohl(cs->clusterId), (intmax_t)ntohll(cs->siteId));
    cs->clusterId = ntohl(cs->clusterId);
    cs->siteId = ntohll(cs->siteId);
    cs->partitionId = ntohl(cs->partitionId);
    cs->hostId = ntohl(cs->hostId);
    cs->logLevels = ntohll(cs->logLevels);
    cs->tempTableMemory = ntohll(cs->tempTableMemory);
    cs->createDrReplicatedStream = ntohl(cs->createDrReplicatedStream);
    bool createDrReplicatedStream = cs->createDrReplicatedStream != 0;
    cs->hostnameLength = ntohl(cs->hostnameLength);

    std::string hostname(cs->data, cs->hostnameLength);
    try {
        m_engine = new VoltDBEngine(this, new StdoutLogProxy());
        m_engine->getLogManager()->setLogLevels(cs->logLevels);
        m_reusedResultBuffer = new char[MAX_MSG_SZ];
        m_exceptionBuffer = new char[MAX_MSG_SZ];
        m_engine->setBuffers( NULL, 0, m_reusedResultBuffer, MAX_MSG_SZ, m_exceptionBuffer, MAX_MSG_SZ);
        // The tuple buffer gets expanded (doubled) as needed, but never compacted.
        m_tupleBufferSize = MAX_MSG_SZ;
        m_tupleBuffer = new char[m_tupleBufferSize];

        m_engine->initialize(cs->clusterId, cs->siteId, cs->partitionId,
                cs->hostId, hostname, cs->tempTableMemory, createDrReplicatedStream);
        sendSimpleSuccess();
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
}

int8_t VoltDBIPC::toggleProfiler(struct ipc_command *cmd) {
    assert(m_engine);
    if ( ! m_engine) {
        return ERRORCODE_ERROR;
    }

    struct toggle {
        struct ipc_command cmd;
        int toggle;
    }__attribute__((packed));
    struct toggle * cs = (struct toggle*) cmd;

    printf("toggleProfiler: toggle=%d\n", ntohl(cs->toggle));

    // actually, the engine doesn't implement this now.
    // m_engine->ProfilerStart();
    return ERRORCODE_SUCCESS;
}

int8_t VoltDBIPC::releaseUndoToken(struct ipc_command *cmd) {
    assert(m_engine);
    if ( ! m_engine) {
        return ERRORCODE_ERROR;
    }

    struct undo_token * cs = (struct undo_token*) cmd;

    try {
        m_engine->releaseUndoToken(ntohll(cs->token));
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }

    return ERRORCODE_SUCCESS;
}

int8_t VoltDBIPC::undoUndoToken(struct ipc_command *cmd) {
    assert(m_engine);
    if ( ! m_engine) {
        return ERRORCODE_ERROR;
    }

    struct undo_token * cs = (struct undo_token*) cmd;

    try {
        m_engine->undoUndoToken(ntohll(cs->token));
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }

    return ERRORCODE_SUCCESS;
}

int8_t VoltDBIPC::tick(struct ipc_command *cmd) {
    assert (m_engine);
    if ( ! m_engine) {
        return ERRORCODE_ERROR;
    }

    struct tick {
        struct ipc_command cmd;
        int64_t time;
        int64_t lastSpHandle;
    }__attribute__((packed));

    struct tick * cs = (struct tick*) cmd;
    //std::cout << "tick: time=" << cs->time << " txn=" << cs->lastTxnId << std::endl;

    try {
        // no return code. can't fail!
        m_engine->tick(ntohll(cs->time), ntohll(cs->lastSpHandle));
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }

    return ERRORCODE_SUCCESS;
}

void VoltDBIPC::quiesce(struct ipc_command *cmd) {
    struct quiesce {
        struct ipc_command cmd;
        int64_t lastSpHandle;
    }__attribute__((packed));

    struct quiesce *cs = (struct quiesce*)cmd;

    try {
        m_engine->quiesce(ntohll(cs->lastSpHandle));
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }

    sendSimpleSuccess();
}

void VoltDBIPC::executePlanFragments(struct ipc_command *cmd) {
    int errors = 0;

    querypfs *queryCommand = (querypfs*) cmd;

    int32_t numFrags = ntohl(queryCommand->numFragmentIds);

    if (0) {
        std::cout << "querypfs:"
                  << " spHandle=" << ntohll(queryCommand->spHandle)
                  << " lastCommittedSphandle=" << ntohll(queryCommand->lastCommittedSpHandle)
                  << " undoToken=" << ntohll(queryCommand->undoToken)
                  << " numFragIds=" << numFrags << std::endl;
    }

    // data has binary packed fragmentIds first
    int64_t *fragmentIds = (int64_t*) (&(queryCommand->data));
    int64_t *inputDepIds = fragmentIds + numFrags;

    // fix network byte order
    for (int i = 0; i < numFrags; ++i) {
        fragmentIds[i] = ntohll(fragmentIds[i]);
        inputDepIds[i] = ntohll(inputDepIds[i]);
    }

    // ...and fast serialized parameter sets last.
    void* offset = queryCommand->data + (sizeof(int64_t) * numFrags * 2);
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(querypfs) - sizeof(int32_t) * ntohl(queryCommand->numFragmentIds));
    ReferenceSerializeInputBE serialize_in(offset, sz);

    // and reset to space for the results output
    m_engine->resetReusedResultOutputBuffer(1); // reserve 1 byte to add status code

    try {
        errors = m_engine->executePlanFragments(numFrags,
                                                fragmentIds,
                                                inputDepIds,
                                                serialize_in,
                                                ntohll(queryCommand->txnId),
                                                ntohll(queryCommand->spHandle),
                                                ntohll(queryCommand->lastCommittedSpHandle),
                                                ntohll(queryCommand->uniqueId),
                                                ntohll(queryCommand->undoToken));
    }
    catch (const FatalException &e) {
        crashVoltDB(e);
    }

    // write the results array back across the wire
    if (errors == 0) {
        // write the results array back across the wire
        sendSerializedResult();
    } else {
        sendSerializedException(ERRORCODE_ERROR);
    }
}

int8_t VoltDBIPC::loadTable(struct ipc_command *cmd) {
    load_table_cmd *loadTableCommand = (load_table_cmd*) cmd;

    if (0) {
        std::cout << "loadTable:" << " tableId=" << ntohl(loadTableCommand->tableId)
                  << " spHandle=" << ntohll(loadTableCommand->spHandle) << " lastCommittedSpHandle="
                  << ntohll(loadTableCommand->lastCommittedSpHandle) << std::endl;
    }

    const int32_t tableId = ntohl(loadTableCommand->tableId);
    const int64_t txnId = ntohll(loadTableCommand->txnId);
    const int64_t spHandle = ntohll(loadTableCommand->spHandle);
    const int64_t lastCommittedSpHandle = ntohll(loadTableCommand->lastCommittedSpHandle);
    const int64_t uniqueId = ntohll(loadTableCommand->uniqueId);
    const int64_t undoToken = ntohll(loadTableCommand->undoToken);
    const bool undo = loadTableCommand->undo != 0;
    const bool shouldDRStream = loadTableCommand->shouldDRStream != 0;
    // ...and fast serialized table last.
    void* offset = loadTableCommand->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(load_table_cmd));
    try {
        ReferenceSerializeInputBE serialize_in(offset, sz);
        m_engine->setUndoToken(undoToken);

        bool success = m_engine->loadTable(tableId, serialize_in, txnId, spHandle, lastCommittedSpHandle, uniqueId, undo, shouldDRStream);
        if (success) {
            return ERRORCODE_SUCCESS;
        }
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
    return ERRORCODE_ERROR;
}

int8_t VoltDBIPC::setLogLevels(struct ipc_command *cmd) {
    int64_t logLevels = *((int64_t*)&cmd->data[0]);
    try {
        m_engine->getLogManager()->setLogLevels(logLevels);
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
    return ERRORCODE_SUCCESS;
}

/**
 * Retrieve a dependency from Java via the IPC connection.
 * If there is dependency data, it is loaded into the destination table.
 * @return 1 if there were more dependency tables, 0 otherwise
 */
int VoltDBIPC::loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination) {
    VOLT_DEBUG("iterating java dependency for id %d\n", dependencyId);
    // tell java to send the dependency over the socket
    IpcResponseBuilder<ERRORCODE_NEED_DEPENDENCY, 5> request;
    request.append(dependencyId).writeOrDie(m_fd);

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
    if (ERRORCODE_ERROR == responseCode) {
        return 0;
    }
    if (ERRORCODE_SUCCESS != responseCode) {
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

    dependencyLength = ntohl(dependencyLength);
    if (dependencyLength == 0) {
        return 0;
    }

    bytes = 0;
    size_t dependencySz = (size_t)dependencyLength;
    char *dependencyData = new char[dependencySz];
    while (bytes != dependencySz) {
        ssize_t moreBytes = read(m_fd, dependencyData + bytes, dependencySz - bytes);
        if (moreBytes <= 0) {
            break;
        }
        bytes += moreBytes;
    }

    if (bytes != dependencySz) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)dependencyLength);
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    ReferenceSerializeInputBE serialize_in(dependencyData, dependencySz);
    destination->loadTuplesFrom(serialize_in, stringPool);
    delete [] dependencyData;
    return 1;
}

int64_t VoltDBIPC::fragmentProgressUpdate(int32_t batchIndex,
        std::string planNodeName,
        std::string targetTableName,
        int64_t targetTableSize,
        int64_t tuplesProcessed,
        int64_t currMemoryInBytes,
        int64_t peakMemoryInBytes) {
    char message[sizeof(int8_t) +
                 sizeof(int16_t) +
                 planNodeName.size() +
                 sizeof(int16_t) +
                 targetTableName.size() +
                 sizeof(targetTableSize) +
                 sizeof(tuplesProcessed) +
                 sizeof(currMemoryInBytes) +
                 sizeof(peakMemoryInBytes)];
    message[0] = ERRORCODE_NEED_PROGRESS_UPDATE;
    size_t offset = 1;

    *reinterpret_cast<int32_t*>(&message[offset]) = htonl(batchIndex);
    offset += sizeof(batchIndex);

    int16_t strSize = static_cast<int16_t>(planNodeName.size());
    *reinterpret_cast<int16_t*>(&message[offset]) = htons(strSize);
    offset += sizeof(strSize);
    ::memcpy( &message[offset], planNodeName.c_str(), strSize);
    offset += strSize;

    strSize = static_cast<int16_t>(targetTableName.size());
    *reinterpret_cast<int16_t*>(&message[offset]) = htons(strSize);
    offset += sizeof(strSize);
    ::memcpy( &message[offset], targetTableName.c_str(), strSize);
    offset += strSize;

    *reinterpret_cast<int64_t*>(&message[offset]) = htonll(targetTableSize);
    offset += sizeof(targetTableSize);

    *reinterpret_cast<int64_t*>(&message[offset]) = htonll(tuplesProcessed);
    offset += sizeof(tuplesProcessed);

    *reinterpret_cast<int64_t*>(&message[offset]) = htonll(currMemoryInBytes);
    offset += sizeof(tuplesProcessed);

    *reinterpret_cast<int64_t*>(&message[offset]) = htonll(peakMemoryInBytes);
    offset += sizeof(tuplesProcessed);

    int32_t length;
    ssize_t bytes = read(m_fd, &length, sizeof(int32_t));
    if (bytes != sizeof(length)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(length));
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    length = static_cast<int32_t>(ntohl(length) - sizeof(length));
    assert(length > 0);

    int64_t nextStep;
    bytes = read(m_fd, &nextStep, sizeof(nextStep));
    if (bytes != sizeof(nextStep)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
                (intmax_t)bytes, (intmax_t)sizeof(nextStep));
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    nextStep = ntohll(nextStep);

    return nextStep;
}

// A file static helper function that
//   Reads a 4-byte integer from fd that is the length of the following string
//   Reads the bytes for the string
//   Returns those bytes as an std::string
static std::string readLengthPrefixedBytesToStdString(int fd) {
    int32_t length;
    ssize_t numBytesRead = read(fd, &length, sizeof(int32_t));
    if (numBytesRead != sizeof(int32_t)) {
        printf("Error - blocking read failed. %jd read %jd attempted",
               (intmax_t)numBytesRead, (intmax_t)sizeof(int32_t));
        fflush(stdout);
        assert(false);
        exit(-1);
    }
    length = static_cast<int32_t>(ntohl(length) - sizeof(int32_t));
    assert(length > 0);

    boost::scoped_array<char> bytes(new char[length + 1]);
    numBytesRead = 0;
    while (numBytesRead != length) {
        ssize_t oldBytes = numBytesRead;
        numBytesRead += read(fd, bytes.get() + numBytesRead, length - numBytesRead);
        if (oldBytes == numBytesRead) {
            break;
        }
        if (oldBytes > numBytesRead) {
            numBytesRead++;
            break;
        }
    }

    if (numBytesRead != length) {
        printf("Error - blocking read failed. %jd read %jd attempted",
               (intmax_t)numBytesRead, (intmax_t)length);
        fflush(stdout);
        assert(false);
        exit(-1);
    }

    // null terminate
    bytes[length] = '\0';

    // need to return a string
    return std::string(bytes.get());

}

std::string VoltDBIPC::decodeBase64AndDecompress(const std::string& base64Data) {
    const size_t messageSize = sizeof(int8_t) + sizeof(int32_t) + base64Data.size();
    char message[messageSize];
    size_t offset = 0;

    message[0] = static_cast<char>(ERRORCODE_NEED_DECODE_BASE64_AND_DECOMPRESS);
    offset++;

    *reinterpret_cast<int32_t*>(&message[offset]) = htonl(static_cast<int32_t>(base64Data.size()));
    offset += sizeof(int32_t);

    ::memcpy(&message[offset], base64Data.c_str(), base64Data.size());

    writeOrDie(m_fd, message, messageSize);

    return readLengthPrefixedBytesToStdString(m_fd);
}

/**
 * Retrieve a plan from Java via the IPC connection for a fragment id.
 * Plan is JSON. Returns the empty string on failure, but failure is
 * probably going to be detected somewhere else.
 */
std::string VoltDBIPC::planForFragmentId(int64_t fragmentId) {
    IpcResponseBuilder<ERRORCODE_NEED_PLAN, 9> message;
    message.append(fragmentId).writeOrDie(m_fd);

    return readLengthPrefixedBytesToStdString(m_fd);
}

//FIXME: There are fatal conditions detected above like (!m_engine) that weakly
// return a nondescript ERRORCODE_ERROR when they should really just crash.
// VoltDBIPC should be able to trigger a crash in these cases without
// constructing a dummy FatalException -- at least not explicitly.
// So, there should be a no-argument version of VoltDBIPC::crashVoltDB.
//FIXME: The signature for this function established in Topend should use
// "const FatalException&" to not require passing the structure by value.
// It's more a flexibility/correctness issue than a performance issue.
// FatalException subtypes may want to define state and bahavior that would be
// useful in these methods. They would get lost in the copy-construction of a
// generic base FatalException instance.
void VoltDBIPC::crashVoltDB(FatalException e) {
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
    m_reusedResultBuffer[0] = static_cast<char>(ERRORCODE_CRASH_VOLTDB);
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

    writeOrDie(m_fd, m_reusedResultBuffer, 5 + messageLength);
    exit(-1);
}

void VoltDBIPC::getStats(struct ipc_command *cmd) {
    get_stats_cmd *getStatsCommand = (get_stats_cmd*) cmd;

    const int32_t selector = ntohl(getStatsCommand->selector);
    const int32_t numLocators = ntohl(getStatsCommand->num_locators);
    bool interval = (getStatsCommand->interval != 0);
    const int64_t now = ntohll(getStatsCommand->now);
    int32_t *locators = new int32_t[numLocators];
    for (int ii = 0; ii < numLocators; ii++) {
        locators[ii] = ntohl(getStatsCommand->locators[ii]);
    }

    m_engine->resetReusedResultOutputBuffer(1); // reserve 1 byte to add status code

    try {
        int result = m_engine->getStats(
                static_cast<int>(selector),
                locators,
                numLocators,
                interval,
                now);

        delete [] locators;

        // write the results array back across the wire
        if (result == 1) {
            sendSerializedResult();
        }
        else if (result == 0) {
            int32_t zero = 0;
            sendPrimitiveResult(zero);
        }
        else {
            sendSerializedException(ERRORCODE_ERROR);
        }
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
}

void VoltDBIPC::activateTableStream(struct ipc_command *cmd) {
    activate_tablestream *activateTableStreamCommand = (activate_tablestream*) cmd;
    const CatalogId tableId = ntohl(activateTableStreamCommand->tableId);
    const TableStreamType streamType =
            static_cast<TableStreamType>(ntohl(activateTableStreamCommand->streamType));

    // Provide access to the serialized message data, i.e. the predicates.
    void* offset = activateTableStreamCommand->data;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(activate_tablestream));
    int64_t undoToken = ntohll(activateTableStreamCommand->undoToken);
    ReferenceSerializeInputBE serialize_in(offset, sz);

    try {
        bool result = m_engine->activateTableStream(tableId, streamType, undoToken, serialize_in);
        sendPrimitiveResult(result);
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
}

void VoltDBIPC::tableStreamSerializeMore(struct ipc_command *cmd) {
    tablestream_serialize_more *tableStreamSerializeMore = (tablestream_serialize_more*) cmd;
    const CatalogId tableId = ntohl(tableStreamSerializeMore->tableId);
    const TableStreamType streamType =
            static_cast<TableStreamType>(ntohl(tableStreamSerializeMore->streamType));
    // Need to adapt the simpler incoming data describing buffers to conform to
    // what VoltDBEngine::tableStreamSerializeMore() needs. The incoming data
    // is an array of buffer lengths. The outgoing data must be an array of
    // ptr/offset/length triplets referencing segments of m_tupleBuffer, which
    // is reallocated as needed.
    const int bufferCount = ntohl(tableStreamSerializeMore->bufferCount);
    try {

        if (bufferCount <= 0) {
            throwFatalException("Bad buffer count in tableStreamSerializeMore: %d", bufferCount);
        }

        // Need two passes, one to determine size, the other to populate buffer
        // data. Can't do this until the base buffer is properly allocated.
        // Note that m_reusedResultBuffer is used for input data and
        // m_tupleBuffer is used for output data.

        void *inptr = tableStreamSerializeMore->data;
        int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(tablestream_serialize_more));
        ReferenceSerializeInputBE in1(inptr, sz);

        // Pass 1 - calculate size and allow for status code byte and count length integers.
        size_t outputSize = 1;
        for (size_t i = 0; i < bufferCount; i++) {
            in1.readLong(); in1.readInt(); // skip address and offset, used for jni only
            outputSize += in1.readInt() + 4;
        }

        // Reallocate buffer as needed.
        // Avoid excessive thrashing by over-allocating in powers of 2.
        if (outputSize > m_tupleBufferSize) {
            while (outputSize > m_tupleBufferSize) {
                m_tupleBufferSize *= 2;
            }
            delete [] m_tupleBuffer;
            m_tupleBuffer = new char[m_tupleBufferSize];
        }

        // Pass 2 - rescan input stream and generate final buffer data.
        ReferenceSerializeInputBE in2(inptr, sz);
        // 1 byte status and 4 byte count
        size_t offset = 5;
        ReferenceSerializeOutput out1(m_reusedResultBuffer, MAX_MSG_SZ);
        out1.writeInt(bufferCount);
        for (size_t i = 0; i < bufferCount; i++) {
            in2.readLong(); in2.readInt(); // skip address and offset, used for jni only
            int length = in2.readInt();
            out1.writeLong((long)m_tupleBuffer);
            // Allow for the length int written later.
            offset += sizeof(int);
            out1.writeInt(static_cast<int>(offset));
            out1.writeInt(length);
            offset += length;
        }

        // Perform table stream serialization.
        ReferenceSerializeInputBE out2(m_reusedResultBuffer, MAX_MSG_SZ);
        std::vector<int> positions;
        int64_t remaining = m_engine->tableStreamSerializeMore(tableId, streamType, out2, positions);

        // Finalize the tuple buffer by adding the status code, buffer count,
        // and remaining tuple count.
        // Inject positions (lengths) into previously skipped int-size gaps.
        m_tupleBuffer[0] = static_cast<char>(ERRORCODE_SUCCESS);
        *reinterpret_cast<int32_t*>(&m_tupleBuffer[1]) = htonl(bufferCount);
        offset = 1 + sizeof(int32_t);
        *reinterpret_cast<int64_t*>(&m_tupleBuffer[offset]) = htonll(remaining);
        offset += sizeof(int64_t);
        if (remaining > 0) {
            std::vector<int>::const_iterator ipos;
            for (ipos = positions.begin(); ipos != positions.end(); ++ipos) {
                int length = *ipos;
                *reinterpret_cast<int32_t*>(&m_tupleBuffer[offset]) = htonl(length);
                offset += length + sizeof(int32_t);
            }
        } else {
            // If we failed or finished, we've set the count, so stop right there.
            outputSize = offset;
        }

        // Ship it.
        writeOrDie(m_fd, m_tupleBuffer, outputSize);

    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
}

void VoltDBIPC::processRecoveryMessage( struct ipc_command *cmd) {
    recovery_message *recoveryMessage = (recovery_message*) cmd;
    const int32_t messageLength = ntohl(recoveryMessage->messageLength);
    ReferenceSerializeInputBE input(recoveryMessage->message, messageLength);
    RecoveryProtoMsg message(&input);
    m_engine->processRecoveryMessage(&message);
    sendSimpleSuccess();
}

void VoltDBIPC::tableHashCode( struct ipc_command *cmd) {
    table_hash_code *hashCodeRequest = (table_hash_code*) cmd;
    const int32_t tableId = ntohl(hashCodeRequest->tableId);
    int64_t tableHashCode = m_engine->tableHashCode(tableId);
    sendPrimitiveResult(tableHashCode);
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
    sendPrimitiveResult(result);
}

void VoltDBIPC::getUSOForExportTable(struct ipc_command *cmd) {
    get_uso *get = (get_uso*)cmd;

    int32_t tableSignatureLength = ntohl(get->tableSignatureLength);
    std::string tableSignature(get->tableSignature, tableSignatureLength);

    size_t ackOffset;
    int64_t seqNo;
    m_engine->getUSOForExportTable(ackOffset, seqNo, tableSignature);

    int64_t ackOffsetI64 = static_cast<int64_t>(ackOffset);
    IpcResponseBuilder<ERRORCODE_SUCCESS, 1 + 2*sizeof(int64_t)> response;
    response.append(ackOffsetI64).append(seqNo).writeOrDie(m_fd);
}

void VoltDBIPC::hashinate(struct ipc_command* cmd) {
    hashinate_msg* hash = (hashinate_msg*)cmd;
    NValueArray& params = m_engine->getParameterContainer();

    HashinatorType hashinatorType = static_cast<HashinatorType>(ntohl(hash->hashinatorType));
    int32_t configLength = ntohl(hash->configLength);
    boost::scoped_ptr<TheHashinator> hashinator;
    switch (hashinatorType) {
    case HASHINATOR_LEGACY:
        hashinator.reset(LegacyHashinator::newInstance(hash->data));
        break;
    case HASHINATOR_ELASTIC:
        hashinator.reset(ElasticHashinator::newInstance(hash->data, NULL, 0));
        break;
    default:
        try {
            throwFatalException("Unrecognized hashinator type %d", hashinatorType);
        } catch (const FatalException &e) {
            crashVoltDB(e);
        }
    }
    void* offset = hash->data + configLength;
    int sz = static_cast<int> (ntohl(cmd->msgsize) - sizeof(hash));
    ReferenceSerializeInputBE serialize_in(offset, sz);

    try {
        int cnt = serialize_in.readShort();
        assert(cnt> -1);
        Pool *pool = m_engine->getStringPool();
        deserializeParameterSetCommon(cnt, serialize_in, params, pool);
        int retval = hashinator->hashinate(params[0]);
        pool->purge();
        sendPrimitiveResult(retval);
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }

}

void VoltDBIPC::updateHashinator(struct ipc_command *cmd) {
    hashinate_msg* hash = (hashinate_msg*)cmd;

    HashinatorType hashinatorType = static_cast<HashinatorType>(ntohl(hash->hashinatorType));
    try {
        m_engine->updateHashinator(hashinatorType, hash->data, NULL, 0);
        sendSimpleSuccess();
    } catch (const FatalException &e) {
        crashVoltDB(e);
    }
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
    sendPrimitiveResult(poolAllocations);
}

int64_t VoltDBIPC::getQueuedExportBytes(int32_t partitionId, std::string signature) {
    m_reusedResultBuffer[0] = ERRORCODE_NEED_QUEUED_EXPORT_BYTES_STAT;
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[1]) = htonl(partitionId);
    *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[5]) = htonl(static_cast<int32_t>(signature.size()));
    ::memcpy( &m_reusedResultBuffer[9], signature.c_str(), signature.size());
    writeOrDie(m_fd, m_reusedResultBuffer, 9 + signature.size());

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
        StreamBlock *block,
        bool sync,
        bool endOfStream) {
    int32_t index = 0;
    m_reusedResultBuffer[index++] = ERRORCODE_NEED_BUFFER_EXPORT;
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
        writeOrDie(m_fd, m_reusedResultBuffer, index + 4);
        // Memset the first 8 bytes to initialize the MAGIC_HEADER_SPACE_FOR_JAVA
        ::memset(block->rawPtr(), 0, 8);
        writeOrDie(m_fd, block->rawPtr(), block->rawLength());
        // Need the delete in the if statement for valgrind
        delete [] block->rawPtr();
    } else {
        *reinterpret_cast<int32_t*>(&m_reusedResultBuffer[index]) = htonl(0);
        writeOrDie(m_fd, m_reusedResultBuffer, index + 4);
    }
}

void VoltDBIPC::executeTask(struct ipc_command *cmd) {
    execute_task *task = (execute_task*)cmd;
    voltdb::TaskType taskId = static_cast<voltdb::TaskType>(ntohll(task->taskId));
    m_engine->resetReusedResultOutputBuffer(1); // reserve 1 byte to add status code
    try {
        m_engine->executeTask(taskId, task->task);
        sendSerializedResult();
    } catch (const FatalException& e) {
        crashVoltDB(e);
    }
}

void VoltDBIPC::applyBinaryLog(struct ipc_command *cmd) {
    try {
        apply_binary_log *params = (apply_binary_log*)cmd;
        m_engine->applyBinaryLog(ntohll(params->txnId),
                                 ntohll(params->spHandle),
                                 ntohll(params->lastCommittedSpHandle),
                                 ntohll(params->uniqueId),
                                 ntohll(params->undoToken),
                                 params->log);
        sendSimpleSuccess();
    } catch (const FatalException& e) {
        crashVoltDB(e);
    }
}

void VoltDBIPC::pushDRBuffer(int32_t partitionId, voltdb::StreamBlock *block) {
    if (block != NULL) {
        delete[] block->rawPtr();
    }
}

void VoltDBIPC::fallbackToEEAllocatedBuffer(char *buffer, size_t length) { }

static bool verbose = false;

void *eethread(void *ptr) {
    // copy and free the file descriptor ptr allocated by the select thread
    int *fdPtr = static_cast<int*>(ptr);
    int fd = *fdPtr;
    delete fdPtr;
    fdPtr = NULL;

    /* max message size that can be read from java */
    int max_ipc_message_size = (1024 * 1024 * 2);

    // requests larger than this will cause havoc.
    // cry havoc and let loose the dogs of war
    boost::shared_array<char> data(new char[max_ipc_message_size]);
    memset(data.get(), 0, max_ipc_message_size);

    // instantiate voltdbipc to interface to EE.
    boost::scoped_ptr<VoltDBIPC> voltipc(new VoltDBIPC(fd));

    // loop until disconnected
    // or until a profiling run has set the early termination flag.
    while (true) {
        size_t bytesread = 0;

        // read the header
        while (bytesread < 4) {
            std::size_t b = read(fd, data.get() + bytesread, 4 - bytesread);
            if (b == 0) {
                if (verbose) {
                    printf("expectable connection eof reading header\n");
                }
                return NULL;
            }
            if (b == -1) {
                printf("unexpected connection read error reading header\n");
                return NULL;
            }
            bytesread += b;
        }

        // read the message body into the reusable data buffer
        int msg_size = ntohl(((ipc_command*) data.get())->msgsize);
        if (verbose) { printf("Received message size %d\n", msg_size); }
        if (msg_size > max_ipc_message_size) {
            max_ipc_message_size = msg_size;
            char* newdata = new char[max_ipc_message_size];
            memset(newdata, 0, max_ipc_message_size);
            memcpy(newdata, data.get(), 4);
            data.reset(newdata);
        }

        while (bytesread < msg_size) {
            std::size_t b = read(fd, data.get() + bytesread, msg_size - bytesread);
            if (b == 0) {
                printf("unexpected connection eof reading message after %jd of %d expected message bytes\n",
                       bytesread, msg_size);
                return NULL;
            }
            if (b == -1) {
                printf("unexpected connection read error reading message after %jd of %d expected message bytes\n",
                       bytesread, msg_size);
                return NULL;
            }
            bytesread += b;
        }

        // dispatch the request
        ipc_command *cmd = (ipc_command*) data.get();

        // size at least length + command
        if (ntohl(cmd->msgsize) < sizeof(ipc_command)) {
            printf("bytesread=%zx cmd=%d msgsize=%d\n",
                   bytesread, cmd->command, ntohl(cmd->msgsize));
            for (int ii = 0; ii < bytesread; ++ii) {
                printf("%x ", data[ii]);
            }
            assert(ntohl(cmd->msgsize) >= sizeof(struct ipc_command));
        }
        voltipc->execute(cmd);
        if (::terminateEarlyForProfiling) {
            return NULL;
        }
    }
    // dead code path
    return NULL;
}

int main(int argc, char **argv) {
    //Create a pool ref to init the thread local in case a poll message comes early
    voltdb::ThreadLocalPool poolRef;
    const int pid = getpid();
    printf("== pid = %d ==\n", pid);
    fflush(stdout);
    int sock = -1;
    int fd = -1;

    int eecount = 1;
    int port = 0; // 0 means pick any port

    // allow caller to specify the number of ees - defaults to 1
    if (argc > 1) {
        char *eecountStr = argv[1];
        assert(eecountStr);
        eecount = atoi(eecountStr);
        assert(eecount >= 0);
        printf("== thread count = %d ==\n", eecount);
    }

    boost::shared_array<pthread_t> eeThreads(new pthread_t[eecount]);

    // allow caller to override port with the second argument
    if (argc > 2) {
        char *portStr = argv[2];
        assert(portStr);
        port = atoi(portStr);
        assert(port > 0);
        assert(port <= 65535);
    }

    // allow verbose with the third argument
    if (argc > 3) {
        char* verboseOption = argv[3];
        assert(*verboseOption == 'v');
        if (verboseOption) {
            printf("== verbose option = %s (verbose?) ==\n", verboseOption);
            verbose = true;
        }
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

    // connect to each Site from Java over a new socket
    for (int ee = 0; ee < eecount; ee++) {
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

        // make a heap file descriptor to pass to the thread (which it will free)
        int *fdPtr = new int;
        *fdPtr = fd;

        int status = pthread_create(&eeThreads[ee], NULL, eethread, fdPtr);
        if (status) {
            // error
        }
    }

    close(sock);

    // wait for all of the EEs to finish
    for (int ee = 0; ee < eecount; ee++) {
        int code = pthread_join(eeThreads[ee], NULL);
        // stupid if to avoid compiler warning
        if (code != 0) {
            assert(code == 0);
        }
    }

    fflush(stdout);
    return 0;
}
