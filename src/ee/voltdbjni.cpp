/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * @defgroup jni The JNI entry points for local execution engine written in C++.
 * An execution engine object is VoltDBEngine* created by create() method.
 * Every JNI method requires the VoltDBEngine* as a parameter instead
 * of having any static (it's simpler and safer, isn't it?).
 * For more details, see the package Javadoc of org.voltdb.jni.
 * @{
*/

#include <string>
#include <vector>
#include <signal.h>
#include <dlfcn.h>
#ifdef LINUX
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>
#ifndef __USE_GNU
#define  __USE_GNU
#endif // __USE_GNU
#include <sched.h>
#include <cerrno>
#include <stdlib.h>
#endif // LINUX
#ifdef MACOSX
#include <mach/task.h>
#include <mach/mach.h>
#endif // MACOSX

// Print an error if trying to compile on 32-bit systemes.
#ifdef LINUX
#if __SIZEOF_POINTER__ == 4
#error VoltDB server does not compile or run on 32-bit platforms. The Java client library does (ant jars)
#endif // __SIZEOF_POINTER__ == 4
#else
#ifndef __x86_64
#error VoltDB server does not compile or run on 32-bit platforms. The Java client library does (ant jars)
#endif // __x86_64
#endif // LINUX

#ifdef PROFILE_ENABLED
#include <google/profiler.h>
#endif

//#include <jni/jni.h>
// TODO: gcc picks up wrong jni_md.h and results in compile error (bad
// declaration of jboolean) if I include the jni.h in externals.
// Can we assume jni.h? or still better to have jni.h in externals?
#include <jni.h>

#include "org_voltdb_jni_ExecutionEngine.h" // the header file output by javah
#include "org_voltcore_utils_DBBPool.h" //Utility method for DBBContainer
#include "org_voltdb_utils_PosixAdvise.h" //Utility method for invoking madvise/fadvise
#include "org_voltdb_utils_DirectIoFileChannel.h" //Utility methods for working with direct IO

#include "boost/shared_ptr.hpp"
#include "boost/scoped_array.hpp"
#include "boost/ptr_container/ptr_vector.hpp"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/TheHashinator.h"
#include "common/Pool.hpp"
#include "common/FatalException.hpp"
#include "common/SegvException.hpp"
#include "common/ElasticHashinator.h"
#include "common/ThreadLocalPool.h"
#include "storage/DRTupleStream.h"
#include "murmur3/MurmurHash3.h"
#include "execution/VoltDBEngine.h"
#include "execution/JNITopend.h"
#include "boost/pool/pool.hpp"
#include "crc/crc32c.h"
#include "boost/crc.hpp"
#include "logging/JNILogProxy.h"

#include "logging/LogDefs.h"
#include "logging/Logger.h"

// Defines a function as a JNIEXPORT for a shared library
#define SHAREDLIB_JNIEXPORT __attribute__((visibility("default"))) JNIEXPORT

using namespace std;
using namespace voltdb;

#define castToEngine(x) reinterpret_cast<VoltDBEngine*>((x));\
    currentEngine = reinterpret_cast<VoltDBEngine*>((x))
#define updateJNILogProxy(x) const_cast<JNILogProxy*>(dynamic_cast<const JNILogProxy*>(x->getLogManager()->getLogProxy()))->setJNIEnv(env)

/*
 * Yes, I know. This is ugly. But in order to get some useful information and
 * properly call crashVoltDB when a signal is caught, I cannot think of a
 * different way.
 */
static VoltDBEngine *currentEngine = NULL;
static JavaVM *currentVM = NULL;
static jfieldID field_fd;

void signalHandler(int signum, siginfo_t *info, void *context) {
    if (currentVM == NULL || currentEngine == NULL)
        return;

    char err_msg[128];
    snprintf(err_msg, sizeof err_msg, "SIGSEGV caught: signal number %d, error value %d,"
            " signal code %d\n\n", info->si_signo, info->si_errno, info->si_code);
    err_msg[sizeof err_msg - 1] = '\0';
    std::string message = err_msg;
    message.append(currentEngine->debug());

    JNIEnv *env;
    if (currentVM->AttachCurrentThread((void **)(void *)&env, NULL) != 0)
        exit(-1);
    Topend *topend = static_cast<JNITopend*>(currentEngine->getTopend())->updateJNIEnv(env);
    topend->crashVoltDB(SegvException(message.c_str(), context, __FILE__, __LINE__));
    currentVM->DetachCurrentThread();
}

void setupSigHandler(void) {
#ifdef __linux__
    struct sigaction action;
    struct sigaction orig_action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = signalHandler;
    action.sa_flags = SA_SIGINFO;
    if(sigaction(SIGSEGV, &action, &orig_action) < 0)
        return;
    /*
     * This detects if we are running in Sun's JVM and libjsig.so is loaded. The
     * trick is that the interposed version of sigaction() returns the same
     * signal handler as the one we passed in, the original version of
     * sigaction() returns the old signal handler in place before we put in
     * ours. So here we check if the returned one is ours.
     */
    if (orig_action.sa_sigaction != NULL
        && orig_action.sa_sigaction != signalHandler)
        sigaction(SIGSEGV, &orig_action, NULL);
#endif
}

////////////////////////////////////////////////////////////////////////////
// Create / Destroy
////////////////////////////////////////////////////////////////////////////
/**
 * Just creates a new VoltDBEngine object and returns it to Java.
 * Never fail to destroy() for the VoltDBEngine* once you call this method
 * NOTE: Call initialize() separately for initialization.
 * This does strictly nothing so that this method never throws an exception.
 * @return the created VoltDBEngine pointer casted to jlong.
*/
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeCreate(JNIEnv *env, jobject obj, jboolean isSunJVM) {
    // obj is the instance pointer of the ExecutionEngineJNI instance
    // that is creating this native EE. Turn this into a global reference
    // and only use that global reference for calling back to Java.
    // The jobject parameter here (and in all the other native interfaces)
    // is a local reference and only valid until the return of the
    // native invocation. Since calling patterns like java->ee->java->ee-
    // exist, the local jobject pointers are basically uncacheable. The
    // second java->ee call may generate a new local reference that would
    // be invalid in the previous stack frames (after the return of the
    // last ee native call.)

    jobject java_ee = env->NewGlobalRef(obj);
    if (java_ee == NULL) {
        vassert(!"Failed to allocate global reference to java EE.");
        throw std::exception();
        return 0;
    }
    JavaVM *vm;
    env->GetJavaVM(&vm);
    currentVM = vm;
    if (isSunJVM == JNI_TRUE)
        setupSigHandler();
    // retrieving the fieldId fd of FileDescriptor for later use
    jclass class_fdesc = env->FindClass("java/io/FileDescriptor");
    if (class_fdesc == NULL) {
        vassert(!"Failed to find filed if of FileDescriptor.");
        throw std::exception();
        return 0;
    }
    // poke the "fd" field with the file descriptor
    field_fd = env->GetFieldID(class_fdesc, "fd", "I");

    JNITopend *topend = NULL;
    VoltDBEngine *engine = NULL;
    try {
        topend = new JNITopend(env, java_ee);
        engine = new VoltDBEngine( topend, JNILogProxy::getJNILogProxy(env, vm));
    } catch (const FatalException &e) {
        if (topend != NULL) {
            topend->crashVoltDB(e);
        }
    }
    return reinterpret_cast<jlong>(engine);
}
/**
 * Releases all resources held in the execution engine.
 * @param engine_ptr the VoltDBEngine pointer to be destroyed
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDestroy(
    JNIEnv *env, jobject obj,
    jlong engine_ptr) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    JNITopend* topend = static_cast<JNITopend*>(engine->getTopend());
    topend->updateJNIEnv(env);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    delete engine;
    delete topend;
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
}

/**
 * decommision the execution engine.
 * @param engine_ptr the VoltDBEngine pointer
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDecommission(
    JNIEnv *env, jobject obj, jlong engine_ptr,
    jboolean remove, jboolean promote, jint newSitePerHost) {
    VOLT_DEBUG("nativeDecommission() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        VOLT_ERROR("engine_ptr was NULL or invalid pointer");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);

    //copy to std::string. utf_chars may or may not by a copy of the string
    VOLT_DEBUG("calling decommission...");

    try {
        bool success = engine->decommission(remove, promote, newSitePerHost);

        if (success) {
            VOLT_DEBUG("decommission succeeded");
            return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
        }
    } catch (const SerializableEEException &e) {
        engine->resetReusedResultOutputBuffer();
        e.serialize(engine->getExceptionOutputSerializer());
    } catch (const FatalException &fe) {
        if (topend != NULL) {
            topend->crashVoltDB(fe);
        }
    }

    VOLT_ERROR("decommission failed");
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

////////////////////////////////////////////////////////////////////////////
// Initialization
////////////////////////////////////////////////////////////////////////////
/**
 * Initializes the execution engine with given parameter.
 * @param enginePtr the VoltDBEngine pointer to be initialized
 * @param clusterId id of the cluster the execution engine belongs to
 * @param nodeId this id will be set to the execution engine
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeInitialize(
    JNIEnv *env, jobject obj,
    jlong enginePtr,
    jint clusterIndex,
    jlong siteId,
    jint partitionId,
    jint sitesPerHost,
    jint hostId,
    jbyteArray hostname,
    jint drClusterId,
    jint defaultDrBufferSize,
    jboolean drIgnoreConflicts,
    jint drCrcErrorIgnoreMax,
    jboolean drCrcErrorIgnoreFatal,
    jlong tempTableMemory,
    jboolean createDrReplicatedStream,
    jint compactionThreshold)
{
    VOLT_DEBUG("nativeInitialize() start");
    VoltDBEngine *engine = castToEngine(enginePtr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        VOLT_ERROR("engine_ptr was NULL or invalid pointer");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated

        jbyte *hostChars = env->GetByteArrayElements( hostname, NULL);
        std::string hostString(reinterpret_cast<char*>(hostChars), env->GetArrayLength(hostname));
        env->ReleaseByteArrayElements( hostname, hostChars, JNI_ABORT);
        // initialization is separated from constructor so that constructor
        // never fails.
        VOLT_DEBUG("calling initialize...");
        engine->initialize(clusterIndex,
                           siteId,
                           partitionId,
                           sitesPerHost,
                           hostId,
                           hostString,
                           drClusterId,
                           defaultDrBufferSize,
                           drIgnoreConflicts,
                           drCrcErrorIgnoreMax,
                           drCrcErrorIgnoreFatal,
                           tempTableMemory,
                           createDrReplicatedStream,
                           static_cast<int32_t>(compactionThreshold));
        VOLT_DEBUG("initialize succeeded");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    }
    catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * Load the system catalog for this engine.
 * @param engine_ptr the VoltDBEngine pointer
 * @param serialized_catalog the root catalog object serialized as text strings.
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeLoadCatalog(
    JNIEnv *env, jobject obj,
    jlong engine_ptr, jlong timestamp, jbyteArray serialized_catalog) {
    VOLT_DEBUG("nativeLoadCatalog() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        VOLT_ERROR("engine_ptr was NULL or invalid pointer");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }

    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);

    //copy to std::string. utf_chars may or may not by a copy of the string
    jbyte * utf_chars = env->GetByteArrayElements(serialized_catalog, NULL);
    string str(reinterpret_cast<char*>(utf_chars), env->GetArrayLength(serialized_catalog));
    env->ReleaseByteArrayElements(serialized_catalog, utf_chars, JNI_ABORT);
    VOLT_DEBUG("calling loadCatalog...");

    try {
        bool success = engine->loadCatalog(timestamp, str);

        if (success) {
            VOLT_DEBUG("loadCatalog succeeded");
            return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
        }
    } catch (const SerializableEEException &e) {
        engine->resetReusedResultOutputBuffer();
        e.serialize(engine->getExceptionOutputSerializer());
    } catch (const FatalException &fe) {
        if (topend != NULL) {
            topend->crashVoltDB(fe);
        }
    }

    VOLT_ERROR("loadCatalog failed");
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * Load the system catalog for this engine.
 * @param engine_ptr the VoltDBEngine pointer
 * @param serialized_catalog the root catalog object serialized as text strings.
 * human-readable text strings separated by line feeds.
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL
Java_org_voltdb_jni_ExecutionEngine_nativeUpdateCatalog(
    JNIEnv *env, jobject obj,
    jlong engine_ptr, jlong timestamp, jboolean streamChanges, jbyteArray catalog_diffs) {
    VOLT_DEBUG("nativeUpdateCatalog() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        VOLT_ERROR("engine_ptr was NULL or invalid pointer");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }

    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);

    //copy to std::string. utf_chars may or may not by a copy of the string
    jbyte* utf_chars = env->GetByteArrayElements(catalog_diffs, NULL);
    string str(reinterpret_cast<char*>(utf_chars), env->GetArrayLength(catalog_diffs));
    env->ReleaseByteArrayElements(catalog_diffs, utf_chars, JNI_ABORT);
    VOLT_DEBUG("calling loadCatalog...");

    try {
        bool success = engine->updateCatalog( timestamp, streamChanges, str);

        if (success) {
            VOLT_DEBUG("updateCatalog succeeded");
            return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
        }
    } catch (const SerializableEEException &e) {
        engine->resetReusedResultOutputBuffer();
        e.serialize(engine->getExceptionOutputSerializer());
    } catch (const FatalException &fe) {
        if (topend != NULL) {
            topend->crashVoltDB(fe);
        }
    }

    VOLT_ERROR("updateCatalog failed");
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * This method is called to initially load table data from a heap array
 * @param pointer the VoltDBEngine pointer
 * @param table_id catalog ID of the table
 * @param serialized_table the table data to be loaded
 * @param txnId ID of current transaction
 * @param spHandle for this sp transaction
 * @param lastCommittedSpHandle spHandle which was most recently committed
 * @param uniqueId for this transaction
 * @param undoToken for this transaction
 * @param callerId ID for LoadTableCaller enum
*/
SHAREDLIB_JNIEXPORT jint JNICALL
Java_org_voltdb_jni_ExecutionEngine_nativeLoadTable__JI_3BJJJJJB (
    JNIEnv *env, jobject obj, jlong engine_ptr, jint table_id,
    jbyteArray serialized_table, jlong txnId, jlong spHandle, jlong lastCommittedSpHandle,
    jlong uniqueId, jlong undoToken, jbyte callerId)
{
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }

    engine->resetReusedResultOutputBuffer();

    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);
    VOLT_DEBUG("loading table %d in C++ on thread %d", table_id, ThreadLocalPool::getThreadPartitionId());

    // deserialize dependency.
    jsize length = env->GetArrayLength(serialized_table);
    VOLT_DEBUG("deserializing %d bytes on thread %d", (int) length, ThreadLocalPool::getThreadPartitionId());
    jbyte *bytes = env->GetByteArrayElements(serialized_table, NULL);
    ReferenceSerializeInputBE serialize_in(bytes, length);
    try {
        try {
            bool success = engine->loadTable(table_id, serialize_in, txnId,
                                             spHandle, lastCommittedSpHandle, uniqueId, undoToken,
                                             LoadTableCaller::get(static_cast<LoadTableCaller::Id>(callerId)));
            env->ReleaseByteArrayElements(serialized_table, bytes, JNI_ABORT);
            VOLT_DEBUG("deserialized table");

            if (success)
                return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
        } catch (const SerializableEEException &e) {
            engine->resetReusedResultOutputBuffer();
            e.serialize(engine->getExceptionOutputSerializer());
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }

    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * This method is called to initially load table data from a direct byte buffer
 * @param engine_ptr the VoltDBEngine pointer
 * @param table_id catalog ID of the table
 * @param serialized_table the table data to be loaded as a direct byte buffer
 * @param txnId ID of current transaction
 * @param spHandle for this sp transaction
 * @param lastCommittedSpHandle spHandle which was most recently committed
 * @param uniqueId for this transaction
 * @param undoToken for this transaction
 * @param callerId ID for LoadTableCaller enum
*/
SHAREDLIB_JNIEXPORT jint JNICALL
Java_org_voltdb_jni_ExecutionEngine_nativeLoadTable__JILjava_nio_ByteBuffer_2JJJJJB (
    JNIEnv *env, jobject obj, jlong engine_ptr, jint table_id,
    jobject serialized_table, jlong txnId, jlong spHandle, jlong lastCommittedSpHandle,
    jlong uniqueId, jlong undoToken, jbyte callerId)
{
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }

    engine->resetReusedResultOutputBuffer();

    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);
    VOLT_DEBUG("loading table %d in C++ on thread %d", table_id, ThreadLocalPool::getThreadPartitionId());

    char *bytes = static_cast<char *>(env->GetDirectBufferAddress(serialized_table));
    jlong length = env->GetDirectBufferCapacity(serialized_table);
    VOLT_DEBUG("deserializing %d bytes on thread %d", (int) length, ThreadLocalPool::getThreadPartitionId());
    ReferenceSerializeInputBE serialize_in(bytes, length);

    try {
        bool success = engine->loadTable(table_id, serialize_in, txnId, spHandle, lastCommittedSpHandle, uniqueId,
                undoToken, LoadTableCaller::get(static_cast<LoadTableCaller::Id>(callerId)));
        VOLT_DEBUG("deserialized table");

        if (success)
            return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    } catch (const SerializableEEException &e) {
        engine->resetReusedResultOutputBuffer();
        e.serialize(engine->getExceptionOutputSerializer());
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }

    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}


////////////////////////////////////////////////////////////////////////////
// PlanNode Execution
////////////////////////////////////////////////////////////////////////////
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

/**
 * Utility used for deserializing ParameterSet passed from Java.
 */
int deserializeParameterSet(const char* serialized_parameterset, jint serialized_length,
    NValueArray &params, Pool *stringPool) {
    // deserialize parameters as ValueArray.
    // We don't use SerializeIO here because it makes a copy.
    ReferenceSerializeInputBE serialize_in(serialized_parameterset, serialized_length);

    // see org.voltdb.ParameterSet.
    // TODO : make it a class. later, later, later...
    int cnt = serialize_in.readShort();
    if (cnt < 0) {
        throwFatalException( "parameter count is negative: %d", cnt);
    }
    vassert(cnt <= MAX_PARAM_COUNT);
    deserializeParameterSetCommon(cnt, serialize_in, params, stringPool);
    return cnt;
}

/**
 * Sets (or re-sets) the buffer shared between java and the EE. This is for reducing
 * cost of GetDirectBufferAddress().
 * @param engine_ptr the VoltDBEngine pointer
 * @param parameter_buffer direct byte buffer to be set
 * @param m_parameterBuffersize size of the buffer
 * @param per_fragment_stats_buffer direct byte buffer to be set
 * @param per_fragment_stats_buffer_size size of the buffer
 * @param first_result_buffer direct byte buffer to be set
 * @param first_result_buffer_size size of the buffer
 * @param next_result_buffer direct byte buffer to be set
 * @param next_result_buffer_size size of the buffer
 * @param exception_buffer direct byte buffer to be set
 * @param exception_buffer_size size of the buffer
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetBuffers
  (JNIEnv *env, jobject obj, jlong engine_ptr,
    jobject parameter_buffer,          jint parameter_buffer_size,
    jobject per_fragment_stats_buffer, jint per_fragment_stats_buffer_size,
    jobject udf_buffer,                jint udf_buffer_size,
    jobject first_result_buffer,       jint first_result_buffer_size,
    jobject next_result_buffer,        jint next_result_buffer_size,
    jobject exception_buffer,          jint exception_buffer_size)
{
    VOLT_DEBUG("nativeSetBuffers() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated

        char *parameterBuffer = reinterpret_cast<char*>(
                env->GetDirectBufferAddress(parameter_buffer));
        int parameterBufferCapacity = parameter_buffer_size;

        char *perFragmentStatsBuffer = reinterpret_cast<char*>(
                env->GetDirectBufferAddress(per_fragment_stats_buffer));
        int perFragmentStatsBufferCapacity = per_fragment_stats_buffer_size;

        char *udfBuffer = reinterpret_cast<char*>(
                 env->GetDirectBufferAddress(udf_buffer));
        int udfBufferCapacity = udf_buffer_size;

        char *firstReusedResultBuffer = reinterpret_cast<char*>(
                env->GetDirectBufferAddress(first_result_buffer));
        int firstReusedResultBufferCapacity = first_result_buffer_size;

        char *nextReusedResultBuffer = reinterpret_cast<char*>(
                env->GetDirectBufferAddress(next_result_buffer));
        int nextReusedResultBufferCapacity = next_result_buffer_size;

        char *exceptionBuffer = reinterpret_cast<char*>(
                 env->GetDirectBufferAddress(exception_buffer));
        int exceptionBufferCapacity = exception_buffer_size;

        engine->setBuffers(parameterBuffer,         parameterBufferCapacity,
                           perFragmentStatsBuffer,  perFragmentStatsBufferCapacity,
                           udfBuffer,               udfBufferCapacity,
                           firstReusedResultBuffer, firstReusedResultBufferCapacity,
                           nextReusedResultBuffer,  nextReusedResultBufferCapacity,
                           exceptionBuffer,         exceptionBufferCapacity);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }

    return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
}

/**
 * Executes multiple plan fragments with the given parameter sets and gets the results.
 * @param pointer the VoltDBEngine pointer
 * @param plan_fragment_ids ID of the plan fragment to be executed.
 * @param outputBuffer buffer to be filled with the tables.
 * @param outputCapacity maximum number of bytes to write to buffer.
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecutePlanFragments
(JNIEnv *env,
        jobject obj,
        jlong engine_ptr,
        jint batch_index,
        jint num_fragments,
        jlongArray plan_fragment_ids,
        jlongArray input_dep_ids,
        jlong txnId,
        jlong spHandle,
        jlong lastCommittedSpHandle,
        jlong uniqueId,
        jlong undoToken,
        jboolean traceOn)
{
    //VOLT_DEBUG("nativeExecutePlanFragments() start");

    // setup
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        engine->resetPerFragmentStatsOutputBuffer();
        engine->resetReusedResultOutputBuffer(0, batch_index);
        static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);

        // fragment info
        vassert(num_fragments <= MAX_BATCH_COUNT);
        jlong* fragmentIdsBuffer = engine->getBatchFragmentIdsContainer();
        env->GetLongArrayRegion(plan_fragment_ids, 0, num_fragments, fragmentIdsBuffer);

        // if there are dep ids, read them into the buffer after the fragments
        jlong* depIdsBuffer = NULL;
        if (input_dep_ids) {
            depIdsBuffer = engine->getBatchDepIdsContainer();
            env->GetLongArrayRegion(input_dep_ids, 0, num_fragments, depIdsBuffer);
        }

        // all fragments' parameters are in this buffer
        ReferenceSerializeInputBE serialize_in(engine->getParameterBuffer(), engine->getParameterBufferCapacity());

        int failures = engine->executePlanFragments(num_fragments, fragmentIdsBuffer, input_dep_ids ? depIdsBuffer : NULL,
              serialize_in, txnId, spHandle, lastCommittedSpHandle, uniqueId, undoToken, traceOn == JNI_TRUE);
        if (failures > 0) {
            return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
        } else {
            return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * Serialize the result temporary table.
 * @param engine_ptr the VoltDBEngine pointer
 * @param table_id Id of the table to be serialized
 * @return serialized temporary table
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSerializeTable(
        JNIEnv *env,
        jobject obj,
        jlong engine_ptr,
        jint table_id,
        jobject output_buffer,
        jint output_capacity) {
    //VOLT_DEBUG("nativeSerializeTable() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    if (engine == NULL) {
        VOLT_ERROR("The VoltDBEngine pointer is null!");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        void* data = env->GetDirectBufferAddress(output_buffer);
        ReferenceSerializeOutput out(data, output_capacity);
        engine->serializeTable(table_id, out);
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    getBufferCRC32
 * Signature: (Ljava/nio/ByteBuffer;II)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltcore_utils_DBBPool_getBufferCRC32
  (JNIEnv *env, jclass clazz, jobject buffer, jint offset, jint length) {
    char *address = reinterpret_cast<char*>(env->GetDirectBufferAddress(buffer));
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return -1;
    }
    vassert(address);
    boost::crc_32_type crc;
    crc.process_bytes(address + offset, length);
    return static_cast<jint>(crc.checksum());
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    getBufferCRC32
 * Signature: (Ljava/nio/ByteBuffer;II)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltcore_utils_DBBPool_getCRC32
  (JNIEnv *env, jclass clazz, jlong ptr, jint offset, jint length) {
    char *address = reinterpret_cast<char*>(ptr);
    vassert(address);
    boost::crc_32_type crc;
    crc.process_bytes(address + offset, length);
    return static_cast<jint>(crc.checksum());
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    getBufferCRC32C
 * Signature: (Ljava/nio/ByteBuffer;II)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltcore_utils_DBBPool_getBufferCRC32C
  (JNIEnv *env, jclass clazz, jobject buffer, jint offset, jint length) {
    char *address = reinterpret_cast<char*>(env->GetDirectBufferAddress(buffer));
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return -1;
    }
    vassert(address);
    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c( crc, address + offset, length);
    return static_cast<jint>(vdbcrc::crc32cFinish(crc));
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    getBufferCRC32
 * Signature: (Ljava/nio/ByteBuffer;II)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltcore_utils_DBBPool_getCRC32C
  (JNIEnv *env, jclass clazz, jlong ptr, jint offset, jint length) {
    char *address = reinterpret_cast<char*>(ptr);
    vassert(address);
    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c( crc, address + offset, length);
    return static_cast<jint>(vdbcrc::crc32cFinish(crc));
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    getMurmur3128
 * Signature: (JII)J
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltcore_utils_DBBPool_getMurmur3128__JII
  (JNIEnv *env, jclass clazz, jlong ptr, jint offset, jint length) {
    return MurmurHash3_x64_128( reinterpret_cast<char*>(ptr) + offset, length, 0);
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    getMurmur3128
 * Signature: (JII)J
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltcore_utils_DBBPool_getMurmur3128__J
  (JNIEnv *env, jclass clazz, jlong value) {
    return MurmurHash3_x64_128( value );
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTick
 * Signature: (JJJ)V
 *
 * Called roughly every 1 second by the Java Runtime to allow the EE to do
 * periodic non-transactional work.
 *
 * @param env Pointer to the JNIEnv for this thread
 * @param obj Pointer to the object on which this method was called
 * @param engine_ptr Pointer to a VoltDBEngine instance
 * @param timeInMillis The current java timestamp (System.currentTimeMillis());
 * @param lastCommittedSpHandle The id of the last committed transaction.
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTick
  (JNIEnv *env, jobject obj, jlong engine_ptr, jlong timeInMillis, jlong lastCommittedSpHandle) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        engine->tick(timeInMillis, lastCommittedSpHandle);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeQuiesce
 * Signature: (JJ)V
 *
 * Called to instruct the EE to reach an idle steady state.
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeQuiesce
  (JNIEnv *env, jobject obj, jlong engine_ptr, jlong lastCommittedSpHandle)
{
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        // JNIEnv pointer can change between calls, must be updated
        updateJNILogProxy(engine);
        engine->quiesce(lastCommittedSpHandle);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
}

/**
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetStats
 * Signature: (I[I)Z
 *
 * Called to retrieve statistics
 *
 * @param env Pointer to the JNIEnv for this thread
 * @param obj Pointer to the object on which this method was called
 * @param engine_ptr Pointer to a VoltDBEngine instance
 * @param selector Ordinal value from StatisticsSelectorType enum indicating the type of stats to retrieve
 * @param locatorsArray Java array of CatalogIds indicating what set of sources should the statistics be retrieved from.
 * @return Number of result tables, 0 on no results, -1 on failure.
 */
SHAREDLIB_JNIEXPORT jint JNICALL
Java_org_voltdb_jni_ExecutionEngine_nativeGetStats(JNIEnv *env, jobject obj,
                                                   jlong pointer, jint selector,
                                                   jintArray locatorsArray,
                                                   jboolean jinterval,
                                                   jlong now) {
    VoltDBEngine *engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    /*
     * Can't use the standard JNI EE error code here because this method
     * actually uses that integer to indicate the number of result tables.
     */
    int result = -1;

    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);
    engine->resetReusedResultOutputBuffer();

    /*
     * Retrieve locators if any
     */
    int *locators = NULL;
    int numLocators = 0;
    if (locatorsArray != NULL) {
        locators = env->GetIntArrayElements(locatorsArray, NULL);
        if (locators == NULL) {
            env->ExceptionDescribe();
            return JNI_FALSE;
        }
        numLocators = env->GetArrayLength(locatorsArray);
        if (env->ExceptionCheck()) {
            env->ExceptionDescribe();
            env->ReleaseIntArrayElements(locatorsArray, locators, JNI_ABORT);
            return JNI_FALSE;
        }
    }
    const bool interval = jinterval == JNI_TRUE ? true : false;

    try {
        result = engine->getStats(static_cast<int>(selector), locators,
                                  numLocators, interval, now);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }

    env->ReleaseIntArrayElements(locatorsArray, locators, JNI_ABORT);

    return static_cast<jint>(result);
}

/**
 * Turns on or off profiler.
 * @returns 0 on success.
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeToggleProfiler
(JNIEnv *env, jobject obj, jlong engine_ptr, jint toggle)
{
    VOLT_DEBUG("nativeToggleProfiler in C++ called");
// set on build command line via build.py
#ifdef PROFILE_ENABLED
    VoltDBEngine *engine = castToEngine(engine_ptr);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    if (engine) {
        if (toggle) {
            ProfilerStart("/tmp/gprof.prof");
        }
        else {
            ProfilerStop();
            ProfilerFlush();
        }
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    }
#endif
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * Release the undo token
 * @returns JNI_TRUE on success. JNI_FALSE otherwise.
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeReleaseUndoToken
(JNIEnv *env, jobject obj, jlong engine_ptr, jlong undoToken, jboolean isEmptyDRTxn) {
    VOLT_DEBUG("nativeReleaseUndoToken in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        if (engine) {
            engine->releaseUndoToken(undoToken, isEmptyDRTxn);
            return JNI_TRUE;
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return JNI_FALSE;
}

/**
 * Undo the undo token
 * @returns JNI_TRUE on success. JNI_FALSE otherwise.
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeUndoUndoToken
(JNIEnv *env, jobject obj, jlong engine_ptr, jlong undoToken) {
    VOLT_DEBUG("nativeUndoUndoToken in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        if (engine) {
              engine->undoUndoToken(undoToken);
            return JNI_TRUE;
        }
        return JNI_FALSE;
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return false;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetLogLevels
 * Signature: (JJ)Z
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetLogLevels
  (JNIEnv *env, jobject obj, jlong engine_ptr, jlong logLevels) {
    VOLT_DEBUG("nativeSetLogLevels in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        if (engine) {
            engine->getLogManager()->setLogLevels(logLevels);
        }
        return JNI_FALSE;
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return false;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetSnapshotSchema
 * Signature: (JIBZ)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetSnapshotSchema(
        JNIEnv *env, jobject obj, jlong engine_ptr, jint tableId, jbyte schemaFilterType, jboolean forceLive)
{
    VoltDBEngine *engine = castToEngine(engine_ptr);
    voltdb::HiddenColumnFilter::Type hiddenColumnFilter = static_cast<voltdb::HiddenColumnFilter::Type>(schemaFilterType);
    return engine->getSnapshotSchema(tableId, hiddenColumnFilter, forceLive);
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeActivateTableStream
 * Signature: (JIIIJ[B)Z
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeActivateTableStream(
        JNIEnv *env, jobject obj, jlong engine_ptr, jint tableId, jint streamType, jbyte schemaFilterType, jlong undoToken,
        jbyteArray serialized_predicates)
{
    VOLT_DEBUG("nativeActivateTableStream in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);

    // deserialize predicates.
    jsize length = env->GetArrayLength(serialized_predicates);
    VOLT_DEBUG("deserializing %d predicate bytes ...", (int) length);
    jbyte *bytes = env->GetByteArrayElements(serialized_predicates, NULL);
    ReferenceSerializeInputBE serialize_in(bytes, length);
    try {
        try {
            voltdb::TableStreamType tableStreamType = static_cast<voltdb::TableStreamType>(streamType);
            voltdb::HiddenColumnFilter::Type hiddenColumnFilter = static_cast<voltdb::HiddenColumnFilter::Type>(schemaFilterType);
            bool success = engine->activateTableStream(tableId, tableStreamType, hiddenColumnFilter, undoToken, serialize_in);
            env->ReleaseByteArrayElements(serialized_predicates, bytes, JNI_ABORT);
            VOLT_DEBUG("deserialized predicates (success=%d)", (int)success);
            return success;
        } catch (SerializableEEException &e) {
            engine->resetReusedResultOutputBuffer();
            e.serialize(engine->getExceptionOutputSerializer());
        }
    } catch (const FatalException& e) {
        topend->crashVoltDB(e);
    }

    return false;
}

// Templated utility function to convert a Java array to a C array with error
// checking.  Use overloaded adapter functions to allow the template to call
// different JNIEnv method names for different types.
// Return true if successful.
inline jlong *envGetArrayElements(JNIEnv *env, jlongArray jarray, jboolean *isCopy) {
    return env->GetLongArrayElements(jarray, isCopy);
}
inline jint *envGetArrayElements(JNIEnv *env, jintArray jarray, jboolean *isCopy) {
    return env->GetIntArrayElements(jarray, isCopy);
}
inline void envReleaseArrayElements(JNIEnv *env, jlongArray jarray, jlong *carray, jint mode) {
    return env->ReleaseLongArrayElements(jarray, carray, mode);
}
inline void envReleaseArrayElements(JNIEnv *env, jintArray jarray, jint *carray, jint mode) {
    return env->ReleaseIntArrayElements(jarray, carray, mode);
}
template <typename Tin, typename Tout>
static bool getArrayElements(
        JNIEnv *env,
        Tin jarray,
        Tout *&retarray,
        jint &retlength) {
    if (jarray == NULL) {
        VOLT_ERROR("getArrayElements: NULL array received");
        return false;
    }
    retarray = envGetArrayElements(env, jarray, NULL);
    if (retarray == NULL) {
        env->ExceptionDescribe();
        VOLT_ERROR("getArrayElements: NULL array extracted");
        return false;
    }
    retlength = env->GetArrayLength(jarray);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        envReleaseArrayElements(env, jarray, retarray, JNI_ABORT);
        VOLT_ERROR("getLongArrayElements: exception while extracting long array");
        return false;
    }
    return true;
}

/*
 * Serialize more tuples to one or more output streams.
 * Returns a long for the remaining tuple count, -1 for an error.
 * Streams an int position array through the reused result buffer.
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTableStreamSerializeMore
 * Signature: (JII[B)J;
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTableStreamSerializeMore
(JNIEnv *env,
 jobject obj,
 jlong engine_ptr,
 jint tableId,
 jint streamType,
 jbyteArray serialized_buffers) {
    VOLT_DEBUG("nativeTableStreamSerializeMore in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jsize length = env->GetArrayLength(serialized_buffers);
    VOLT_DEBUG("nativeTableStreamSerializeMore: deserializing %d buffer bytes ...", (int) length);
    jbyte *bytes = env->GetByteArrayElements(serialized_buffers, NULL);
    ReferenceSerializeInputBE serialize_in(bytes, length);
    try {
        try {
            voltdb::TableStreamType tst = static_cast<voltdb::TableStreamType>(streamType);
            jlong tuplesRemaining = engine->tableStreamSerializeMore(tableId, tst, serialize_in);
            env->ReleaseByteArrayElements(serialized_buffers, bytes, JNI_ABORT);
            return tuplesRemaining;
        } catch (const SQLException &e) {
            throwFatalException("%s", e.message().c_str());
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    // Won't get here.
    return TABLE_STREAM_SERIALIZATION_ERROR;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeTableHashCode
 * Signature: (JI)J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTableHashCode
  (JNIEnv *env, jobject obj, jlong engine_ptr, jint tableId) {
    VOLT_DEBUG("nativeTableHashCode in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        try {
            return engine->tableHashCode(tableId);
        } catch (const SQLException &e) {
            throwFatalException("%s", e.message().c_str());
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 0;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetExportStreamPositions
 *
 * @param ackAction  true if this call contains an ack
 * @param pollAction true if this call requests a poll
 * @param syncAction true if the stream offset being set for a table
 * @param ackOffset  if acking, the universal stream offset being acked/released
 * @param streamName    Name of the stream to which the Export action applies
 *
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetExportStreamPositions
  (JNIEnv *env,
   jobject obj,
   jlong engine_ptr,
   jlong ackOffset,
   jlong seqNo,
   jlong genId,
   jbyteArray streamName) {
    VOLT_DEBUG("nativeExportAction in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jbyte *streamNameChars = env->GetByteArrayElements(streamName, NULL);
    std::string streamNameStr(reinterpret_cast<char *>(streamNameChars), env->GetArrayLength(streamName));
    env->ReleaseByteArrayElements(streamName, streamNameChars, JNI_ABORT);
    try {
        try {
            engine->resetReusedResultOutputBuffer();
            engine->setExportStreamPositions(static_cast<int64_t>(ackOffset),
                                        static_cast<int64_t>(seqNo),
                                        static_cast<int64_t>(genId),
                                        streamNameStr);
        } catch (const SQLException &e) {
            throwFatalException("%s", e.message().c_str());
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
}

/**
 * Complete the deletion of the Migrated Table rows.
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeDeleteMigratedRows
 *
 * @param txnId The transactionId of the currently executing stored procedure
 * @param spHandle The spHandle of the currently executing stored procedure
 * @param uniqueId The uniqueId of the currently executing stored procedure
 * @param mTableName The name of the table that the deletes should be applied to.
 * @param deletableTxnId The transactionId of the last row that can be deleted
 * @param maxRowCount The upper bound on the number of rows that can be deleted (batch size)
 * @param undoToken The token marking the rollback point for this transaction
 * @return true if more rows to be deleted
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDeleteMigratedRows(
        JNIEnv *env, jobject obj, jlong engine_ptr,
        jlong txnId, jlong spHandle, jlong uniqueId,
        jbyteArray tableName, jlong deletableTxnId, jlong undoToken)
{
    VOLT_DEBUG("nativeDeleteMigratedRows in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jbyte *tableNameChars = env->GetByteArrayElements(tableName, NULL);
    std::string tableNameStr(reinterpret_cast<char *>(tableNameChars), env->GetArrayLength(tableName));
    env->ReleaseByteArrayElements(tableName, tableNameChars, JNI_ABORT);
    try {
        try {
            engine->resetReusedResultOutputBuffer();
            return engine->deleteMigratedRows(static_cast<int64_t>(txnId),
                                              static_cast<int64_t>(spHandle),
                                              static_cast<int64_t>(uniqueId),
                                              tableNameStr,
                                              static_cast<int64_t>(deletableTxnId),
                                              static_cast<int64_t>(undoToken));
        } catch (const SQLException &e) {
            throwFatalException("%s", e.message().c_str());
        }
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return false;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetUSOForExportTable
 * Signature: (J[B)[J
 */
SHAREDLIB_JNIEXPORT jlongArray JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetUSOForExportTable
  (JNIEnv *env, jobject obj, jlong engine_ptr, jbyteArray streamName) {

    VOLT_DEBUG("nativeGetUSOForExportTable in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jbyte *streamNameChars = env->GetByteArrayElements(streamName, NULL);
    std::string streamNameStr(reinterpret_cast<char *>(streamNameChars), env->GetArrayLength(streamName));
    env->ReleaseByteArrayElements(streamName, streamNameChars, JNI_ABORT);
    try {
        jlong data[3];
        size_t ackOffset;
        int64_t seqNo;
        int64_t generationId;
        engine->getUSOForExportTable(ackOffset, seqNo, generationId, streamNameStr);
        data[0] = ackOffset;
        data[1] = seqNo;
        data[2] = generationId;
        jlongArray retval = env->NewLongArray(3);
        env->SetLongArrayRegion(retval, 0, 3, data);
        return retval;
    }
    catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return NULL;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeHashinate
 * Signature: (JI)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeHashinate(JNIEnv *env, jobject obj, jlong engine_ptr, jlong configPtr, jint tokenCount)
{
    VOLT_DEBUG("nativeHashinate in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        NValueArray& params = engine->getExecutorContext()->getParameterContainer();
        Pool *stringPool = engine->getStringPool();
        deserializeParameterSet(engine->getParameterBuffer(), engine->getParameterBufferCapacity(), params, engine->getStringPool());
        boost::scoped_ptr<TheHashinator> hashinator;
        const char *configValue = voltdb::ValuePeeker::peekObjectValue(params[1]);
        hashinator.reset(ElasticHashinator::newInstance(configValue, reinterpret_cast<int32_t*>(configPtr), static_cast<uint32_t>(tokenCount)));
        int retval =
            hashinator->hashinate(params[0]);
        stringPool->purge();
        return retval;
    } catch (const FatalException &e) {
        std::cout << "HASHINATE ERROR: " << e.m_reason << std::endl;
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeUpdateHashinator
 * Signature: (JI)I
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeUpdateHashinator(JNIEnv *env, jobject obj, jlong engine_ptr, jlong configPtr, jint tokenCount)
{
    VOLT_DEBUG("nativeUpdateHashinator in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        //Fast path processing, just use the config given by pointer
        if (configPtr != 0) {
            engine->updateHashinator(NULL, reinterpret_cast<int32_t*>(configPtr), static_cast<uint32_t>(tokenCount));
            return;
        }
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        NValueArray& params = engine->getExecutorContext()->getParameterContainer();
        Pool *stringPool = engine->getStringPool();
        deserializeParameterSet(engine->getParameterBuffer(), engine->getParameterBufferCapacity(), params, engine->getStringPool());
        const char *configValue = voltdb::ValuePeeker::peekObjectValue(params[0]);
        engine->updateHashinator(configValue, reinterpret_cast<int32_t*>(configPtr), static_cast<uint32_t>(tokenCount));
        stringPool->purge();
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetThreadLocalPoolAllocations
 * Signature: ()J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetThreadLocalPoolAllocations
  (JNIEnv *, jclass) {
    return ThreadLocalPool::getPoolAllocationSize();
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeGetRSS
 * Signature: ()J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetRSS
  (JNIEnv *, jclass) {

    // This code only does anything useful on MACOSX.
    // It returns the RSS size in bytes.
    // On linux, procfs is read to get RSS

#ifdef MACOSX
    // inspired by http://blog.kuriositaet.de/?p=257
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    if (KERN_SUCCESS != task_info(mach_task_self(),
       TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count))
    {
        return -1;
    }
    return t_info.resident_size;
#else
    return -1;
#endif // MACOSX
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    nativeFreeMemory
 * Signature: (J)V
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltcore_utils_DBBPool_nativeFreeMemory
  (JNIEnv *env, jclass clazz, jlong ptr) {
    // Used to free memory allocated by nativeAllocateUnsafeByteBuffer and nativeAlignedAllocateUnsafeByteBuffer
    // nativeAlignedAllocateUnsafeByteBuffer has to use a low level allocation so malloc and free must be used
    ::free(reinterpret_cast<void*>(ptr));
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    nativeAllocateUnsafeByteBuffer
 * Signature: (J)Ljava/nio/ByteBuffer;
 */
SHAREDLIB_JNIEXPORT jobject JNICALL Java_org_voltcore_utils_DBBPool_nativeAllocateUnsafeByteBuffer
  (JNIEnv *jniEnv, jclass, jlong size) {
    jobject buffer = nullptr;

    // See nativeFreeMemory for why malloc is being used
    void *memory = ::malloc(size);
    if (memory != nullptr) {
        buffer = jniEnv->NewDirectByteBuffer( memory, size);
        if (buffer == NULL) {
            jniEnv->ExceptionDescribe();
            throw std::exception();
        }
    }
    return buffer;
}

/*
 * Class:     org_voltdb_utils_PosixAdvise
 * Method:    madvise
 * Signature: (JJI)J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_utils_PosixAdvise_madvise
  (JNIEnv *, jclass, jlong addr, jlong length, jint advice) {
#ifdef LINUX
    return posix_madvise(reinterpret_cast<void*>(addr), static_cast<size_t>(length), advice);
#else
    return 0;
#endif
}

/**
 * Utility used for access file descriptor number from JAVA FileDescriptor class
 */
jint getFdFromFileDescriptor(JNIEnv *env, jobject fdObject) {
    return env-> GetIntField(fdObject, field_fd);
}

/*
 * Class:     org_voltdb_utils_PosixAdvise
 * Method:    nativeFadvise
 * Signature: (Ljava/io/FileDescriptor;JJI)J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_utils_PosixAdvise_nativeFadvise
        (JNIEnv *env, jclass, jobject fdObject, jlong offset, jlong length, jint advice) {
#ifdef LINUX
    return posix_fadvise(getFdFromFileDescriptor(env,fdObject), static_cast<off_t>(offset), static_cast<off_t>(length), advice);
#else
    return 0;
#endif
}

/*
 * Class:     org_voltdb_utils_PosixAdvise
 * Method:    sync_file_range
 * Signature: (Ljava/io/FileDescriptor;JJI)J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_utils_PosixAdvise_sync_1file_1range
        (JNIEnv *env, jclass, jobject fdObject, jlong offset, jlong length, jint advice) {
#ifdef LINUX
#ifndef __NR_sync_file_range
#error VoltDB server requires that your kernel headers define __NR_sync_file_range.
#endif
    return syscall(__NR_sync_file_range, getFdFromFileDescriptor(env,fdObject), static_cast<loff_t>(offset), static_cast<loff_t>(length),
                   static_cast<unsigned int>(advice));
#elif MACOSX
    return -1;
#else
    return fdatasync(getFdFromFileDescriptor(env,fdObject));
#endif
}


/*
 * Class:     org_voltdb_utils_PosixAdvise
 * Method:    fallocate
 * Signature: (Ljava/io/FileDescriptor;JJ)J
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_utils_PosixAdvise_fallocate
        (JNIEnv *env, jclass, jobject fdObject, jlong offset, jlong length) {
#ifdef MACOSX
    return -1;
#else
    return posix_fallocate(getFdFromFileDescriptor(env,fdObject), static_cast<off_t>(offset), static_cast<off_t>(length));
#endif
}

/*
 * Class:     Java_org_voltdb_utils_DirectIoFileChannel
 * Method:    nativeOpen
 * Signature: ([B)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_utils_DirectIoFileChannel_nativeOpen
        (JNIEnv *env, jclass, jbyteArray pathArray) {
#ifdef MACOSX
    return -ENOTSUP;
#else
    jbyte* pathBytes = env->GetByteArrayElements(pathArray, nullptr);
    int length = env->GetArrayLength(pathArray);

    std::string path(reinterpret_cast<char*>(pathBytes), length);
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_DIRECT, S_IRUSR | S_IWUSR | S_IRGRP);

    return fd >= 0 ? fd : -errno;
#endif
}

/*
 * Class:     org_voltcore_utils_DBBPool
 * Method:    nativeAllocateAlignedUnsafeByteBuffer
 * Signature: (II)Ljava/nio/ByteBuffer;
 */
SHAREDLIB_JNIEXPORT jobject JNICALL Java_org_voltcore_utils_DBBPool_nativeAllocateAlignedUnsafeByteBuffer
  (JNIEnv *jniEnv, jclass, jint alignment, jint size) {
    jobject buffer = nullptr;

#ifdef LINUX
    void *memory = ::aligned_alloc(alignment, size);
    if (memory != nullptr) {
        buffer = jniEnv->NewDirectByteBuffer(memory, size);
        if (buffer == NULL) {
            jniEnv->ExceptionDescribe();
            throw std::exception();
        }
    }
#endif

    return buffer;
}


SHAREDLIB_JNIEXPORT jlong JNICALL
Java_org_voltdb_jni_ExecutionEngine_nativeApplyBinaryLog (
    JNIEnv *env, jobject obj, jlong engine_ptr,
    jlong txnId, jlong spHandle, jlong lastCommittedSpHandle, jlong uniqueId, jint remoteClusterId, jlong undoToken)
{
    VoltDBEngine *engine = castToEngine(engine_ptr);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    if (engine == NULL) {
        return -1L;
    }

    engine->resetReusedResultOutputBuffer();

    //JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);
    VOLT_DEBUG("applying MP binary log in C++...");

    try {
        return engine->applyBinaryLog(txnId, spHandle, lastCommittedSpHandle, uniqueId, remoteClusterId, undoToken,
                engine->getParameterBuffer() + sizeof(int64_t));
    } catch (const SerializableEEException &e) {
        engine->resetReusedResultOutputBuffer();
        e.serialize(engine->getExceptionOutputSerializer());
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }

    return -1L;
}

SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecuteTask
  (JNIEnv *env, jobject obj, jlong engine_ptr) {
    VOLT_DEBUG("nativeExecuteTask in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
        engine->resetReusedResultOutputBuffer();

        ReferenceSerializeInputBE input(engine->getParameterBuffer(), engine->getParameterBufferCapacity());
        TaskType taskId = static_cast<TaskType>(input.readLong());
        engine->executeTask(taskId, input);
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    } catch (const SerializableEEException &e) {
        engine->resetReusedResultOutputBuffer();
        e.serialize(engine->getExceptionOutputSerializer());
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }

    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    getTestDRBuffer
 * Signature: (II[I[IJ)[B
 */
SHAREDLIB_JNIEXPORT jbyteArray JNICALL Java_org_voltdb_jni_ExecutionEngine_getTestDRBuffer
  (JNIEnv *env, jclass clazz, jint drProtocolVersion, jint partitionId, jintArray partitionKeyValues, jintArray flags,
          jlong startSequenceNumber) {
    try {
        jint *partitionKeyValuesJPtr = env->GetIntArrayElements(partitionKeyValues, NULL);
        int32_t *partitionKeyValuesPtr = reinterpret_cast<int32_t *>(partitionKeyValuesJPtr);
        std::vector<int32_t> partitionKeyValueList(partitionKeyValuesPtr,
            partitionKeyValuesPtr + env->GetArrayLength(partitionKeyValues));
        env->ReleaseIntArrayElements(partitionKeyValues, partitionKeyValuesJPtr, JNI_ABORT);

        jint *flagsJPtr = env->GetIntArrayElements(flags, NULL);
        int32_t *flagsPtr = reinterpret_cast<int32_t *>(flagsJPtr);
        std::vector<int32_t> flagList(flagsPtr, flagsPtr + env->GetArrayLength(flags));
        env->ReleaseIntArrayElements(flags, flagsJPtr, JNI_ABORT);

        vassert(partitionKeyValueList.size() == flagList.size());

        char *output = new char[1024 * 256];
        int32_t length = DRTupleStream::getTestDRBuffer(static_cast<uint8_t>(drProtocolVersion), partitionId,
                                                        partitionKeyValueList, flagList,
                                                        startSequenceNumber, output);
        jbyteArray array = env->NewByteArray(length);
        jbyte *arrayBytes = env->GetByteArrayElements(array, NULL);
        ::memcpy(arrayBytes, output, length);
        env->ReleaseByteArrayElements(array, arrayBytes, 0);
        return array;
    } catch (const FatalException &e) {
        cerr << e.m_reason << std::endl;
        cerr << e.m_backtracepath << std::endl;
        cerr << e.m_filename << ":" << e.m_lineno << std::endl;
        for (int ii = 0; ii < e.m_traces.size(); ii++) {
            cerr << e.m_traces[ii] << std::endl;
        }
        exit(-1);
    }
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetViewsEnabled
 * Signature: (J[BZ)V
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetViewsEnabled
  (JNIEnv *env, jobject object, jlong engine_ptr, jbyteArray viewNamesAsBytes, jboolean enabled) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    jbyte *viewNamesChars = env->GetByteArrayElements(viewNamesAsBytes, NULL);
    std::string viewNames(reinterpret_cast<char *>(viewNamesChars), env->GetArrayLength(viewNamesAsBytes));
    env->ReleaseByteArrayElements(viewNamesAsBytes, viewNamesChars, JNI_ABORT);
    engine->setViewsEnabled(viewNames, enabled);
}

/*
 * Implemention of ExecutionEngineJNI.nativeDisableExternalStreams
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDisableExternalStreams
  (JNIEnv *env, jobject object, jlong engine_ptr) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    engine->disableExternalStreams();
}

/*
 * Implemention of ExecutionEngineJNI.nativeExternalStreamsEnabled
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExternalStreamsEnabled
  (JNIEnv *env, jobject object, jlong engine_ptr) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    vassert(engine);
    return engine->externalStreamsEnabled();
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeStoreTopicsGroup
 * Signature: (JJ)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeStoreTopicsGroup
  (JNIEnv *env, jclass clazz, jlong pointer, jlong undoQuantum) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    ReferenceSerializeInputBE in(engine->getParameterBuffer(), engine->getParameterBufferCapacity());
    try {
        return engine->storeTopicsGroup(undoQuantum, in);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeDeleteTopicsGroup
 * Signature: (JJ[B)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDeleteTopicsGroup
  (JNIEnv *env, jclass clazz, jlong pointer, jlong undoQuantum, jbyteArray groupIdIn) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jbyte* groupIdBytes = env->GetByteArrayElements(groupIdIn, nullptr);
    NValue groupId = ValueFactory::getTempStringValue(reinterpret_cast<char*>(groupIdBytes),
            env->GetArrayLength(groupIdIn));
    env->ReleaseByteArrayElements(groupIdIn, groupIdBytes, JNI_ABORT);
    try {
        return engine->deleteTopicsGroup(undoQuantum, groupId);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeFetchTopicsGroups
 * Signature: (JI)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeFetchTopicsGroups
  (JNIEnv *env, jclass clazz, jlong pointer, jint maxReultSize, jbyteArray startGroupIdIn) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    NValue groupId;
    if (startGroupIdIn == nullptr) {
        groupId = ValueFactory::getNullStringValue();
    } else {
        jbyte* groupIdBytes = env->GetByteArrayElements(startGroupIdIn, nullptr);
        groupId = ValueFactory::getTempStringValue(reinterpret_cast<char*>(groupIdBytes),
                env->GetArrayLength(startGroupIdIn));
        env->ReleaseByteArrayElements(startGroupIdIn, groupIdBytes, JNI_ABORT);
    }
    try {
        return engine->fetchTopicsGroups(maxReultSize, groupId);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return -1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeCommitTopicsGroupOffsets
 * Signature: (JJJS[B)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeCommitTopicsGroupOffsets
  (JNIEnv *env, jclass clazz, jlong pointer, jlong spUniqueId, jlong undoQuantum, jshort requestVersion,
          jbyteArray groupIdIn) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jbyte* groupIdBytes = env->GetByteArrayElements(groupIdIn, nullptr);
    NValue groupId = ValueFactory::getTempStringValue(reinterpret_cast<char*>(groupIdBytes),
            env->GetArrayLength(groupIdIn));
    env->ReleaseByteArrayElements(groupIdIn, groupIdBytes, JNI_ABORT);
    ReferenceSerializeInputBE in(engine->getParameterBuffer(), engine->getParameterBufferCapacity());
    try {
        in.limit(in.readInt());
        return engine->commitTopicsGroupOffsets(spUniqueId, undoQuantum, requestVersion, groupId, in);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeFetchTopicsGroupOffsets
 * Signature: (JS[B)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeFetchTopicsGroupOffsets
  (JNIEnv *env, jclass clazz, jlong pointer, jshort requestVersion, jbyteArray groupIdIn) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    jbyte* groupIdBytes = env->GetByteArrayElements(groupIdIn, nullptr);
    NValue groupId = ValueFactory::getTempStringValue(reinterpret_cast<char*>(groupIdBytes),
            env->GetArrayLength(groupIdIn));
    env->ReleaseByteArrayElements(groupIdIn, groupIdBytes, JNI_ABORT);
    ReferenceSerializeInputBE in(engine->getParameterBuffer(), engine->getParameterBufferCapacity());
    try {
        in.limit(in.readInt());
        return engine->fetchTopicsGroupOffsets(requestVersion, groupId, in);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeDeleteExpiredTopicsOffsets
 * Signature: (JJJ)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeDeleteExpiredTopicsOffsets
  (JNIEnv *env, jclass clazz, jlong pointer, jlong undoToken, jlong deleteOlderThan) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        return engine->deleteExpiredTopicsOffsets(undoToken, deleteOlderThan);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeSetReplicableTables
 * Signature: (JI[[B)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetReplicableTables
  (JNIEnv *env, jclass clazz, jlong pointer, jint clusterId, jobjectArray tables) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        if (tables == nullptr) {
            return engine->setReplicableTables(clusterId, nullptr);
        }

        std::vector<std::string> replicableTables;

        jsize size = env->GetArrayLength(tables);
        for (int i = 0; i < size; ++i) {
            jbyteArray tableName = static_cast<jbyteArray>(env->GetObjectArrayElement(tables, i));
            jbyte* tableNameBytes = env->GetByteArrayElements(tableName, nullptr);
            replicableTables.emplace_back(reinterpret_cast<char*>(tableNameBytes), env->GetArrayLength(tableName));
        }

        return engine->setReplicableTables(clusterId, &replicableTables);
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeClearAllReplicableTables
 * Signature: (J)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeClearAllReplicableTables
  (JNIEnv *env, jclass clazz, jlong pointer) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        engine->clearAllReplicableTables();
        return 0;
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeClearReplicableTables
 * Signature: (J)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeClearReplicableTables
        (JNIEnv *env, jclass clazz, jlong pointer, jint clusterId) {
    auto engine = castToEngine(pointer);
    Topend *topend = static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    try {
        engine->clearReplicableTables(clusterId);
        return 0;
    } catch (const FatalException &e) {
        topend->crashVoltDB(e);
    }
    return 1;
}

/** @} */ // end of JNI doxygen group
