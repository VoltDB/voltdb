/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
 * terms and conditions:
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
#ifdef LINUX
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <sys/mman.h>
#include <unistd.h>
#ifndef __USE_GNU
#define  __USE_GNU
#endif
#include <sched.h>
#endif

//#include <google/profiler.h>

//#include <jni/jni.h>
// TODO: gcc picks up wrong jni_md.h and results in compile error (bad
// declaration of jboolean) if I include the jni.h in externals.
// Can we assume jni.h? or still better to have jni.h in externals?
#include <jni.h>

#include "org_voltdb_jni_ExecutionEngine.h" // the header file output by javah
#include "org_voltdb_utils_DBBPool.h" //Utility method for DBBContainer
#include "org_voltdb_utils_ThreadUtils.h"


#include "boost/shared_ptr.hpp"
#include "boost/scoped_array.hpp"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/TheHashinator.h"
#include "common/Pool.hpp"
#include "execution/VoltDBEngine.h"
#include "execution/JNITopend.h"
#include "json_spirit/json_spirit.h"
#include "boost/pool/pool.hpp"
#include "boost/crc.hpp"
#include "logging/JNILogProxy.h"

#include "logging/LogDefs.h"
#include "logging/Logger.h"

// Defines a function as a JNIEXPORT for a shared library
#define SHAREDLIB_JNIEXPORT __attribute__((visibility("default"))) JNIEXPORT

using namespace std;
using namespace voltdb;

#define castToEngine(x) reinterpret_cast<VoltDBEngine*>((x));
#define updateJNILogProxy(x) const_cast<JNILogProxy*>(dynamic_cast<const JNILogProxy*>(x->getLogManager()->getLogProxy()))->setJNIEnv(env)


////////////////////////////////////////////////////////////////////////////
// Create / Destroy
////////////////////////////////////////////////////////////////////////////
/**
 * Just creates a new VoltDBEngine object and retunrs it to Java.
 * Never fail to destroy() for the VoltDBEngine* once you call this method
 * NOTE: Call initialize() separately for initialization.
 * This does strictly nothing so that this method never throws an exception.
 * @return the created VoltDBEngine pointer casted to jlong.
*/
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeCreate(JNIEnv *env, jobject obj) {
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
        assert(!"Failed to allocate global reference to java EE.");
        return 0;
    }
    JavaVM *vm;
    env->GetJavaVM(&vm);
    VoltDBEngine *engine = new VoltDBEngine(new JNITopend(env, java_ee), JNILogProxy::getJNILogProxy(env, vm));
    const voltdb::Logger *logger = voltdb::LogManager::getThreadLogger(voltdb::LOGGERID_HOST);
    logger->log(voltdb::LOGLEVEL_INFO, "Successfully used a JNI log proxy");
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
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    delete engine;
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
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
    jlong enginePtr, jint clusterIndex, jint siteId) {
    VOLT_DEBUG("nativeInitialize() start");
    VoltDBEngine *engine = castToEngine(enginePtr);
    if (engine == NULL) {
        VOLT_ERROR("engine_ptr was NULL or invalid pointer");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated

    // initialization is separated from constructor so that constructor
    // never fails.
    VOLT_DEBUG("calling initialize...");
    bool success = engine->initialize(clusterIndex, siteId);

    if (success) {
        VOLT_DEBUG("initialize succeeded");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    } else {
        VOLT_ERROR("initialize failed");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
}

/**
 * Load the system catalog for this engine.
 * @param engine_ptr the VoltDBEngine pointer
 * @param serialized_catalog the root catalog object serialized as text strings.
 * this parameter is jstring, not jbytearray because Catalog is serialized into
 * human-readable text strings separated by line feeds.
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeLoadCatalog(
    JNIEnv *env, jobject obj,
    jlong engine_ptr, jstring serialized_catalog) {
    VOLT_DEBUG("nativeLoadCatalog() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    if (engine == NULL) {
        VOLT_ERROR("engine_ptr was NULL or invalid pointer");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated

    //copy to std::string. utf_chars may or may not by a copy of the string
    const char* utf_chars = env->GetStringUTFChars(serialized_catalog, NULL);
    string str(utf_chars);
    env->ReleaseStringUTFChars(serialized_catalog, utf_chars);
    VOLT_DEBUG("calling loadCatalog...");
    bool success = engine->loadCatalog(str);

    if (success) {
        VOLT_DEBUG("loadCatalog succeeded");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    } else {
        VOLT_ERROR("loadCatalog failed");
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
}


/**
 * This method is called to initially load table data.
 * @param pointer the VoltDBEngine pointer
 * @param table_id catalog ID of the table
 * @param serialized_table the table data to be loaded
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeLoadTable
(JNIEnv *env, jobject obj, jlong engine_ptr, jint table_id, jbyteArray serialized_table,
        jlong txnId, jlong lastCommittedTxnId, jlong undoToken) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    engine->setUndoToken(undoToken);
    VOLT_DEBUG("loading table %d in C++...", table_id);

    // deserialize dependency.
    jsize length = env->GetArrayLength(serialized_table);
    VOLT_DEBUG("deserializing %d bytes ...", (int) length);
    jbyte *bytes = env->GetByteArrayElements(serialized_table, NULL);
    ReferenceSerializeInput serialize_in(bytes, length);
    bool success = engine->loadTable(table_id, serialize_in, txnId, lastCommittedTxnId);
    env->ReleaseByteArrayElements(serialized_table, bytes, JNI_ABORT);
    VOLT_DEBUG("deserialized table");

    if (success) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
    } else {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
}


////////////////////////////////////////////////////////////////////////////
// PlanNode Execution
////////////////////////////////////////////////////////////////////////////
/**
 * Utility used for deserializing ParameterSet passed from Java.
 */
void deserializeParameterSetCommon(int cnt, ReferenceSerializeInput &serialize_in,
                                   NValueArray &params, Pool *stringPool)
{
    for (int i = 0; i < cnt; ++i) {
        params[i] = NValue::deserializeFromAllocateForStorage(serialize_in, stringPool);
    }
}

/**
 * Utility used for deserializing ParameterSet passed from Java.
 */
int deserializeParameterSet(const char* serialized_parameterset, jint serialized_length,
    NValueArray &params, Pool *stringPool) {
    // deserialize parameters as ValueArray.
    // We don't use SerializeIO here because it makes a copy.
    ReferenceSerializeInput serialize_in(serialized_parameterset, serialized_length);

    // see org.voltdb.ParameterSet.
    // TODO : make it a class. later, later, later...
    int cnt = serialize_in.readShort();
    if (cnt < 0) {
        VOLT_ERROR("parameter count is negative: %d", cnt);
        assert (false);
    }
    assert (cnt < MAX_PARAM_COUNT);
    deserializeParameterSetCommon(cnt, serialize_in, params, stringPool);
    return cnt;
}

/**
 * Sets (or re-sets) the buffer shared between java and the EE. This is for reducing
 * cost of GetDirectBufferAddress().
 * @param pointer the VoltDBEngine pointer
 * @param parameter_buffer direct byte buffer to be set
 * @param m_parameterBuffersize size of the buffer
 * @param result_buffer direct byte buffer to be set
 * @param result_buffer_size size of the buffer
 * @param exception_buffer direct byte buffer to be set
 * @param exception_buffer_size size of the buffer
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeSetBuffers
  (JNIEnv *env, jobject obj, jlong engine_ptr, jobject parameter_buffer, jint parameter_buffer_size,
   jobject result_buffer, jint result_buffer_size,
   jobject exception_buffer, jint exception_buffer_size)
{
    VOLT_DEBUG("nativeSetBuffers() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    if (engine == NULL) {
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    }
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated

    char *parameterBuffer = reinterpret_cast<char*>(
            env->GetDirectBufferAddress(parameter_buffer));
    int parameterBufferCapacity = parameter_buffer_size;

    char *reusedResultBuffer = reinterpret_cast<char*>(
            env->GetDirectBufferAddress(result_buffer));
    int reusedResultBufferCapacity = result_buffer_size;

    char *exceptionBuffer = reinterpret_cast<char*>(
             env->GetDirectBufferAddress(exception_buffer));
    int exceptionBufferCapacity = exception_buffer_size;

    engine->setBuffers(parameterBuffer, parameterBufferCapacity,
        reusedResultBuffer, reusedResultBufferCapacity,
        exceptionBuffer, exceptionBufferCapacity);

    return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
}

/**
 * Executes a plan fragment with the given parameter set.
 * @param engine_ptr the VoltDBEngine pointer
 * @param plan_fragment_id ID of the plan fragment to be executed.
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecutePlanFragment (
        JNIEnv *env,
        jobject obj,
        jlong engine_ptr,
        jlong plan_fragment_id,
        jint outputDependencyId,
        jint inputDependencyId,
        jlong txnId,
        jlong lastCommittedTxnId,
        jlong undoToken) {
    VOLT_DEBUG("nativeExecutePlanFragment() start");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    assert(engine);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    engine->setUndoToken(undoToken);
    static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    engine->resetReusedResultOutputBuffer();
    NValueArray &params = engine->getParameterContainer();
    Pool *stringPool = engine->getStringPool();
    const int paramcnt = deserializeParameterSet(engine->getParameterBuffer(), engine->getParameterBufferCapacity(), params, engine->getStringPool());
    engine->setUsedParamcnt(paramcnt);
    const int retval = engine->executeQuery(plan_fragment_id, outputDependencyId, inputDependencyId, params, txnId, lastCommittedTxnId, true, true);
    stringPool->purge();
    return retval;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeExecuteCustomPlanFragment
 * Signature: (JLjava/lang/String;JJJ)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecuteCustomPlanFragment (
        JNIEnv *env,
        jobject obj,
        jlong engine_ptr,
        jstring plan,
        jint outputDependencyId,
        jint inputDependencyId,
        jlong txnId,
        jlong lastCommittedTxnId,
        jlong undoToken) {
    VOLT_DEBUG("nativeExecuteCustomPlanFragment() start");

    // setup
    VoltDBEngine *engine = castToEngine(engine_ptr);
    assert(engine);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    engine->resetReusedResultOutputBuffer();
    engine->setUndoToken(undoToken);
    static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    Pool *stringPool = engine->getStringPool();

    // convert java plan string to stdc++ string plan
    const char *str = static_cast<const char*>(env->GetStringUTFChars(plan, NULL));
    assert(str);
    string cppplan = str;
    env->ReleaseStringUTFChars(plan, str);

    // execute
    engine->setUsedParamcnt(0);
    int retval = engine->executePlanFragment(cppplan, outputDependencyId, inputDependencyId, txnId, lastCommittedTxnId);

    // cleanup
    stringPool->purge();
    return retval;
}

/**
 * Executes multiple plan fragments with the given parameter sets and gets the results.
 * @param pointer the VoltDBEngine pointer
 * @param plan_fragment_ids ID of the plan fragment to be executed.
 * @param outputBuffer buffer to be filled with the tables.
 * @param outputCapacity maximum number of bytes to write to buffer.
 * @return error code
*/
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeExecuteQueryPlanFragmentsAndGetResults
(JNIEnv *env,
        jobject obj,
        jlong engine_ptr,
        jlongArray plan_fragment_ids,
        jint num_fragments,
        jlong txnId,
        jlong lastCommittedTxnId,
        jlong undoToken) {
    //VOLT_DEBUG("nativeExecuteQueryPlanFragmentAndGetResults() start");

    // setup
    VoltDBEngine *engine = castToEngine(engine_ptr);
    assert(engine);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    engine->resetReusedResultOutputBuffer();
    engine->setUndoToken(undoToken);
    static_cast<JNITopend*>(engine->getTopend())->updateJNIEnv(env);
    Pool *stringPool = engine->getStringPool();

    // fragment info
    int batch_size = num_fragments;
    assert (batch_size <= MAX_BATCH_COUNT);
    jlong* fragment_ids_buffer = engine->getBatchFragmentIdsContainer();
    env->GetLongArrayRegion(plan_fragment_ids, 0, batch_size, fragment_ids_buffer);

    // all fragments' parameters are in this buffer
    ReferenceSerializeInput serialize_in(engine->getParameterBuffer(), engine->getParameterBufferCapacity());
    NValueArray &params = engine->getParameterContainer();

    // count failures
    int failures = 0;

    for (int i = 0; i < batch_size; ++i) {
        int cnt = serialize_in.readShort();
        if (cnt < 0) {
            VOLT_ERROR("parameter count is negative: %d", cnt);
            assert (false);
        }
        assert (cnt < MAX_PARAM_COUNT);
        deserializeParameterSetCommon(cnt, serialize_in, params, stringPool);

        engine->setUsedParamcnt(cnt);
        // success is 0 and error is 1.
        if (engine->executeQuery(fragment_ids_buffer[i], 1, -1,
                                 params, txnId, lastCommittedTxnId, i == 0,
                                 i == (batch_size - 1)))
        {
            ++failures;
        }
    }

    // cleanup
    stringPool->purge();

    if (failures > 0)
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    else
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
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
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    void* data = env->GetDirectBufferAddress(output_buffer);
    ReferenceSerializeOutput out(data, output_capacity);

    bool success = engine->serializeTable(table_id, &out);
    if (!success) return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
    else return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;
}

/**
 * Utility for appending error code on top of id vector.
 */
jintArray appendErrorCode(JNIEnv *env, int error_code, const std::vector<int32_t>& ids) {
    jsize result_size = static_cast<jsize>(ids.size()) + 1; // +1 for error code
    boost::scoped_array<jint> result(new jint[result_size]);
    result[0] = error_code;
    for (int i = 0; i < ids.size(); ++i) {
        result[i + 1] = ids[i];
    }
    //wrap it to jintArray
    jintArray array = env->NewIntArray(result_size);
    env->SetIntArrayRegion(array, 0, result_size, result.get());
    VOLT_DEBUG("returned %d elements including error code", result_size);
    return array;
}

/*
 * Class:     org_voltdb_utils_DBBPool
 * Method:    getBufferAddress
 * Signature: (Ljava/nio/ByteBuffer;)J
 *
 * Returns the native address of the provided DirectByteBuffer as a long
 * @param env Pointer to the JNIEnv for this thread
 * @param obj Pointer to the object on which this method was called
 * @param buffer DirectByteBuffer
 * @return Native address of the DirectByteBuffer as a long
 */
SHAREDLIB_JNIEXPORT jlong JNICALL Java_org_voltdb_utils_DBBPool_getBufferAddress
  (JNIEnv *env, jclass clazz, jobject buffer)
{
    void *address = env->GetDirectBufferAddress(buffer);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return (jlong)address;
    }
    assert(address);
    return reinterpret_cast<jlong>(address);
}

/*
 * Class:     org_voltdb_utils_DBBPool
 * Method:    getBufferCRC32
 * Signature: (Ljava/nio/ByteBuffer;II)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_utils_DBBPool_getBufferCRC32
  (JNIEnv *env, jclass clazz, jobject buffer, jint offset, jint length) {
    char *address = reinterpret_cast<char*>(env->GetDirectBufferAddress(buffer));
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return -1;
    }
    assert(address);
    boost::crc_32_type crc;
    crc.process_bytes(address + offset, length);
    return static_cast<jint>(crc.checksum());
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
 * @param lastCommittedTxnId The id of the last committed transaction.
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeTick
  (JNIEnv *env, jobject obj, jlong engine_ptr, jlong timeInMillis, jlong lastCommittedTxnId) {
    VoltDBEngine *engine = castToEngine(engine_ptr);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    engine->tick(timeInMillis, lastCommittedTxnId);
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeQuiesce
 * Signature: (JJ)V
 *
 * Called to instruct the EE to reach an idle steady state.
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeQuiesce
  (JNIEnv *env, jobject obj, jlong engine_ptr, jlong lastCommittedTxnId)
{
    VoltDBEngine *engine = castToEngine(engine_ptr);
    // JNIEnv pointer can change between calls, must be updated
    updateJNILogProxy(engine);
    engine->quiesce(lastCommittedTxnId);
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
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeGetStats
  (JNIEnv *env, jobject obj, jlong pointer, jint selector, jintArray locatorsArray) {
    VoltDBEngine *engine = castToEngine(pointer);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
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

    int result = engine->getStats(static_cast<int>(selector), locators, numLocators);
    env->ReleaseIntArrayElements(locatorsArray, locators, JNI_ABORT);
    return static_cast<jint>(result);
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    hashinate
 * Signature: (J)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_hashinate__JI
  (JNIEnv *, jobject, jlong value, jint partitionCount) {
    return voltdb::TheHashinator::hashinate(value, partitionCount);
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    hashinate
 * Signature: (Ljava/lang/String;I)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_hashinate__Ljava_lang_String_2I
  (JNIEnv *env, jobject, jstring value, jint partitionCount) {
    jsize length = env->GetStringUTFLength(value);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return 0;
    }
    const char *string = env->GetStringUTFChars( value, NULL);
    if (string == NULL) {
        return 0;
    }
    int32_t retval = voltdb::TheHashinator::hashinate(string, length, partitionCount);
    env->ReleaseStringUTFChars(value, string);
    return retval;
}

/**
 * Turns on or off profiler.
 * @returns 0 on success.
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeToggleProfiler
(JNIEnv *env, jobject obj, jlong engine_ptr, jint toggle)
{
    VOLT_DEBUG("nativeToggleProfiler in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    if (engine) {
        /*if (toggle) {
            ProfilerStart("/tmp/gprof.prof");
        }
        else {
            ProfilerStop();
        }*/
        return org_voltdb_jni_ExecutionEngine_ERRORCODE_SUCCESS;

    }
    return org_voltdb_jni_ExecutionEngine_ERRORCODE_ERROR;
}

/**
 * Release the undo token
 * @returns JNI_TRUE on success. JNI_FALSE otherwise.
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeReleaseUndoToken
(JNIEnv *env, jobject obj, jlong engine_ptr, jlong undoToken)
{
    VOLT_DEBUG("nativeReleaseUndoToken in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    if (engine) {
        engine->releaseUndoToken(undoToken);
        return JNI_TRUE;
    }
    return JNI_FALSE;
}

/**
 * Undo the undo token
 * @returns JNI_TRUE on success. JNI_FALSE otherwise.
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeUndoUndoToken
(JNIEnv *env, jobject obj, jlong engine_ptr, jlong undoToken)
{
    VOLT_DEBUG("nativeUndoUndoToken in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    if (engine) {
        engine->undoUndoToken(undoToken);
        return JNI_TRUE;
    }
    return JNI_FALSE;
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
    updateJNILogProxy(engine); //JNIEnv pointer can change between calls, must be updated
    if (engine) {
        engine->getLogManager()->setLogLevels(logLevels);
    }
    return JNI_FALSE;
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeActivateCopyOnWrite
 * Signature: (JI)Z
 */
SHAREDLIB_JNIEXPORT jboolean JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeActivateCopyOnWrite
  (JNIEnv *env, jobject obj, jlong engine_ptr, jint tableId) {
    VOLT_DEBUG("nativeActivateCopyOnWrite in C++ called");
    VoltDBEngine *engine = castToEngine(engine_ptr);
    return engine->activateCopyOnWrite(tableId);
}

/*
 * Class:     org_voltdb_jni_ExecutionEngine
 * Method:    nativeCOWSerializeMore
 * Signature: (JJIII)I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_jni_ExecutionEngine_nativeCOWSerializeMore
  (JNIEnv *env,
   jobject obj,
   jlong engine_ptr,
   jlong bufferPtr,
   jint offset,
   jint length,
   jint tableId) {
    VOLT_DEBUG("nativeSetLogLevels in C++ called");
    ReferenceSerializeOutput out(reinterpret_cast<char*>(bufferPtr) + offset, length - offset);
    VoltDBEngine *engine = castToEngine(engine_ptr);
    return engine->cowSerializeMore( &out, tableId);
}

#ifdef LINUX
/*
 * Class:     org_voltdb_utils_ThreadUtils
 * Method:    getThreadAffinity
 * Signature: ()[Z
 */
SHAREDLIB_JNIEXPORT jbooleanArray JNICALL Java_org_voltdb_utils_ThreadUtils_getThreadAffinity
  (JNIEnv *env, jclass clazz) {
    /*
     * First get the affinity mask for this thread.
     */
    cpu_set_t mycpuid;
    CPU_ZERO(&mycpuid);
    sched_getaffinity(0, sizeof(mycpuid), &mycpuid);

    //Also get the total number of processors
    long int NUM_PROCS = sysconf(_SC_NPROCESSORS_CONF);

    /*
     * Create the Java array and get the memory region.
     */
    jbooleanArray returnArray = env->NewBooleanArray(static_cast<jsize>(NUM_PROCS));
    if (returnArray == NULL) {
        return NULL;
    }
    jboolean *boolArray = env->GetBooleanArrayElements(returnArray, NULL);
    if (boolArray == NULL) {
        return NULL;
    }

    for (int ii = 0; ii < NUM_PROCS; ii++) {
        if (CPU_ISSET(ii, &mycpuid)) {
            boolArray[ii] = JNI_TRUE;
        } else {
            boolArray[ii] = JNI_FALSE;
        }
    }

    /*
     * Now released and commit the boolArray
     */
    env->ReleaseBooleanArrayElements(returnArray, boolArray, 0);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return NULL;
    }
    return returnArray;
}


/*
 * Class:     org_voltdb_utils_ThreadUtils
 * Method:    setThreadAffinity
 * Signature: ([Z)V
 */
SHAREDLIB_JNIEXPORT void JNICALL Java_org_voltdb_utils_ThreadUtils_setThreadAffinity
  (JNIEnv *env, jclass clazz, jbooleanArray coresArray) {
    jsize numCores = env->GetArrayLength(coresArray);
    if (env->ExceptionCheck()) {
        env->ExceptionDescribe();
        return;
    }
    jboolean *cores = env->GetBooleanArrayElements(coresArray, NULL);
    if (cores == NULL) {
        return;
    }

    //Also get the total number of processors
    //sysconf(_SC_NPROCESSORS_CONF);
    //assert(numCores <= NUM_PROCS);

    cpu_set_t mask;
    CPU_ZERO(&mask);
    for (int ii = 0; ii < numCores; ii++) {
        if (JNI_TRUE == cores[ii]) {
            CPU_SET(ii, &mask);
        }
    }

    if ( sched_setaffinity( 0, sizeof(mask), &mask) == -1) {
        std::cout << "Couldn't set CPU affinity" << std::endl;
        assert(false);
    }
}

/*
 * Class:     org_voltdb_utils_ThreadUtils
 * Method:    getNumCores
 * Signature: ()I
 */
SHAREDLIB_JNIEXPORT jint JNICALL Java_org_voltdb_utils_ThreadUtils_getNumCores
  (JNIEnv *env, jclass clazz) {
    long int NUM_PROCS = sysconf(_SC_NPROCESSORS_CONF);
    return static_cast<jint>(NUM_PROCS);
}
#endif

/** @} */ // end of JNI doxygen group
