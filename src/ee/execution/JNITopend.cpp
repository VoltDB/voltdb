/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#include "JNITopend.h"

#include "common/StreamBlock.h"
#include "storage/table.h"

using namespace std;

namespace voltdb{

// Create an instance of this class on the stack to release all local
// references created during its lifetime.
class JNILocalFrameBarrier {
  private:
    JNIEnv * m_env;
    int32_t m_refs;
    int32_t m_result;

    jboolean m_isCopy;
    jbyteArray m_jbuf;
    jbyte* m_bytes;

  public:
    JNILocalFrameBarrier(JNIEnv* env, int32_t numReferences) {
        m_env = env;
        m_refs = numReferences;
        m_result = m_env->PushLocalFrame(m_refs);
        m_isCopy = JNI_FALSE;
        m_jbuf = 0;
        m_bytes = NULL;
    }

    void addDependencyRef(jboolean isCopy, jbyteArray jbuf, jbyte* bytes)
    {
        m_isCopy = isCopy;
        m_jbuf = jbuf;
        m_bytes = bytes;
    }

    ~JNILocalFrameBarrier() {
        if (m_isCopy == JNI_TRUE)
        {
            m_env->ReleaseByteArrayElements(m_jbuf, m_bytes, 0);
        }
        // pass jobject* to get pointer to previous frame?
        m_env->PopLocalFrame(NULL);
    }

    int32_t checkResult() {
        return m_result;
    }
};

jmethodID JNITopend::initJavaUserDefinedMethod(const char* name) {
    // if this is the start method, we are going to pass in the functionId
    if (strcmp(name, "callJavaUserDefinedAggregateStart") == 0) {
        return m_jniEnv->GetMethodID(m_jniClass, name, "(I)I");
    }
    // if this is not the start method, we do not have to pass in any parameter
    else {
        return m_jniEnv->GetMethodID(m_jniClass, name, "()I");
    }
    m_jniEnv->ExceptionDescribe();
    throw std::exception();
}

JNITopend::JNITopend(JNIEnv *env, jobject caller) : m_jniEnv(env), m_javaExecutionEngine(caller), m_jniClass(m_jniEnv->GetObjectClass(m_javaExecutionEngine)),
    m_callJavaUserDefinedFunctionMID(initJavaUserDefinedMethod("callJavaUserDefinedFunction")),
    m_callJavaUserDefinedAggregateStartMID(initJavaUserDefinedMethod("callJavaUserDefinedAggregateStart")),
    m_callJavaUserDefinedAggregateAssembleMID(initJavaUserDefinedMethod("callJavaUserDefinedAggregateAssemble")),
    m_callJavaUserDefinedAggregateCombineMID(initJavaUserDefinedMethod("callJavaUserDefinedAggregateCombine")),
    m_callJavaUserDefinedAggregateWorkerEndMID(initJavaUserDefinedMethod("callJavaUserDefinedAggregateWorkerEnd")),
    m_callJavaUserDefinedAggregateCoordinatorEndMID(initJavaUserDefinedMethod("callJavaUserDefinedAggregateCoordinatorEnd")){
    // Cache the method id for better performance. It is valid until the JVM unloads the class:
    // http://java.sun.com/javase/6/docs/technotes/guides/jni/spec/design.html#wp17074
    VOLT_TRACE("found class: %d", m_jniClass == NULL);
    if (m_jniClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_jniClass != 0);
        throw std::exception();
    }

    m_fallbackToEEAllocatedBufferMID =
            m_jniEnv->GetMethodID(
                    m_jniClass,
                    "fallbackToEEAllocatedBuffer",
                    "(Ljava/nio/ByteBuffer;)V");
    if (m_fallbackToEEAllocatedBufferMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_fallbackToEEAllocatedBufferMID != 0);
        throw std::exception();
    }

    m_resizeUDFBufferMID = m_jniEnv->GetMethodID(
            m_jniClass, "resizeUDFBuffer", "(I)V");
    if (m_resizeUDFBufferMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_resizeUDFBufferMID != 0);
        throw std::exception();
    }

    m_nextDependencyMID = m_jniEnv->GetMethodID(m_jniClass, "nextDependencyAsBytes", "(I)[B");
    if (m_nextDependencyMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_nextDependencyMID != 0);
        throw std::exception();
    }

    m_traceLogMID = m_jniEnv->GetMethodID(m_jniClass, "traceLog", "(ZLjava/lang/String;Ljava/lang/String;)V");
    if (m_traceLogMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_traceLogMID != 0);
        throw std::exception();
    }

    m_fragmentProgressUpdateMID = m_jniEnv->GetMethodID(m_jniClass, "fragmentProgressUpdate", "(IIJJJ)J");
    if (m_fragmentProgressUpdateMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_fragmentProgressUpdateMID != 0);
        throw std::exception();
    }

    m_planForFragmentIdMID = m_jniEnv->GetMethodID(m_jniClass, "planForFragmentId", "(J)[B");
    if (m_planForFragmentIdMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_planForFragmentIdMID != 0);
        throw std::exception();
    }

    m_crashVoltDBMID =
        m_jniEnv->GetStaticMethodID(
            m_jniClass,
            "crashVoltDB",
            "(Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;I)V");
    if (m_crashVoltDBMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_crashVoltDBMID != NULL);
        throw std::exception();
    }

    m_exportManagerClass = m_jniEnv->FindClass("org/voltdb/export/ExportManager");
    if (m_exportManagerClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_exportManagerClass != NULL);
        throw std::exception();
    }

    m_exportManagerClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_exportManagerClass));
    if (m_exportManagerClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_exportManagerClass != NULL);
        throw std::exception();
    }

    m_pushExportBufferMID = m_jniEnv->GetStaticMethodID(
            m_exportManagerClass,
            "pushExportBuffer",
            "(ILjava/lang/String;JJJJJJLorg/voltcore/utils/DBBPool$BBContainer;)V");
    if (m_pushExportBufferMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_pushExportBufferMID != NULL);
        throw std::exception();
    }

    m_partitionDRGatewayClass = m_jniEnv->FindClass("org/voltdb/PartitionDRGateway");
    if (m_partitionDRGatewayClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_partitionDRGatewayClass != NULL);
        throw std::exception();
    }

    m_partitionDRGatewayClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_partitionDRGatewayClass));
    if (m_partitionDRGatewayClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_partitionDRGatewayClass != NULL);
        throw std::exception();
    }

    m_pushDRBufferMID = m_jniEnv->GetStaticMethodID(
            m_partitionDRGatewayClass,
            "pushDRBuffer",
            "(IJJJJJILorg/voltcore/utils/DBBPool$BBContainer;)J");

    m_reportDRBufferMID = m_jniEnv->GetStaticMethodID(
            m_partitionDRGatewayClass,
            "reportDRBuffer",
            "(ILjava/lang/String;Ljava/nio/ByteBuffer;)V"
    );

    m_pushPoisonPillMID = m_jniEnv->GetStaticMethodID(
            m_partitionDRGatewayClass,
            "pushPoisonPill",
            "(ILjava/lang/String;Lorg/voltcore/utils/DBBPool$BBContainer;)V");

    if (m_pushDRBufferMID == NULL || m_pushPoisonPillMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_pushDRBufferMID != NULL && m_pushPoisonPillMID != NULL);
        throw std::exception();
    }


    m_drConflictReporterClass = m_jniEnv->FindClass("org/voltdb/DRConflictReporter");
    if (m_drConflictReporterClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_drConflictReporterClass != NULL);
        throw std::exception();
    }

    m_drConflictReporterClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_drConflictReporterClass));
    if (m_drConflictReporterClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_drConflictReporterClass != NULL);
        throw std::exception();
    }

    m_reportDRConflictMID = m_jniEnv->GetStaticMethodID(
            m_drConflictReporterClass,
            "reportDRConflict",
            "(IIJLjava/lang/String;ZIILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;ILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I");
    if (m_reportDRConflictMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_reportDRConflictMID != NULL);
        throw std::exception();
    }

    m_decompressionClass = m_jniEnv->FindClass("org/voltdb/utils/CompressionService");
    if (m_decompressionClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_decompressionClass != NULL);
        throw std::exception();
    }

    m_decompressionClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_decompressionClass));
    if (m_decompressionClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_decompressionClass != NULL);
        throw std::exception();
    }

    m_decodeBase64AndDecompressToBytesMID = m_jniEnv->GetStaticMethodID(
            m_decompressionClass,
            "decodeBase64AndDecompressToBytes",
            "(Ljava/lang/String;)[B");
    if (m_decodeBase64AndDecompressToBytesMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_decodeBase64AndDecompressToBytesMID != NULL);
        throw std::exception();
    }

    m_storeLargeTempTableBlockMID = m_jniEnv->GetMethodID(m_jniClass,
                                                          "storeLargeTempTableBlock",
                                                          "(JJLjava/nio/ByteBuffer;)Z");
    if (m_storeLargeTempTableBlockMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_storeLargeTempTableBlockMID != 0);
        throw std::exception();
    }

    m_loadLargeTempTableBlockMID = m_jniEnv->GetMethodID(m_jniClass,
                                                         "loadLargeTempTableBlock",
                                                          "(JJLjava/nio/ByteBuffer;)Z");
    if (m_loadLargeTempTableBlockMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_loadLargeTempTableBlockMID != 0);
        throw std::exception();
    }

    m_releaseLargeTempTableBlockMID = m_jniEnv->GetMethodID(m_jniClass,
                                                            "releaseLargeTempTableBlock",
                                                            "(JJ)Z");
    if (m_releaseLargeTempTableBlockMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_releaseLargeTempTableBlockMID != 0);
        throw std::exception();
    }

    // ByteBuffers allocated by EE must be discarded properly, so return them
    // wrapped in a NDBBWrapperContainer.
    m_NDBBWClass = m_jniEnv->FindClass("org/voltcore/utils/DBBPool$NDBBWrapperContainer");
    if (m_NDBBWClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_NDBBWClass != NULL);
        throw std::exception();
    }

    m_NDBBWClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_NDBBWClass));
    if (m_NDBBWClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_NDBBWClass != NULL);
        throw std::exception();
    }

    m_NDBBWConstructorMID = m_jniEnv->GetMethodID(m_NDBBWClass, "<init>", "(Ljava/nio/ByteBuffer;)V");
    if (m_NDBBWConstructorMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        vassert(m_NDBBWConstructorMID != 0);
        throw std::exception();
    }
}


void JNITopend::fallbackToEEAllocatedBuffer(char *buffer, size_t length) {
    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 1);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("Unable to load dependency: jni frame error.");
        throw std::exception();
    }

    jobject jbuffer = m_jniEnv->NewDirectByteBuffer(buffer, length);
    if (jbuffer == NULL) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }

    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_fallbackToEEAllocatedBufferMID, jbuffer);
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
}

int JNITopend::loadNextDependency(int32_t dependencyId, voltdb::Pool *stringPool, Table* destination) {
    VOLT_DEBUG("iterating java dependency for id %d", dependencyId);

    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 10);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("Unable to load dependency: jni frame error.");
        throw std::exception();
    }

    jbyteArray jbuf = (jbyteArray)(m_jniEnv->CallObjectMethod(m_javaExecutionEngine,
                                                              m_nextDependencyMID,
                                                              dependencyId));

    if (!jbuf) {
        return 0;
    }

    jsize length = m_jniEnv->GetArrayLength(jbuf);
    if (length > 0) {
        jboolean is_copy;
        jbyte *bytes = m_jniEnv->GetByteArrayElements(jbuf, &is_copy);
        // Add the dependency buffer info to the stack object
        // so it'll get cleaned up if loadTuplesFrom throws
        jni_frame.addDependencyRef(is_copy, jbuf, bytes);
        ReferenceSerializeInputBE serialize_in(bytes, length);
        destination->loadTuplesFrom(serialize_in, stringPool);
        return 1;
    }
    else {
        return 0;
    }
}

void JNITopend::traceLog(bool isBegin, const char *name, const char *args) {
    jstring nameStr = m_jniEnv->NewStringUTF(name);
    jstring argsStr = m_jniEnv->NewStringUTF(args);

    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_traceLogMID,
                             isBegin ? JNI_TRUE : JNI_FALSE, nameStr, argsStr);

    m_jniEnv->DeleteLocalRef(nameStr);
    m_jniEnv->DeleteLocalRef(argsStr);

    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
}

int64_t JNITopend::fragmentProgressUpdate(
                int32_t batchIndex,
                PlanNodeType planNodeType,
                int64_t tuplesProcessed,
                int64_t currMemoryInBytes,
                int64_t peakMemoryInBytes) {
    jlong nextStep = m_jniEnv->CallLongMethod(m_javaExecutionEngine,
                                              m_fragmentProgressUpdateMID,
                                              batchIndex,
                                              static_cast<int32_t>(planNodeType),
                                              tuplesProcessed,
                                              currMemoryInBytes,
                                              peakMemoryInBytes);
    return (int64_t)nextStep;
}

// A local helper to convert a jbyteArray to an std::string.
// Callers should be aware that an empty string may be returned if
// jbuf is null.
static std::string jbyteArrayToStdString(JNIEnv* jniEnv,
                                         JNILocalFrameBarrier& jniFrame,
                                         jbyteArray jbuf) {

    if (!jbuf)
        return "";

    jsize length = jniEnv->GetArrayLength(jbuf);
    if (length > 0) {
        jboolean isCopy;
        jbyte *bytes = jniEnv->GetByteArrayElements(jbuf, &isCopy);
        jniFrame.addDependencyRef(isCopy, jbuf, bytes);
        return std::string(reinterpret_cast<char*>(bytes), length);
    }

    return "";
 }

std::string JNITopend::planForFragmentId(int64_t fragmentId) {
    VOLT_DEBUG("fetching plan for id %d", (int) fragmentId);

    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 10);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("Unable to load dependency: jni frame error.");
        throw std::exception();
    }

    jbyteArray jbuf = (jbyteArray)(m_jniEnv->CallObjectMethod(m_javaExecutionEngine,
                                                              m_planForFragmentIdMID,
                                                              fragmentId));
    // jbuf might be NULL or might have 0 length here.  In that case
    // we'll return a 0-length string to the caller, who will return
    // an appropriate error.
    return jbyteArrayToStdString(m_jniEnv, jni_frame, jbuf);
}

std::string JNITopend::decodeBase64AndDecompress(const std::string& base64Str) {
    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 2);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("Unable to load dependency: jni frame error.");
        throw std::exception();
    }

    jstring jBase64Str = m_jniEnv->NewStringUTF(base64Str.c_str());
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }

    jbyteArray jbuf = (jbyteArray)m_jniEnv->CallStaticObjectMethod(m_decompressionClass,
                                                                   m_decodeBase64AndDecompressToBytesMID,
                                                                   jBase64Str);
    return jbyteArrayToStdString(m_jniEnv, jni_frame, jbuf);
}

bool JNITopend::storeLargeTempTableBlock(LargeTempTableBlock* block) {
    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 1);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("JNI frame error");
        throw std::exception();
    }

    std::unique_ptr<char[]> storage = block->releaseData();
    jobject blockByteBuffer = m_jniEnv->NewDirectByteBuffer(storage.get(), LargeTempTableBlock::BLOCK_SIZE_IN_BYTES);
    if (blockByteBuffer == NULL) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }

    LargeTempTableBlockId blockId = block->id();
    jboolean success = m_jniEnv->CallBooleanMethod(m_javaExecutionEngine,
                                                   m_storeLargeTempTableBlockMID,
                                                   blockId.getSiteId(),
                                                   blockId.getBlockCounter(),
                                                   blockByteBuffer);
    // It's assumed that when control returns to this method the block
    // will have been persisted to disk.  The memory for the block
    // will be returned to the OS when above unique_ptr goes out of
    // scope.
    return success;
}

bool JNITopend::loadLargeTempTableBlock(LargeTempTableBlock* block) {
    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 1);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("JNI frame error");
        throw std::exception();
    }

    // Memory allocation should really be done by LargeTempTableBLock
    // cache, and it should pass in the storage for the loaded block.
    std::unique_ptr<char[]> storage(new char[LargeTempTableBlock::BLOCK_SIZE_IN_BYTES]);
    jobject blockByteBuffer = m_jniEnv->NewDirectByteBuffer(storage.get(), LargeTempTableBlock::BLOCK_SIZE_IN_BYTES);
    if (blockByteBuffer == NULL) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }

    bool success = m_jniEnv->CallBooleanMethod(m_javaExecutionEngine,
                                               m_loadLargeTempTableBlockMID,
                                               block->id(),
                                               blockByteBuffer);
    if (success) {
        block->setData(std::move(storage));
    }

    return success;
}

bool JNITopend::releaseLargeTempTableBlock(LargeTempTableBlockId blockId) {
    jboolean success = (jboolean)m_jniEnv->CallBooleanMethod(m_javaExecutionEngine,
                                                             m_releaseLargeTempTableBlockMID,
                                                             blockId.getSiteId(),
                                                             blockId.getBlockCounter());
    return success;
}

int32_t JNITopend::callJavaUserDefinedFunction() {
    return (int32_t)m_jniEnv->CallIntMethod(m_javaExecutionEngine,
                                            m_callJavaUserDefinedFunctionMID);
}

int32_t JNITopend::callJavaUserDefinedAggregateStart(int functionId) {
    return (int32_t)m_jniEnv->CallIntMethod(m_javaExecutionEngine,
                                            m_callJavaUserDefinedAggregateStartMID,
                                            functionId);
}

int32_t JNITopend::callJavaUserDefinedAggregateAssemble() {
    return (int32_t)m_jniEnv->CallIntMethod(m_javaExecutionEngine,
                                            m_callJavaUserDefinedAggregateAssembleMID);
}

int32_t JNITopend::callJavaUserDefinedAggregateCombine() {
    return (int32_t)m_jniEnv->CallIntMethod(m_javaExecutionEngine,
                                            m_callJavaUserDefinedAggregateCombineMID);
}

int32_t JNITopend::callJavaUserDefinedAggregateWorkerEnd() {
    return (int32_t)m_jniEnv->CallIntMethod(m_javaExecutionEngine,
                                            m_callJavaUserDefinedAggregateWorkerEndMID);
}

int32_t JNITopend::callJavaUserDefinedAggregateCoordinatorEnd() {
    return (int32_t)m_jniEnv->CallIntMethod(m_javaExecutionEngine,
                                            m_callJavaUserDefinedAggregateCoordinatorEndMID);
}

void JNITopend::resizeUDFBuffer(int32_t size) {
    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_resizeUDFBufferMID, size);
}

void JNITopend::crashVoltDB(FatalException e) {
    //Enough references for the reason string, traces array, and traces strings
    JNILocalFrameBarrier jni_frame =
            JNILocalFrameBarrier(
                    m_jniEnv,
                    static_cast<int32_t>(e.m_traces.size()) + 4);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("Unable to load dependency: jni frame error.");
        throw std::exception();
    }
    jstring jReason = m_jniEnv->NewStringUTF(e.m_reason.c_str());
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
    jstring jFilename = m_jniEnv->NewStringUTF(e.m_filename);
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
    jobjectArray jTracesArray =
            m_jniEnv->NewObjectArray(
                    static_cast<jsize>(e.m_traces.size()),
                    m_jniEnv->FindClass("java/lang/String"),
                    NULL);
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
    for (int ii = 0; ii < e.m_traces.size(); ii++) {
        jstring traceString = m_jniEnv->NewStringUTF(e.m_traces[ii].c_str());
        m_jniEnv->SetObjectArrayElement( jTracesArray, ii, traceString);
    }
    m_jniEnv->CallStaticVoidMethod(
            m_jniEnv->GetObjectClass(m_javaExecutionEngine),
            m_crashVoltDBMID,
            jReason,
            jTracesArray,
            jFilename,
            static_cast<int32_t>(e.m_lineno));
    throw std::exception();
}

JNITopend::~JNITopend() {
    m_jniEnv->DeleteGlobalRef(m_javaExecutionEngine);
    m_jniEnv->DeleteGlobalRef(m_exportManagerClass);
    m_jniEnv->DeleteGlobalRef(m_partitionDRGatewayClass);
    m_jniEnv->DeleteGlobalRef(m_drConflictReporterClass);
    m_jniEnv->DeleteGlobalRef(m_decompressionClass);
    m_jniEnv->DeleteGlobalRef(m_NDBBWClass);
}

void JNITopend::pushExportBuffer(
        int32_t partitionId,
        string tableName,
        ExportStreamBlock *block) {
    jstring tableNameString = m_jniEnv->NewStringUTF(tableName.c_str());

    if (block != NULL) {
        jobject container = getDirectBufferContainer(block->rawPtr(), block->rawLength());
        m_jniEnv->CallStaticVoidMethod(
                m_exportManagerClass,
                m_pushExportBufferMID,
                partitionId,
                tableNameString,
                block->startSequenceNumber(),
                block->getCommittedSequenceNumber(),
                block->getRowCount(),
                block->lastSpUniqueId(),
                block->lastCommittedSpHandle(),
                reinterpret_cast<jlong>(block->rawPtr()),
                container);
        m_jniEnv->DeleteLocalRef(container);
        delete block;
    } else {
        m_jniEnv->CallStaticVoidMethod(
                m_exportManagerClass,
                m_pushExportBufferMID,
                partitionId,
                tableNameString,
                static_cast<int64_t>(0),
                static_cast<int64_t>(0),
                static_cast<int64_t>(0),
                static_cast<int64_t>(0),
                static_cast<int64_t>(0),
                NULL,
                NULL);
    }
    m_jniEnv->DeleteLocalRef(tableNameString);
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
}

int64_t JNITopend::pushDRBuffer(int32_t partitionId, DrStreamBlock *block) {
    int64_t retval = -1;
    if (block != NULL) {
        jobject container = getDirectBufferContainer(block->rawPtr(), block->rawLength());
        retval = m_jniEnv->CallStaticLongMethod(
                m_partitionDRGatewayClass,
                m_pushDRBufferMID,
                partitionId,
                block->lastCommittedSpHandle(),
                block->startDRSequenceNumber(),
                block->lastDRSequenceNumber(),
                block->lastSpUniqueId(),
                block->lastMpUniqueId(),
                block->drEventType(),
                container);
        m_jniEnv->DeleteLocalRef(container);
        delete block;
    }
    return retval;
}

void JNITopend::reportDRBuffer(int32_t partitionId, const char *reason, const char *buffer, size_t length) {

    if (buffer != NULL) {
        jstring jReason = m_jniEnv->NewStringUTF(reason);
        if (jReason == NULL) {
            m_jniEnv->ExceptionDescribe();
            throw std::exception();
        }
        jobject jbuffer = m_jniEnv->NewDirectByteBuffer(const_cast<char *>(buffer), length);
        if (jbuffer == NULL) {
            m_jniEnv->ExceptionDescribe();
            throw std::exception();
        }
        m_jniEnv->CallStaticLongMethod(
                m_partitionDRGatewayClass,
                m_reportDRBufferMID,
                partitionId, jReason, jbuffer, length);
        m_jniEnv->DeleteLocalRef(jbuffer);
        m_jniEnv->DeleteLocalRef(jReason);
    }
}

void JNITopend::pushPoisonPill(int32_t partitionId, std::string& reason, DrStreamBlock *block) {
    jstring jReason = m_jniEnv->NewStringUTF(reason.c_str());

    if (block != NULL) {
        jobject container = getDirectBufferContainer(block->rawPtr(), block->rawLength());
        m_jniEnv->CallStaticLongMethod(
                m_partitionDRGatewayClass,
                m_pushPoisonPillMID,
                partitionId,
                jReason,
                container);
        m_jniEnv->DeleteLocalRef(container);
        delete block;
    }
    m_jniEnv->DeleteLocalRef(jReason);
}

static boost::shared_array<char> serializeToDirectByteBuffer(JNIEnv *jniEngine, Table *table, jobject &byteBuffer) {
    if (table) {
        size_t serializeSize = table->getAccurateSizeToSerialize();
        boost::shared_array<char> backingArray(new char[serializeSize]);
        ReferenceSerializeOutput conflictSerializeOutput(backingArray.get(), serializeSize);
        table->serializeToWithoutTotalSize(conflictSerializeOutput);
        byteBuffer = jniEngine->NewDirectByteBuffer(static_cast<void*>(backingArray.get()),
                                                            static_cast<int32_t>(serializeSize));
        if (byteBuffer == NULL) {
            jniEngine->ExceptionDescribe();
            throw std::exception();
        }
        return backingArray;
    }
    return boost::shared_array<char>();
}

int JNITopend::reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp,
        std::string tableName, bool isReplicatedTable, DRRecordType action,
        DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
        Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
        DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
        Table *newMetaTableForInsert, Table *newTupleTableForInsert) {
    // prepare tablename
    jstring tableNameString = m_jniEnv->NewStringUTF(tableName.c_str());

    // prepare input buffers for delete conflict - these buffers only accessed in RO mode in java upcall,
    // hence Java doesn't need to discard them explicitly

    jobject existingMetaRowsBufferForDelete = NULL;
    boost::shared_array<char> existingMetaArrayForDelete = serializeToDirectByteBuffer(m_jniEnv,
                                                                                       existingMetaTableForDelete,
                                                                                       existingMetaRowsBufferForDelete);

    jobject existingTupleRowsBufferForDelete = NULL;
    boost::shared_array<char> existingTupleArrayForDelete = serializeToDirectByteBuffer(m_jniEnv,
                                                                                        existingTupleTableForDelete,
                                                                                        existingTupleRowsBufferForDelete);
    jobject expectedMetaRowsBufferForDelete = NULL;
    boost::shared_array<char> expectedMetaArrayForDelete = serializeToDirectByteBuffer(m_jniEnv,
                                                                                       expectedMetaTableForDelete,
                                                                                       expectedMetaRowsBufferForDelete);

    jobject expectedTupleRowsBufferForDelete = NULL;
    boost::shared_array<char> expectedTupleArrayForDelete = serializeToDirectByteBuffer(m_jniEnv,
                                                                                        expectedTupleTableForDelete,
                                                                                        expectedTupleRowsBufferForDelete);

    jobject existingMetaRowsBufferForInsert = NULL;
    boost::shared_array<char> existingMetaArrayForInsert = serializeToDirectByteBuffer(m_jniEnv,
                                                                                       existingMetaTableForInsert,
                                                                                       existingMetaRowsBufferForInsert);

    jobject existingTupleRowsBufferForInsert = NULL;
    boost::shared_array<char> existingTupleArrayForInsert = serializeToDirectByteBuffer(m_jniEnv,
                                                                                        existingTupleTableForInsert,
                                                                                        existingTupleRowsBufferForInsert);

    jobject newMetaRowsBufferForInsert = NULL;
    boost::shared_array<char> newMetaArrayForInsert = serializeToDirectByteBuffer(m_jniEnv,
                                                                                  newMetaTableForInsert,
                                                                                  newMetaRowsBufferForInsert);

    jobject newTupleRowsBufferForInsert = NULL;
    boost::shared_array<char> newTupleArrayForInsert = serializeToDirectByteBuffer(m_jniEnv,
                                                                                   newTupleTableForInsert,
                                                                                   newTupleRowsBufferForInsert);

    int32_t retval = m_jniEnv->CallStaticIntMethod(m_drConflictReporterClass,
                                            m_reportDRConflictMID,
                                            partitionId,
                                            remoteClusterId,
                                            remoteTimestamp,
                                            tableNameString,
                                            isReplicatedTable,
                                            action,
                                            deleteConflict,
                                            existingMetaRowsBufferForDelete,
                                            existingTupleRowsBufferForDelete,
                                            expectedMetaRowsBufferForDelete,
                                            expectedTupleRowsBufferForDelete,
                                            insertConflict,
                                            existingMetaRowsBufferForInsert,
                                            existingTupleRowsBufferForInsert,
                                            newMetaRowsBufferForInsert,
                                            newTupleRowsBufferForInsert);

    m_jniEnv->DeleteLocalRef(tableNameString);
    m_jniEnv->DeleteLocalRef(existingMetaRowsBufferForDelete);
    m_jniEnv->DeleteLocalRef(existingTupleRowsBufferForDelete);
    m_jniEnv->DeleteLocalRef(expectedMetaRowsBufferForDelete);
    m_jniEnv->DeleteLocalRef(expectedTupleRowsBufferForDelete);
    m_jniEnv->DeleteLocalRef(existingMetaRowsBufferForInsert);
    m_jniEnv->DeleteLocalRef(existingTupleRowsBufferForInsert);
    m_jniEnv->DeleteLocalRef(newMetaRowsBufferForInsert);
    m_jniEnv->DeleteLocalRef(newTupleRowsBufferForInsert);

    return retval;
}
}
