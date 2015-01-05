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
#include "JNITopend.h"
#include <cassert>
#include <iostream>

#include "common/debuglog.h"
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

JNITopend::JNITopend(JNIEnv *env, jobject caller) : m_jniEnv(env), m_javaExecutionEngine(caller) {
    // Cache the method id for better performance. It is valid until the JVM unloads the class:
    // http://java.sun.com/javase/6/docs/technotes/guides/jni/spec/design.html#wp17074
    jclass jniClass = m_jniEnv->GetObjectClass(m_javaExecutionEngine);
    VOLT_TRACE("found class: %d", jniClass == NULL);
    if (jniClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(jniClass != 0);
        throw std::exception();
    }

    m_fallbackToEEAllocatedBufferMID =
            m_jniEnv->GetMethodID(
                    jniClass,
                    "fallbackToEEAllocatedBuffer",
                    "(Ljava/nio/ByteBuffer;)V");
    if (m_fallbackToEEAllocatedBufferMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_fallbackToEEAllocatedBufferMID != 0);
        throw std::exception();
    }

    m_nextDependencyMID = m_jniEnv->GetMethodID(jniClass, "nextDependencyAsBytes", "(I)[B");
    if (m_nextDependencyMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_nextDependencyMID != 0);
        throw std::exception();
    }

    m_fragmentProgressUpdateMID = m_jniEnv->GetMethodID(jniClass, "fragmentProgressUpdate", "(ILjava/lang/String;Ljava/lang/String;JJJJ)J");
    if (m_fragmentProgressUpdateMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_fragmentProgressUpdateMID != 0);
        throw std::exception();
    }

    m_planForFragmentIdMID = m_jniEnv->GetMethodID(jniClass, "planForFragmentId", "(J)[B");
    if (m_planForFragmentIdMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_planForFragmentIdMID != 0);
        throw std::exception();
    }

    m_crashVoltDBMID =
        m_jniEnv->GetStaticMethodID(
            jniClass,
            "crashVoltDB",
            "(Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;I)V");
    if (m_crashVoltDBMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_crashVoltDBMID != NULL);
        throw std::exception();
    }

    m_exportManagerClass = m_jniEnv->FindClass("org/voltdb/export/ExportManager");
    if (m_exportManagerClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_exportManagerClass != NULL);
        throw std::exception();
    }

    m_exportManagerClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_exportManagerClass));
    if (m_exportManagerClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_exportManagerClass != NULL);
        throw std::exception();
    }

    m_pushExportBufferMID = m_jniEnv->GetStaticMethodID(
            m_exportManagerClass,
            "pushExportBuffer",
            "(JILjava/lang/String;JJLjava/nio/ByteBuffer;ZZ)V");
    if (m_pushExportBufferMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_pushExportBufferMID != NULL);
        throw std::exception();
    }

    m_getQueuedExportBytesMID = m_jniEnv->GetStaticMethodID(
            m_exportManagerClass,
            "getQueuedExportBytes",
            "(ILjava/lang/String;)J");
    if (m_getQueuedExportBytesMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_getQueuedExportBytesMID != NULL);
        throw std::exception();
    }

    m_partitionDRGatewayClass = m_jniEnv->FindClass("org/voltdb/PartitionDRGateway");
    if (m_partitionDRGatewayClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_partitionDRGatewayClass != NULL);
        throw std::exception();
    }

    m_partitionDRGatewayClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_partitionDRGatewayClass));
    if (m_partitionDRGatewayClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_partitionDRGatewayClass != NULL);
        throw std::exception();
    }

    m_pushDRBufferMID = m_jniEnv->GetStaticMethodID(
            m_partitionDRGatewayClass,
            "pushDRBuffer",
            "(ILjava/nio/ByteBuffer;)V");
    if (m_pushDRBufferMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_pushDRBufferMID != NULL);
        throw std::exception();
    }

    m_encoderClass = m_jniEnv->FindClass("org/voltdb/utils/Encoder");
    if (m_encoderClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_encoderClass != NULL);
        throw std::exception();
    }

    m_encoderClass = static_cast<jclass>(m_jniEnv->NewGlobalRef(m_encoderClass));
    if (m_encoderClass == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_encoderClass != NULL);
        throw std::exception();
    }

    m_decodeBase64AndDecompressToBytesMID = m_jniEnv->GetStaticMethodID(
            m_encoderClass,
            "decodeBase64AndDecompressToBytes",
            "(Ljava/lang/String;)[B");
    if (m_decodeBase64AndDecompressToBytesMID == NULL) {
        m_jniEnv->ExceptionDescribe();
        assert(m_decodeBase64AndDecompressToBytesMID != NULL);
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

int64_t JNITopend::fragmentProgressUpdate(int32_t batchIndex,
                std::string planNodeName,
                std::string targetTableName,
                int64_t targetTableSize,
                int64_t tuplesProcessed,
                int64_t currMemoryInBytes,
                int64_t peakMemoryInBytes) {
        JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 10);
        if (jni_frame.checkResult() < 0) {
                VOLT_ERROR("Unable to load dependency: jni frame error.");
                throw std::exception();
        }

        jstring jPlanNodeName = m_jniEnv->NewStringUTF(planNodeName.c_str());
        if (m_jniEnv->ExceptionCheck()) {
                m_jniEnv->ExceptionDescribe();
                throw std::exception();
        }
        jstring jTargetTableName = m_jniEnv->NewStringUTF(targetTableName.c_str());
        if (m_jniEnv->ExceptionCheck()) {
                m_jniEnv->ExceptionDescribe();
                throw std::exception();
        }

    jlong nextStep = m_jniEnv->CallLongMethod(m_javaExecutionEngine,m_fragmentProgressUpdateMID,
                batchIndex, jPlanNodeName, jTargetTableName, targetTableSize, tuplesProcessed,
                currMemoryInBytes, peakMemoryInBytes);
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

    jbyteArray jbuf = (jbyteArray)m_jniEnv->CallStaticObjectMethod(m_encoderClass,
                                                                   m_decodeBase64AndDecompressToBytesMID,
                                                                   jBase64Str);
    return jbyteArrayToStdString(m_jniEnv, jni_frame, jbuf);
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
}

int64_t JNITopend::getQueuedExportBytes(int32_t partitionId, string signature) {
    jstring signatureString = m_jniEnv->NewStringUTF(signature.c_str());
    int64_t retval = m_jniEnv->CallStaticLongMethod(
            m_exportManagerClass,
            m_getQueuedExportBytesMID,
            partitionId,
            signatureString);
    m_jniEnv->DeleteLocalRef(signatureString);
    return retval;
}

void JNITopend::pushExportBuffer(
        int64_t exportGeneration,
        int32_t partitionId,
        string signature,
        StreamBlock *block,
        bool sync,
        bool endOfStream) {
    jstring signatureString = m_jniEnv->NewStringUTF(signature.c_str());
    if (block != NULL) {
        jobject buffer = m_jniEnv->NewDirectByteBuffer( block->rawPtr(), block->rawLength());
        if (buffer == NULL) {
            m_jniEnv->ExceptionDescribe();
            throw std::exception();
        }
        //std::cout << "Block is length " << block->rawLength() << std::endl;
        m_jniEnv->CallStaticVoidMethod(
                m_exportManagerClass,
                m_pushExportBufferMID,
                exportGeneration,
                partitionId,
                signatureString,
                block->uso(),
                reinterpret_cast<jlong>(block->rawPtr()),
                buffer,
                sync ? JNI_TRUE : JNI_FALSE,
                endOfStream ? JNI_TRUE : JNI_FALSE);
        m_jniEnv->DeleteLocalRef(buffer);
    } else {
        //std::cout << "Block is null" << std::endl;
        m_jniEnv->CallStaticVoidMethod(
                        m_exportManagerClass,
                        m_pushExportBufferMID,
                        exportGeneration,
                        partitionId,
                        signatureString,
                        static_cast<int64_t>(0),
                        NULL,
                        NULL,
                        sync ? JNI_TRUE : JNI_FALSE,
                        endOfStream ? JNI_TRUE : JNI_FALSE);
    }
    m_jniEnv->DeleteLocalRef(signatureString);
    if (m_jniEnv->ExceptionCheck()) {
        m_jniEnv->ExceptionDescribe();
        throw std::exception();
    }
}

void JNITopend::pushDRBuffer(int32_t partitionId, StreamBlock *block) {
    if (block != NULL) {
        jobject buffer = m_jniEnv->NewDirectByteBuffer( block->rawPtr(), block->rawLength());
        if (buffer == NULL) {
            m_jniEnv->ExceptionDescribe();
            throw std::exception();
        }
        //std::cout << "Block is length " << block->rawLength() << std::endl;
        m_jniEnv->CallStaticVoidMethod(
                m_partitionDRGatewayClass,
                m_pushDRBufferMID,
                partitionId,
                buffer);
        m_jniEnv->DeleteLocalRef(buffer);
    }
}
}
