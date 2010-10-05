/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
#include "JNITopend.h"
#include <cassert>

#include "common/debuglog.h"
#include "storage/table.h"


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

    m_nextDependencyMID = m_jniEnv->GetMethodID(jniClass, "nextDependencyAsBytes", "(I)[B");
    assert(m_nextDependencyMID != 0);

    m_crashVoltDBMID =
        m_jniEnv->GetStaticMethodID(
            jniClass,
            "crashVoltDB",
            "(Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;I)V");
    assert(m_crashVoltDBMID != 0);

    if (m_nextDependencyMID == 0 ||
        m_crashVoltDBMID == 0)
    {
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
        ReferenceSerializeInput serialize_in(bytes, length);
        destination->loadTuplesFrom(true, serialize_in, stringPool);
        return 1;
    }
    else {
        return 0;
    }
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

}
