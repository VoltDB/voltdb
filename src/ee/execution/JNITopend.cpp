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

  public:
    JNILocalFrameBarrier(JNIEnv* env, int32_t numReferences) {
        m_env = env;
        m_refs = numReferences;
        m_result = m_env->PushLocalFrame(m_refs);
    }

    ~JNILocalFrameBarrier() {
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

    // get the methods for EL and buffer management and cache them as well
    m_handoffReadyELBufferMID = m_jniEnv->GetMethodID(jniClass, "handoffReadyELBuffer", "(JII)V");
    assert(m_handoffReadyELBufferMID != 0);
    m_claimManagedBufferMID = m_jniEnv->GetMethodID(jniClass, "claimManagedBuffer", "(I)J");
    assert(m_claimManagedBufferMID != 0);
    m_releaseManagedBufferMID = m_jniEnv->GetMethodID(jniClass, "releaseManagedBuffer", "(J)V");
    assert(m_releaseManagedBufferMID != 0);
    m_nextDependencyMID = m_jniEnv->GetMethodID(jniClass, "nextDependencyAsBytes", "(I)[B");
    assert(m_nextDependencyMID != 0);
}

void JNITopend::handoffReadyELBuffer(char* bufferPtr, int32_t bytesUsed, CatalogId tableId) {
    assert(bufferPtr);
    assert(bytesUsed > 0);
    assert(tableId > 0);

    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_handoffReadyELBufferMID,
        reinterpret_cast<int64_t>(bufferPtr), bytesUsed, static_cast<int32_t>(tableId));
}

char* JNITopend::claimManagedBuffer(int32_t desiredSizeInBytes) {
    assert(desiredSizeInBytes > 0);
    assert(m_jniEnv);

    int64_t result = m_jniEnv->CallLongMethod(m_javaExecutionEngine, m_claimManagedBufferMID, desiredSizeInBytes);
    assert(reinterpret_cast<char*>(result));
    return reinterpret_cast<char*>(result);
}

void JNITopend::releaseManagedBuffer(char* bufferPtr) {
    assert(bufferPtr);

    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_releaseManagedBufferMID, reinterpret_cast<int64_t>(bufferPtr));
}

int JNITopend::loadNextDependency(int32_t dependencyId, voltdb::Pool *stringPool, Table* destination) {
    VOLT_DEBUG("iterating java dependency for id %d", dependencyId);

    JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 10);
    if (jni_frame.checkResult() < 0) {
        VOLT_ERROR("Unable to load dependency: jni frame error.");
        assert(false);
        return 0;
    }

    jbyteArray jbuf = (jbyteArray)(m_jniEnv->CallObjectMethod(m_javaExecutionEngine,
                                                              m_nextDependencyMID,
                                                              dependencyId));

    if (!jbuf) {
        return 0;
    }

    jsize length = m_jniEnv->GetArrayLength(jbuf);
    if (length > 0) {
        jbyte *bytes = m_jniEnv->GetByteArrayElements(jbuf, NULL);
        ReferenceSerializeInput serialize_in(bytes, length);
        destination->loadTuplesFrom(true, serialize_in, stringPool);
        return 1;
    }
    else {
        return 0;
    }
}

}
