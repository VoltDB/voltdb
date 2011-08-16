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

#ifndef JNITOPEND_H_
#define JNITOPEND_H_
#include "common/Topend.h"
#include "common/FatalException.hpp"
#include "common/Pool.hpp"
#include <jni.h>

namespace voltdb {

class JNITopend : public Topend {
public:
    JNITopend(JNIEnv *env, jobject caller);
    ~JNITopend();

    inline JNITopend* updateJNIEnv(JNIEnv *env) { m_jniEnv = env; return this; }
    int loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination);
    void crashVoltDB(FatalException e);
    int64_t getQueuedExportBytes(int32_t partitionId, std::string signature);
    void pushExportBuffer(
            int64_t exportGeneration,
            int32_t partitionId,
            std::string signature,
            StreamBlock *block,
            bool sync,
            bool endOfStream);
    void fallbackToEEAllocatedBuffer(char *buffer, size_t length);
private:
    JNIEnv *m_jniEnv;

    /**
     * JNI object corresponding to this engine. for callback functions.
     * if this is NULL, VoltDBEngine will fail to call sendDependency().
    */
    jobject m_javaExecutionEngine;
    jmethodID m_fallbackToEEAllocatedBufferMID;
    jmethodID m_nextDependencyMID;
    jmethodID m_crashVoltDBMID;
    jmethodID m_pushExportBufferMID;
    jmethodID m_getQueuedExportBytesMID;
    jclass m_exportManagerClass;
};

}
#endif /* JNITOPEND_H_ */
