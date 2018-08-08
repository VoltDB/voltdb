/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

#ifndef JNITOPEND_H_
#define JNITOPEND_H_
#include <jni.h>

#include "boost/shared_array.hpp"
#include "common/Topend.h"
#include "common/FatalException.hpp"
#include "common/LargeTempTableBlockId.hpp"
#include "common/Pool.hpp"

namespace voltdb {

class JNITopend : public Topend {
    JNIEnv *m_jniEnv;

    /**
     * JNI object corresponding to this engine. for callback functions.
     * if this is NULL, VoltDBEngine will fail to call sendDependency().
    */
    jobject m_javaExecutionEngine;
    jmethodID m_fallbackToEEAllocatedBufferMID;
    jmethodID m_nextDependencyMID;
    jmethodID m_traceLogMID;
    jmethodID m_fragmentProgressUpdateMID;
    jmethodID m_planForFragmentIdMID;
    jmethodID m_crashVoltDBMID;
    jmethodID m_pushExportBufferMID;
    jmethodID m_pushExportEOFMID;
    jmethodID m_getQueuedExportBytesMID;
    jmethodID m_pushDRBufferMID;
    jmethodID m_pushPoisonPillMID;
    jmethodID m_reportDRConflictMID;
    jmethodID m_decodeBase64AndDecompressToBytesMID;
    jmethodID m_callJavaUserDefinedFunctionMID;
    jmethodID m_resizeUDFBufferMID;
    jmethodID m_storeLargeTempTableBlockMID;
    jmethodID m_loadLargeTempTableBlockMID;
    jmethodID m_releaseLargeTempTableBlockMID;
    jclass m_exportManagerClass;
    jclass m_partitionDRGatewayClass;
    jclass m_decompressionClass;
public:
    JNITopend(JNIEnv *env, jobject caller);
    ~JNITopend();

    inline JNITopend* updateJNIEnv(JNIEnv *env) { m_jniEnv = env; return this; }
    int loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination) override;
    void traceLog(bool isBegin,
                  const char *name,
                  const char *args);
    int64_t fragmentProgressUpdate(
                int32_t batchIndex,
                PlanNodeType planNodeType,
                int64_t tuplesProcessed,
                int64_t currMemoryInBytes,
                int64_t peakMemoryInBytes) override;
    std::string planForFragmentId(int64_t fragmentId) override;
    void crashVoltDB(FatalException e) override;
    int64_t getQueuedExportBytes(int32_t partitionId, std::string signature) override;
    void pushExportBuffer(
            int32_t partitionId,
            std::string signature,
            StreamBlock *block,
            bool sync) override;
    void pushEndOfStream(
            int32_t partitionId,
            std::string signature) override;

    int64_t pushDRBuffer(int32_t partitionId, StreamBlock *block) override;

    void pushPoisonPill(int32_t partitionId, std::string& reason, StreamBlock *block) override;

    int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
            DRConflictType deleteConflict, Table *existingMetaTableForDelete, Table *existingTupleTableForDelete,
            Table *expectedMetaTableForDelete, Table *expectedTupleTableForDelete,
            DRConflictType insertConflict, Table *existingMetaTableForInsert, Table *existingTupleTableForInsert,
            Table *newMetaTableForInsert, Table *newTupleTableForInsert) override;

    void fallbackToEEAllocatedBuffer(char *buffer, size_t length) override;

    std::string decodeBase64AndDecompress(const std::string& buffer) override;

    bool storeLargeTempTableBlock(LargeTempTableBlock* block) override;

    bool loadLargeTempTableBlock(LargeTempTableBlock* block) override;

    bool releaseLargeTempTableBlock(LargeTempTableBlockId blockId) override;

    int32_t callJavaUserDefinedFunction() override;
    void resizeUDFBuffer(int32_t size) override;
};

}
#endif /* JNITOPEND_H_ */
