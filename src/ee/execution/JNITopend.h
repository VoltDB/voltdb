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

#pragma once
#include <jni.h>

#include "common/FatalException.hpp"
#include "common/Topend.h"

namespace voltdb {
   class Pool;
   class LargeTempTableBlockId;
   // Create an instance of this class on the stack to release all local
   // references created during its lifetime.
   class JNILocalFrameBarrier {
      JNIEnv * m_env;
      const int32_t m_refs;
      const int32_t m_result;
      jboolean m_isCopy = JNI_FALSE;
      jbyteArray m_jbuf = nullptr;
      jbyte* m_bytes = nullptr;
   public:
      JNILocalFrameBarrier(JNIEnv* env, int32_t numReferences) :
         m_env(env), m_refs(numReferences), m_result(m_env->PushLocalFrame(m_refs)) {}
      ~JNILocalFrameBarrier() {
         if (m_isCopy == JNI_TRUE) {
            m_env->ReleaseByteArrayElements(m_jbuf, m_bytes, 0);
         }
         // pass jobject* to get pointer to previous frame?
         m_env->PopLocalFrame(nullptr);
      }
      void addDependencyRef(jboolean isCopy, jbyteArray jbuf, jbyte* bytes) {
         m_isCopy = isCopy;
         m_jbuf = jbuf;
         m_bytes = bytes;
      }
      int32_t checkResult() const {
         return m_result;
      }
   };

   class JNITopend : public Topend {
      JNIEnv *m_jniEnv;

      /**
       * JNI object corresponding to this engine. for callback functions.
       * if this is NULL, VoltDBEngine will fail to call sendDependency().
       */
      const jobject m_javaExecutionEngine;
      const jclass m_jniClass,
            m_exportManagerClass,
            m_partitionDRGatewayClass,
            m_decompressionClass;
      /**
       * Cache the method id for better performance. It is valid until the JVM unloads the class:
       * http://java.sun.com/javase/6/docs/technotes/guides/jni/spec/design.html#wp17074
       *
       * The jmethodID, jclass, etc. are all typedefs of raw pointer.
       */

      const jmethodID m_fallbackToEEAllocatedBufferMID,   // from class of m_javaExecutionEngine
            m_callJavaUserDefinedFunctionMID,
            m_resizeUDFBufferMID,
            m_nextDependencyMID,
            m_traceLogMID,
            m_fragmentProgressUpdateMID,
            m_planForFragmentIdMID,
            m_crashVoltDBMID,
            m_storeLargeTempTableBlockMID,
            m_loadLargeTempTableBlockMID,
            m_releaseLargeTempTableBlockMID;

      const jmethodID m_pushExportBufferMID,              // from class m_exportManagerClass
            m_pushExportEOFMID,
            m_getQueuedExportBytesMID;

      const jmethodID m_pushDRBufferMID,                  // from class m_partitionDRGatewayClass
            m_pushPoisonPillMID,
            m_reportDRConflictMID;

      const jmethodID m_decodeBase64AndDecompressToBytesMID;    // from class m_decompressionClass
      // Helper methods for the constructor
      static jclass getJClass(JNIEnv& env, const jobject& engine);
      static jmethodID createJniMethod(JNIEnv& env, const jclass jClass, const char* name, const char* signature);
      static jmethodID createJniStaticMethod(JNIEnv& env, const jclass jClass,
            const char* name, const char* signature);
      template<typename jptr_type> static jptr_type checked(jptr_type id, JNIEnv& env,
            const char* name, const char* signature = "");

      void checkJobject(jobject const ptr) const {
         if (ptr == NULL) {
            m_jniEnv->ExceptionDescribe();
            throw std::runtime_error("jobject is null");
         }
      }
      void checkException() const {
         if (m_jniEnv->ExceptionCheck()) {
            m_jniEnv->ExceptionDescribe();
            throw std::runtime_error("Failed JNI Env exception check");
         }
      }
      void checkFrameBarrier(JNILocalFrameBarrier const& barrier) const {
         if (barrier.checkResult() < 0) {
            VOLT_ERROR("Unable to load dependency: jni frame error.");
            throw std::runtime_error("Unable to load dependency: jni frame error.");
         }
      }
   public:
      JNITopend(JNIEnv *env, jobject caller);
      ~JNITopend();

      JNITopend& updateJNIEnv(JNIEnv *env) {
         m_jniEnv = env;
         return *this;
      }
      int loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination) override;
      void traceLog(bool isBegin, const char *name, const char *args);
      int64_t fragmentProgressUpdate(int32_t batchIndex, PlanNodeType planNodeType,
            int64_t tuplesProcessed, int64_t currMemoryInBytes, int64_t peakMemoryInBytes) override;
      std::string planForFragmentId(int64_t fragmentId) override;
      void crashVoltDB(FatalException const& e) override;
      int64_t getQueuedExportBytes(int32_t partitionId, std::string const& signature) override;
      void pushExportBuffer(int32_t partitionId, std::string const& signature,
            StreamBlock *block, bool sync) override;
      void pushEndOfStream( int32_t partitionId, std::string const& signature) override;
      int64_t pushDRBuffer(int32_t partitionId, StreamBlock *block) override;
      void pushPoisonPill(int32_t partitionId, std::string& reason, StreamBlock *block) override;
      int reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
            DRConflictType deleteConflict, Table* existingMetaTableForDelete, Table* existingTupleTableForDelete,
            Table* expectedMetaTableForDelete, Table* expectedTupleTableForDelete,
            DRConflictType insertConflict, Table* existingMetaTableForInsert, Table* existingTupleTableForInsert,
            Table* newMetaTableForInsert, Table* newTupleTableForInsert) override;
      void fallbackToEEAllocatedBuffer(char *buffer, size_t length) override;
      std::string decodeBase64AndDecompress(const std::string& buffer) override;
      bool storeLargeTempTableBlock(LargeTempTableBlock* block) override;
      bool loadLargeTempTableBlock(LargeTempTableBlock* block) override;
      bool releaseLargeTempTableBlock(LargeTempTableBlockId const& blockId) override;
      int32_t callJavaUserDefinedFunction() override;
      void resizeUDFBuffer(int32_t size) override;
   };
}

