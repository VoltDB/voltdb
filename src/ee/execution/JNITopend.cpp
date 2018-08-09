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
#include "JNITopend.h"
#include "common/StreamBlock.h"
#include "storage/table.h"
#include "common/LargeTempTableBlockId.hpp"

using namespace std;
using namespace voltdb;

jclass JNITopend::getJClass(JNIEnv& env, const jobject& engine) {
   const jclass jniClass = env.GetObjectClass(engine);
   VOLT_TRACE("found class: %d", jniClass == nullptr);
   if (jniClass == nullptr) {
      env.ExceptionDescribe();
      throw std::invalid_argument("Failed to call GetObjectClass() method on jobject engine");
   } else {
      return jniClass;
   }
}

jmethodID JNITopend::createJniMethod(JNIEnv& env, const jclass jClass,
      const char* name, const char* signature) {
   return checked(env.GetMethodID(jClass, name, signature),
         env, name, signature);
}

jmethodID JNITopend::createJniStaticMethod(JNIEnv& env, const jclass jClass,
      const char* name, const char* signature) {
   return checked(env.GetStaticMethodID(jClass, name, signature),
         env, name, signature);
}

template<typename jptr_type>
jptr_type JNITopend::checked(jptr_type id, JNIEnv& env, const char* name, const char* signature) {
   if (id == nullptr) {
      env.ExceptionDescribe();
      throw std::invalid_argument(
            std::string("Failed to set JNI method ")
            .append(name).append(" @ ").append(signature));
   } else {
      return id;
   }
}

JNITopend::JNITopend(JNIEnv *env, jobject caller) : m_jniEnv(env), m_javaExecutionEngine(caller),
   // classes
   m_jniClass(getJClass(*m_jniEnv, m_javaExecutionEngine)),
   m_exportManagerClass(checked(m_jniEnv->FindClass("org/voltdb/export/ExportManager"),
            *m_jniEnv, "org/voltdb/export/ExportManager")),
   m_partitionDRGatewayClass(checked(
            static_cast<jclass>(
               m_jniEnv->NewGlobalRef(     // Class created from global ref based on
                  checked(m_jniEnv->FindClass("org/voltdb/PartitionDRGateway"),    // finding the class
                     *m_jniEnv, "org/voltdb/PartitionDRGateway"))),
            *m_jniEnv, "org/voltdb/PartitionDRGateway")),
    m_decompressionClass(checked(m_jniEnv->FindClass("org/voltdb/utils/CompressionService"),
             *m_jniEnv, "org/voltdb/utils/CompressionService")),
   // JNI class' methods
   m_fallbackToEEAllocatedBufferMID(createJniMethod(*m_jniEnv, m_jniClass,
            "fallbackToEEAllocatedBuffer", "(Ljava/nio/ByteBuffer;)V")),
   m_callJavaUserDefinedFunctionMID(createJniMethod(*m_jniEnv, m_jniClass,
            "callJavaUserDefinedFunction", "()I")),
   m_resizeUDFBufferMID(createJniMethod(*m_jniEnv, m_jniClass,
            "resizeUDFBuffer", "(I)V")),
   m_nextDependencyMID(createJniMethod(*m_jniEnv, m_jniClass,
            "nextDependencyAsBytes", "(I)[B")),
   m_traceLogMID(createJniMethod(*m_jniEnv, m_jniClass,
            "traceLog", "(ZLjava/lang/String;Ljava/lang/String;)V")),
   m_fragmentProgressUpdateMID(createJniMethod(*m_jniEnv, m_jniClass,
            "fragmentProgressUpdate", "(IIJJJ)J")),
   m_planForFragmentIdMID(createJniMethod(*m_jniEnv, m_jniClass,
            "planForFragmentId", "(J)[B")),
   m_crashVoltDBMID(createJniStaticMethod(*m_jniEnv, m_jniClass,
            "crashVoltDB", "(Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;I)V")),
   m_storeLargeTempTableBlockMID(createJniMethod(*m_jniEnv, m_jniClass,
            "storeLargeTempTableBlock", "(JJLjava/nio/ByteBuffer;)Z")),
   m_loadLargeTempTableBlockMID(createJniMethod(*m_jniEnv, m_jniClass,
            "loadLargeTempTableBlock", "(JJLjava/nio/ByteBuffer;)Z")),
   m_releaseLargeTempTableBlockMID(createJniMethod(*m_jniEnv, m_jniClass,
            "releaseLargeTempTableBlock", "(JJ)Z")),
   // export class' methods
   m_pushExportBufferMID(createJniStaticMethod(*m_jniEnv, m_exportManagerClass,
            "pushExportBuffer", "(ILjava/lang/String;JJLjava/nio/ByteBuffer;Z)V")),
   m_pushExportEOFMID(createJniStaticMethod(*m_jniEnv, m_exportManagerClass,
            "pushEndOfStream", "(ILjava/lang/String;)V")),
   m_getQueuedExportBytesMID(createJniStaticMethod(*m_jniEnv, m_exportManagerClass,
            "getQueuedExportBytes", "(ILjava/lang/String;)J")),
   // DRGateway class' methods
   m_pushDRBufferMID(createJniStaticMethod(*m_jniEnv, m_partitionDRGatewayClass,
            "pushDRBuffer", "(IJJJJILjava/nio/ByteBuffer;)J")),
   m_pushPoisonPillMID(createJniStaticMethod(*m_jniEnv, m_partitionDRGatewayClass,
            "pushPoisonPill", "(ILjava/lang/String;Ljava/nio/ByteBuffer;)V")),
   m_reportDRConflictMID(createJniStaticMethod(*m_jniEnv, m_partitionDRGatewayClass,
            "reportDRConflict",
            "(IIJLjava/lang/String;IILjava/nio/ByteBuffer;Ljava/nio/ByteBuffer;"
            "Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;ILjava/nio/ByteBuffer;"
            "Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)I")),
   // CompressionService class' methods
   m_decodeBase64AndDecompressToBytesMID(
         createJniStaticMethod(*m_jniEnv, m_decompressionClass,
            "decodeBase64AndDecompressToBytes", "(Ljava/lang/String;)[B")) { }

JNITopend::~JNITopend() {
    m_jniEnv->DeleteGlobalRef(m_javaExecutionEngine);
    m_jniEnv->DeleteGlobalRef(m_exportManagerClass);
    m_jniEnv->DeleteGlobalRef(m_partitionDRGatewayClass);
    m_jniEnv->DeleteGlobalRef(m_decompressionClass);
}

void JNITopend::fallbackToEEAllocatedBuffer(char *buffer, size_t length) {
    checkFrameBarrier(JNILocalFrameBarrier(m_jniEnv, 1));
    jobject jbuffer = m_jniEnv->NewDirectByteBuffer(buffer, length);
    checkJobject(jbuffer);
    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_fallbackToEEAllocatedBufferMID, jbuffer);
    checkException();
}

int JNITopend::loadNextDependency(int32_t dependencyId, Pool *stringPool, Table* destination) {
   VOLT_DEBUG("iterating java dependency for id %d", dependencyId);
   JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 10);
   checkFrameBarrier(jni_frame);
   const jbyteArray jbuf = reinterpret_cast<jbyteArray>(
         m_jniEnv->CallObjectMethod(m_javaExecutionEngine, m_nextDependencyMID, dependencyId));
   const jsize len = jbuf ? m_jniEnv->GetArrayLength(jbuf) : 0;
   if (len > 0) {
      jboolean is_copy;
      jbyte *bytes = m_jniEnv->GetByteArrayElements(jbuf, &is_copy);
      // Add the dependency buffer info to the stack object
      // so it'll get cleaned up if loadTuplesFrom throws
      jni_frame.addDependencyRef(is_copy, jbuf, bytes);
      ReferenceSerializeInputBE serialize_in(bytes, len);
      destination->loadTuplesFrom(serialize_in, stringPool);
      return 1;
   } else {
      return 0;
   }
}

void JNITopend::traceLog(bool isBegin, const char *name, const char *args) {
    const jstring nameStr = m_jniEnv->NewStringUTF(name),
          argsStr = m_jniEnv->NewStringUTF(args);
    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_traceLogMID,
          isBegin ? JNI_TRUE : JNI_FALSE, nameStr, argsStr);
    m_jniEnv->DeleteLocalRef(nameStr);
    m_jniEnv->DeleteLocalRef(argsStr);
    checkException();
}

int64_t JNITopend::fragmentProgressUpdate(int32_t batchIndex, PlanNodeType planNodeType,
      int64_t tuplesProcessed, int64_t currMemoryInBytes, int64_t peakMemoryInBytes) {
   return m_jniEnv->CallLongMethod(m_javaExecutionEngine, m_fragmentProgressUpdateMID,
            batchIndex, static_cast<int32_t>(planNodeType), tuplesProcessed,
            currMemoryInBytes, peakMemoryInBytes);
}

// A local helper to convert a jbyteArray to an std::string.
// Callers should be aware that an empty string may be returned if
// jbuf is null.
static std::string jbyteArrayToStdString(JNIEnv* jniEnv, JNILocalFrameBarrier& jniFrame, jbyteArray jbuf) {
   const jsize len = jbuf ? jniEnv->GetArrayLength(jbuf) : 0;
   if (len > 0) {
      jboolean isCopy;
      jbyte *bytes = jniEnv->GetByteArrayElements(jbuf, &isCopy);
      jniFrame.addDependencyRef(isCopy, jbuf, bytes);
      return std::string(reinterpret_cast<char*>(bytes), len);
   } else {
      return "";
   }
}

std::string JNITopend::planForFragmentId(int64_t fragmentId) {
   VOLT_DEBUG("fetching plan for id %l", fragmentId);
   JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 10);
   checkFrameBarrier(jni_frame);
   // jbuf might be NULL or might have 0 length here.  In that case
   // we'll return a 0-length string to the caller, who will return
   // an appropriate error.
   return jbyteArrayToStdString(m_jniEnv, jni_frame,
         static_cast<jbyteArray>(m_jniEnv->CallObjectMethod(
               m_javaExecutionEngine, m_planForFragmentIdMID, fragmentId)));
}

std::string JNITopend::decodeBase64AndDecompress(const std::string& base64Str) {
   JNILocalFrameBarrier jni_frame = JNILocalFrameBarrier(m_jniEnv, 2);
   checkFrameBarrier(jni_frame);
   const jstring jBase64Str = m_jniEnv->NewStringUTF(base64Str.c_str());
   checkException();
   return jbyteArrayToStdString(m_jniEnv, jni_frame,
         reinterpret_cast<jbyteArray>(m_jniEnv->CallStaticObjectMethod(m_decompressionClass,
               m_decodeBase64AndDecompressToBytesMID, jBase64Str)));
}

bool JNITopend::storeLargeTempTableBlock(LargeTempTableBlock* block) {
    checkFrameBarrier(JNILocalFrameBarrier(m_jniEnv, 1));
    std::unique_ptr<char[]> storage = block->releaseData();
    const jobject blockByteBuffer = m_jniEnv->NewDirectByteBuffer(
          storage.get(), LargeTempTableBlock::BLOCK_SIZE_IN_BYTES);
    checkJobject(blockByteBuffer);
    const LargeTempTableBlockId blockId = block->id();
    // It's assumed that when control returns to this method the block
    // will have been persisted to disk.  The memory for the block
    // will be returned to the OS when above unique_ptr goes out of
    // scope.
    return m_jniEnv->CallBooleanMethod(m_javaExecutionEngine, m_storeLargeTempTableBlockMID,
          blockId.getSiteId(), blockId.getBlockCounter(), blockByteBuffer);
}

bool JNITopend::loadLargeTempTableBlock(LargeTempTableBlock* block) {
    checkFrameBarrier(JNILocalFrameBarrier(m_jniEnv, 1));
    // Memory allocation should really be done by LargeTempTableBLock
    // cache, and it should pass in the storage for the loaded block.
    std::unique_ptr<char[]> storage(new char[LargeTempTableBlock::BLOCK_SIZE_IN_BYTES]);
    const jobject blockByteBuffer = m_jniEnv->NewDirectByteBuffer(
          storage.get(), LargeTempTableBlock::BLOCK_SIZE_IN_BYTES);
    checkJobject(blockByteBuffer);
    const bool success = m_jniEnv->CallBooleanMethod(m_javaExecutionEngine,
          m_loadLargeTempTableBlockMID, block->id(), blockByteBuffer);
    if (success) {
        block->setData(std::move(storage));
    }
    return success;
}

bool JNITopend::releaseLargeTempTableBlock(LargeTempTableBlockId const& blockId) {
   return m_jniEnv->CallBooleanMethod(m_javaExecutionEngine,
         m_releaseLargeTempTableBlockMID, blockId.getSiteId(),
         blockId.getBlockCounter());
}

int32_t JNITopend::callJavaUserDefinedFunction() {
   return m_jniEnv->CallIntMethod(m_javaExecutionEngine,
         m_callJavaUserDefinedFunctionMID);
}

void JNITopend::resizeUDFBuffer(int32_t size) {
    m_jniEnv->CallVoidMethod(m_javaExecutionEngine, m_resizeUDFBufferMID, size);
}

void JNITopend::crashVoltDB(FatalException const& e) {
   //Enough references for the reason string, traces array, and traces strings
   checkFrameBarrier(JNILocalFrameBarrier(m_jniEnv, e.traces().size() + 4));
   const jstring jReason = m_jniEnv->NewStringUTF(e.what());
   checkException();
   const jstring jFilename = m_jniEnv->NewStringUTF(e.filename());
   checkException();
   const jobjectArray jTracesArray =
      m_jniEnv->NewObjectArray(static_cast<jsize>(e.traces().size()),
            m_jniEnv->FindClass("java/lang/String"), nullptr);
   checkException();
   for (int ii = 0; ii < e.traces().size(); ii++) {
      const jstring traceString = m_jniEnv->NewStringUTF(e.traces()[ii].c_str());
      m_jniEnv->SetObjectArrayElement(jTracesArray, ii, traceString);
   }
   m_jniEnv->CallStaticVoidMethod(m_jniEnv->GetObjectClass(m_javaExecutionEngine), m_crashVoltDBMID,
         jReason, jTracesArray, jFilename, e.lineno());
   throw e;
}

int64_t JNITopend::getQueuedExportBytes(int32_t partitionId, string const& signature) {
    const jstring signatureString = m_jniEnv->NewStringUTF(signature.c_str());
    const int64_t retval = m_jniEnv->CallStaticLongMethod(m_exportManagerClass, m_getQueuedExportBytesMID,
          partitionId, signatureString);
    m_jniEnv->DeleteLocalRef(signatureString);
    return retval;
}

void JNITopend::pushExportBuffer(int32_t partitionId, string const& signature,
        StreamBlock *block, bool sync) {
    const jstring signatureString = m_jniEnv->NewStringUTF(signature.c_str());
    if (block != nullptr) {
        const jobject buffer = m_jniEnv->NewDirectByteBuffer(block->rawPtr(), block->rawLength());
        checkJobject(buffer);
        m_jniEnv->CallStaticVoidMethod(m_exportManagerClass, m_pushExportBufferMID,
                partitionId, signatureString, block->uso(),
                reinterpret_cast<jlong>(block->rawPtr()),
                buffer, sync ? JNI_TRUE : JNI_FALSE);
        m_jniEnv->DeleteLocalRef(buffer);
    } else {
       m_jniEnv->CallStaticVoidMethod(m_exportManagerClass, m_pushExportBufferMID,
             partitionId, signatureString, 0l, nullptr, nullptr,
             sync ? JNI_TRUE : JNI_FALSE);
    }
    m_jniEnv->DeleteLocalRef(signatureString);
    checkException();
}

void JNITopend::pushEndOfStream(int32_t partitionId, string const& signature) {
    const jstring signatureString = m_jniEnv->NewStringUTF(signature.c_str());
    //std::cout << "Block is null" << std::endl;
    m_jniEnv->CallStaticVoidMethod(m_exportManagerClass, m_pushExportEOFMID,
          partitionId, signatureString);
    m_jniEnv->DeleteLocalRef(signatureString);
    checkException();
}

int64_t JNITopend::pushDRBuffer(int32_t partitionId, StreamBlock *block) {
    if (block == nullptr) {
       return -1l;
    } else {
       const jobject buffer = m_jniEnv->NewDirectByteBuffer(block->rawPtr(), block->rawLength());
       checkJobject(buffer);
       int64_t retVal = m_jniEnv->CallStaticLongMethod(m_partitionDRGatewayClass, m_pushDRBufferMID, partitionId,
             block->startDRSequenceNumber(), block->lastDRSequenceNumber(), block->lastSpUniqueId(),
             block->lastMpUniqueId(), block->drEventType(), buffer);
       m_jniEnv->DeleteLocalRef(buffer);
       return retVal;
    }
}

void JNITopend::pushPoisonPill(int32_t partitionId, std::string& reason, StreamBlock *block) {
    const jstring jReason = m_jniEnv->NewStringUTF(reason.c_str());
    if (block != nullptr) {
        const jobject buffer = m_jniEnv->NewDirectByteBuffer( block->rawPtr(), block->rawLength());
        checkJobject(buffer);
        m_jniEnv->CallStaticLongMethod(m_partitionDRGatewayClass, m_pushPoisonPillMID,
              partitionId, jReason, buffer);
        m_jniEnv->DeleteLocalRef(buffer);
    }
    m_jniEnv->DeleteLocalRef(jReason);
}

static std::unique_ptr<char[]> serializeToDirectByteBuffer(JNIEnv *jniEngine, Table* table, jobject& byteBuffer) {
    if (table) {
        const size_t serializeSize = table->getAccurateSizeToSerialize();
        std::unique_ptr<char[]> backingArray(new char[serializeSize]);
        ReferenceSerializeOutput conflictSerializeOutput(backingArray.get(), serializeSize);
        table->serializeToWithoutTotalSize(conflictSerializeOutput);
        byteBuffer = jniEngine->NewDirectByteBuffer(static_cast<void*>(backingArray.get()),
              static_cast<int32_t>(serializeSize));
       if (byteBuffer == nullptr) {
           jniEngine->ExceptionDescribe();
           throw std::runtime_error("serializeToDirectByteBuffer");
        } else {
           return backingArray;
        }
    } else {
       return std::unique_ptr<char[]>();
    }
}

int JNITopend::reportDRConflict(int32_t partitionId, int32_t remoteClusterId, int64_t remoteTimestamp, std::string tableName, DRRecordType action,
        DRConflictType deleteConflict, Table* existingMetaTableForDelete, Table* existingTupleTableForDelete,
        Table* expectedMetaTableForDelete, Table* expectedTupleTableForDelete,
        DRConflictType insertConflict, Table* existingMetaTableForInsert, Table* existingTupleTableForInsert,
        Table* newMetaTableForInsert, Table* newTupleTableForInsert) {
   // prepare tablename
   const jstring tableNameString = m_jniEnv->NewStringUTF(tableName.c_str());
   // prepare input buffer for delete conflict
   std::array<Table*, 8> tbls {
      existingMetaTableForDelete, existingTupleTableForDelete, expectedMetaTableForDelete,
         expectedTupleTableForDelete, existingMetaTableForInsert, existingTupleTableForInsert,
         newMetaTableForInsert, newTupleTableForInsert
   };
   std::array<jobject, 8> buffers{nullptr};
   std::array<std::unique_ptr<char[]>, 8> expected{};
   for (int index = 0; index < 8; ++index) {
      expected[index] = serializeToDirectByteBuffer(m_jniEnv, tbls[index], buffers[index]);
   }
   const int retval = m_jniEnv->CallStaticIntMethod(m_partitionDRGatewayClass, m_reportDRConflictMID,
         partitionId, remoteClusterId, remoteTimestamp, tableNameString, action, deleteConflict,
         buffers[0], buffers[1], buffers[2], buffers[3], insertConflict,
         buffers[4], buffers[5], buffers[6], buffers[7]);
   m_jniEnv->DeleteLocalRef(tableNameString);
   for (jobject obj : buffers) {
      m_jniEnv->DeleteLocalRef(obj);
   }
   return retval;
}

