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

#include "storage/DRTupleStream.h"

#include "common/TupleSchema.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "common/ExportSerializeIo.h"
#include "common/executorcontext.hpp"
#include "common/UniqueId.hpp"
#include "crc/crc32c.h"
#include "indexes/tableindex.h"
#include "storage/TupleStreamException.h"

#include <vector>
#include <cstdio>
#include <limits>
#include <iostream>
#include <common/debuglog.h>
#include <ctime>
#include <utility>
#include <math.h>

using namespace std;
using namespace voltdb;

DRTupleStream::DRTupleStream(int partitionId, size_t defaultBufferSize, uint8_t drProtocolVersion)
    : AbstractDRTupleStream(partitionId, defaultBufferSize, drProtocolVersion),
      m_initialHashFlag(partitionId == 16383 ? TXN_PAR_HASH_REPLICATED : TXN_PAR_HASH_PLACEHOLDER),
      m_hashFlag(m_initialHashFlag),
      m_firstParHash(LONG_MAX),
      m_lastParHash(LONG_MAX),
      m_beginTxnUso(0),
      m_lastCommittedSpUniqueId(0),
      m_lastCommittedMpUniqueId(0) {
    extendBufferChain(m_defaultCapacity);
}

size_t DRTupleStream::truncateTable(char *tableHandle, std::string const& tableName,
        int partitionColumn, int64_t spHandle, int64_t uniqueId) {
    if (m_guarded) return INVALID_DR_MARK;

    size_t startingUso = m_uso;

    transactionChecks(spHandle, uniqueId);

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    bool requireHashDelimiter = updateParHash(partitionColumn == -1, LONG_MAX);

    if (!m_currBlock) {
        try {
            extendBufferChain(m_defaultCapacity);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR truncate table " + tableName);
            throw;
        }
    }

    size_t tupleMaxLength = TXN_RECORD_HEADER_SIZE + 4 + tableName.size(); // table name length and table name
    if (requireHashDelimiter) {
        tupleMaxLength += HASH_DELIMITER_SIZE;
    }
    if (m_currBlock->remaining() < tupleMaxLength) {
        try {
            extendBufferChain(tupleMaxLength);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR truncate table " + tableName);
            throw;
        }
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(-1);  // hash delimiter for TRUNCATE_TABLE records is always -1
    }
    io.writeByte(static_cast<int8_t>(DR_RECORD_TRUNCATE_TABLE));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));
    io.writeInt(static_cast<int32_t>(tableName.size()));
    io.writeBytes(tableName.c_str(), tableName.size());

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(DR_RECORD_TRUNCATE_TABLE);

    return startingUso;
}

int64_t DRTupleStream::getParHashForTuple(TableTuple& tuple, int partitionColumn) {
    if (partitionColumn != -1) {
        return static_cast<int64_t>(tuple.getNValue(partitionColumn).murmurHash3());
    } else {
        return LONG_MAX;
    }
}

bool DRTupleStream::updateParHash(bool isReplicatedTable, int64_t parHash) {
    if (isReplicatedTable) {
        // the initial value, which is TXN_PAR_HASH_REPLICATED
        vassert(m_hashFlag == m_initialHashFlag);
        return false;
    }

    if (m_hashFlag == TXN_PAR_HASH_PLACEHOLDER) {  // initial status, first record
        m_lastParHash = parHash;
        m_firstParHash = parHash; // save first hash
        // if the first record is TRUNCATE_TABLE, set to SPECIAL, otherwise SINGLE
        m_hashFlag = (parHash == LONG_MAX) ? TXN_PAR_HASH_SPECIAL : TXN_PAR_HASH_SINGLE;
        // no delimiter needed for first record
        return false;
    } else if (parHash != m_lastParHash) {
        m_lastParHash = parHash;
        // set to SPECIAL whenever we see a TRUNCATE_TABLE record
        if (parHash == LONG_MAX) {
            m_hashFlag = TXN_PAR_HASH_SPECIAL;
        } else if (m_hashFlag == TXN_PAR_HASH_SINGLE) { // set to MULTI if it was SINGLE
            m_hashFlag = TXN_PAR_HASH_MULTI;
        }
        // delimiter needed before the pending record
        return true;
    }
    // no delimiter needed for contiguous identical hashes
    return false;
}

/*
 * If SpHandle represents a new transaction, commit previous data.
 * Always serialize the supplied tuple in to the stream.
 * Return m_uso before this invocation - this marks the point
 * in the stream the caller can rollback to if this append
 * should be rolled back.
 */
size_t DRTupleStream::appendTuple(char *tableHandle, int partitionColumn, int64_t spHandle,
        int64_t uniqueId, TableTuple &tuple, DRRecordType type) {
    if (m_guarded) return INVALID_DR_MARK;

    size_t startingUso = m_uso;

    size_t rowHeaderSz = 0;
    size_t rowMetadataSz = 0;
    size_t tupleMaxLength = 0;

    transactionChecks(spHandle, uniqueId);

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    bool requireHashDelimiter = updateParHash(partitionColumn == -1, getParHashForTuple(tuple, partitionColumn));

    // Compute the upper bound on bytes required to serialize tuple.
    // exportxxx: can memoize this calculation.
    tupleMaxLength = computeOffsets(type, tuple, rowHeaderSz, rowMetadataSz) + TXN_RECORD_HEADER_SIZE;
    if (requireHashDelimiter) {
        tupleMaxLength += HASH_DELIMITER_SIZE;
    }

    if (!m_currBlock) {
        try {
            extendBufferChain(m_defaultCapacity);
        } catch (TupleStreamException &e) {
            char msg[64];
            snprintf(msg, sizeof msg, " DR record type %d", type);
            msg[sizeof msg - 1] = '\0';
            e.appendContextToMessage(msg);
            throw;
        }
    }

    if (m_currBlock->remaining() < tupleMaxLength) {
        try {
            extendBufferChain(tupleMaxLength);
        } catch (TupleStreamException &e) {
            char msg[64];
            snprintf(msg, sizeof msg, " DR record type %d", type);
            msg[sizeof msg - 1] = '\0';
            e.appendContextToMessage(msg);
            throw;
        }
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(), m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(static_cast<int32_t>(m_lastParHash));
    }

    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    writeRowTuple(tuple, rowHeaderSz, rowMetadataSz, io);

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(type);

    // std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

size_t DRTupleStream::appendUpdateRecord(
        char *tableHandle, int partitionColumn, int64_t spHandle,
        int64_t uniqueId, TableTuple &oldTuple, TableTuple &newTuple) {
    if (m_guarded) return INVALID_DR_MARK;

    size_t startingUso = m_uso;

    size_t oldRowHeaderSz = 0;
    size_t oldRowMetadataSz = 0;
    size_t newRowHeaderSz = 0;
    size_t newRowMetadataSz = 0;
    size_t maxLength = TXN_RECORD_HEADER_SIZE;

    transactionChecks(spHandle, uniqueId);

    //Drop the row, don't move the USO
    if (!m_enabled) return INVALID_DR_MARK;

    bool requireHashDelimiter = updateParHash(partitionColumn == -1, getParHashForTuple(oldTuple, partitionColumn));

    DRRecordType type = DR_RECORD_UPDATE;
    maxLength += computeOffsets(type, oldTuple, oldRowHeaderSz, oldRowMetadataSz);
    // No danger of replacing the second tuple by an index key, since if the type is going to change
    // it has already done so in the above computeOffsets() call
    maxLength += computeOffsets(type, newTuple, newRowHeaderSz, newRowMetadataSz);
    if (requireHashDelimiter) {
        maxLength += HASH_DELIMITER_SIZE;
    }

    if (!m_currBlock) {
        try {
            extendBufferChain(m_defaultCapacity);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR update tuple");
            throw;
        }
    }

    if (m_currBlock->remaining() < maxLength) {
        try {
            extendBufferChain(maxLength);
        } catch (TupleStreamException &e) {
            e.appendContextToMessage(" DR update tuple");
            throw;
        }
    }

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                                 m_currBlock->remaining());

    if (requireHashDelimiter) {
        io.writeByte(static_cast<int8_t>(DR_RECORD_HASH_DELIMITER));
        io.writeInt(static_cast<int32_t>(m_lastParHash));
    }

    io.writeByte(static_cast<int8_t>(type));
    io.writeLong(*reinterpret_cast<int64_t*>(tableHandle));

    writeRowTuple(oldTuple, oldRowHeaderSz, oldRowMetadataSz, io);
    writeRowTuple(newTuple, newRowHeaderSz, newRowMetadataSz, io);

    // update m_offset
    m_currBlock->consumed(io.position());

    // update uso.
    m_uso += io.position();

    // update row count
    m_txnRowCount += rowCostForDRRecord(type);

    // std::cout << "Appending row " << io.position() << " at " << m_currBlock->offset() << std::endl;
    return startingUso;
}

bool DRTupleStream::transactionChecks(int64_t spHandle, int64_t uniqueId) {
    // Transaction IDs for transactions applied to this tuple stream
    // should always be moving forward in time.
    if (spHandle < m_openTxnId) {
        if (m_enabled) {
            fatalDRErrorWithPoisonPill(spHandle, uniqueId,
                    "Active transactions moving backwards: openSpHandle is %jd, while the truncate spHandle is %jd",
                    (intmax_t)m_openTxnId, (intmax_t)spHandle);
        }
        return false;
    }

    bool switchedToOpen = false;
    if (!m_opened) {
        ++m_openSequenceNumber;

        if (m_enabled) {
            beginTransaction(m_openSequenceNumber, spHandle, uniqueId);
        } else {
            openTransactionCommon(spHandle, uniqueId);
        }
        switchedToOpen = true;
    } else if (m_openUniqueId != uniqueId && m_enabled) {
        fatalDRErrorWithPoisonPill(spHandle, uniqueId, "UniqueId of BeginTxn %s does not match current Txn UniqueId %s",
                UniqueId::toString(UniqueId(m_openUniqueId)).c_str(), UniqueId::toString(UniqueId(uniqueId)).c_str());
    }
    vassert(m_opened);
    return switchedToOpen;
}

void DRTupleStream::writeRowTuple(TableTuple& tuple,
        size_t rowHeaderSz, size_t rowMetadataSz, ExportSerializeOutput &io) {
    size_t startPos = io.position();
    // initialize the full row header to 0. This also
    // has the effect of setting each column non-null.
    ::memset(m_currBlock->mutableDataPtr() + io.position(), 0, rowHeaderSz);
    // the nullarray lives in rowheader after the 4 byte header length prefix
    uint8_t *nullArray =
        reinterpret_cast<uint8_t*>(m_currBlock->mutableDataPtr() + io.position() + rowMetadataSz);

    // Reserve the row header by moving the position beyond the row header.
    // The row header includes the 4 byte length prefix and the null array.
    const size_t lengthPrefixPosition = io.reserveBytes(rowHeaderSz);

    tuple.serializeToDR(io, 0, nullArray);

    ExportSerializeOutput hdr(m_currBlock->mutableDataPtr() + lengthPrefixPosition, rowMetadataSz);
    // add the row length to the header
    hdr.writeInt((int32_t)(io.position() - startPos - sizeof(int32_t)));
}

size_t DRTupleStream::computeOffsets(DRRecordType &type,
        TableTuple &tuple, size_t &rowHeaderSz, size_t &rowMetadataSz) {
    rowMetadataSz = sizeof(int32_t);
    int columnCount;
    switch (type) {
    case DR_RECORD_DELETE:
    case DR_RECORD_UPDATE:
        columnCount = tuple.columnCount();
        break;
    default:
        columnCount = tuple.columnCount();
        break;
    }
    int nullMaskLength = ((columnCount + 7) & -8) >> 3;
    rowHeaderSz = rowMetadataSz + nullMaskLength;
    return rowHeaderSz + tuple.maxDRSerializationSize();
}

void DRTupleStream::beginTransaction(int64_t sequenceNumber, int64_t spHandle, int64_t uniqueId) {
    vassert(!m_opened);

    if (!m_currBlock) {
         extendBufferChain(m_defaultCapacity);
     }

     if (m_currBlock->remaining() < BEGIN_RECORD_SIZE) {
         extendBufferChain(BEGIN_RECORD_SIZE);
     }

     m_currBlock->recordLastBeginTxnOffset();

     if (m_currBlock->lastDRSequenceNumber() != std::numeric_limits<int64_t>::max() &&
         m_currBlock->lastDRSequenceNumber() != (sequenceNumber - 1)) {
         fatalDRErrorWithPoisonPill(spHandle, uniqueId,
                 "Appending begin transaction message to a DR buffer without closing the previous transaction (open=%s)"
                 " Block state: last closed sequence number (%jd), last closed uniqueIds (%s, %s)."
                 " Transaction parameters: sequence number (%jd), uniqueId (%s)."
                 " Stream state: open sequence number (%jd), committed sequence number (%jd), "
                 "open uniqueId (%s), open spHandle (%jd), committed spHandle (%jd)",
                 (m_opened ? "true" : "false"),
                 (intmax_t)m_currBlock->lastDRSequenceNumber(), UniqueId::toString(UniqueId(m_currBlock->lastSpUniqueId())).c_str(),
                 UniqueId::toString(UniqueId(m_currBlock->lastMpUniqueId())).c_str(),
                 (intmax_t)sequenceNumber, UniqueId::toString(UniqueId(uniqueId)).c_str(),
                 (intmax_t)m_openSequenceNumber, (intmax_t)m_committedSequenceNumber,
                 UniqueId::toString(UniqueId(m_openUniqueId)).c_str(), (intmax_t)m_openTxnId, (intmax_t)m_committedTxnId);
         extendBufferChain(m_defaultCapacity);
         m_currBlock->recordLastBeginTxnOffset();
     }

     m_currBlock->startDRSequenceNumber(sequenceNumber);

     ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                              m_currBlock->remaining());
     io.writeByte(m_drProtocolVersion);
     io.writeByte(static_cast<int8_t>(DR_RECORD_BEGIN_TXN));
     io.writeLong(uniqueId);
     io.writeLong(sequenceNumber);
     io.writeByte(0); // placeholder for hash flag
     io.writeInt(0); // placeholder for txn length
     io.writeInt(0);  // placeholder for first partition hash

     m_currBlock->consumed(io.position());

     m_beginTxnUso = m_uso;
     m_uso += io.position();

     m_hashFlag = m_initialHashFlag;
     m_firstParHash = LONG_MAX;
     m_lastParHash = LONG_MAX;

     openTransactionCommon(spHandle, uniqueId);
}

void DRTupleStream::endTransaction(int64_t uniqueId) {
    if (!m_opened) {
        return;
    }

    if (!m_enabled) {
        if (m_openUniqueId != uniqueId) {
            m_opened = false;
            fatalDRErrorWithPoisonPill(m_openTxnId, m_openUniqueId,
                    "Stream UniqueId (%s) does not match the Context's UniqueId (%s)."
                    " DR sequence number is out of sync with UniqueId",
                    UniqueId::toString(UniqueId(m_openUniqueId)).c_str(),
                    UniqueId::toString(UniqueId(uniqueId)).c_str());
        }

        updateLastUniqueId(uniqueId, nullptr);

        commitTransactionCommon();
        return;
    }

    if (!m_currBlock) {
        extendBufferChain(m_defaultCapacity);
    }

    if (m_currBlock->remaining() < END_RECORD_SIZE) {
        extendBufferChain(END_RECORD_SIZE);
    }

    if (m_currBlock->startDRSequenceNumber() == std::numeric_limits<int64_t>::max()) {
        m_opened = false;
        fatalDRErrorWithPoisonPill(m_openTxnId, m_openUniqueId,
                "Appending end transaction message to a DR buffer with no matching begin transaction message."
                "Stream state: open sequence number (%jd), committed sequence number (%jd), open uniqueId (%s), "
                "open spHandle (%jd), committed spHandle (%jd)",
                (intmax_t)m_openSequenceNumber, (intmax_t)m_committedSequenceNumber,
                UniqueId::toString(UniqueId(m_openUniqueId)).c_str(),
                (intmax_t)m_openTxnId, (intmax_t)m_committedTxnId);
        return;
    } else if (m_currBlock->lastDRSequenceNumber() != std::numeric_limits<int64_t>::max() &&
            m_currBlock->lastDRSequenceNumber() > m_openSequenceNumber) {
        m_opened = false;
        fatalDRErrorWithPoisonPill(m_openTxnId, m_openUniqueId,
                "Appending end transaction message to a DR buffer with a greater DR sequence number."
                " Buffer end DR sequence number (%jd), buffer end UniqueIds (%s, %s)."
                " Current DR sequence number (%jd), current UniqueId (%s)",
                (intmax_t)m_currBlock->lastDRSequenceNumber(),
                UniqueId::toString(UniqueId(m_currBlock->lastSpUniqueId())).c_str(),
                UniqueId::toString(UniqueId(m_currBlock->lastMpUniqueId())).c_str(),
                (intmax_t)m_openSequenceNumber, UniqueId::toString(UniqueId(m_openUniqueId)).c_str());
        return;
    }

    if (m_openUniqueId != uniqueId) {
        m_opened = false;
        fatalDRErrorWithPoisonPill(m_openTxnId, m_openUniqueId,
                "Stream UniqueId (%s) does not match the Context's UniqueId (%s)."
                " DR sequence number is out of sync with UniqueId",
                UniqueId::toString(UniqueId(m_openUniqueId)).c_str(),
                UniqueId::toString(UniqueId(uniqueId)).c_str());
        return;
    }

    updateLastUniqueId(uniqueId, m_currBlock);
    m_currBlock->recordLastCommittedSpHandle(m_openTxnId);
    m_currBlock->recordCompletedSequenceNumForDR(m_openSequenceNumber);

    ExportSerializeOutput io(m_currBlock->mutableDataPtr(),
                             m_currBlock->remaining());
    io.writeByte(static_cast<int8_t>(DR_RECORD_END_TXN));
    io.writeLong(m_openSequenceNumber);
    io.writeInt(0); // placeholder for checksum of the entire txn

    m_currBlock->consumed(io.position());

    m_uso += io.position();

    int32_t txnLength = m_uso - m_beginTxnUso;
    ExportSerializeOutput extraio(m_currBlock->mutableDataPtr() - txnLength,
                                  txnLength);
    extraio.position(BEGIN_RECORD_HEADER_SIZE);
    extraio.writeByte(static_cast<int8_t>(m_hashFlag));
    extraio.writeInt(txnLength);
    // if it is the replicated stream or first record is TRUNCATE_TABLE
    // m_firstParHash will be LONG_MAX, and will be written as -1 after casting
    extraio.writeInt(static_cast<int32_t>(m_firstParHash));

    uint32_t crc = vdbcrc::crc32cInit();
    crc = vdbcrc::crc32c(crc, m_currBlock->mutableDataPtr() - txnLength, txnLength - 4);
    crc = vdbcrc::crc32cFinish(crc);
    extraio.position(txnLength - 4);
    extraio.writeInt(crc);

    m_committedUso = m_uso;
    commitTransactionCommon();

    size_t bufferRowCount = m_currBlock->updateRowCountForDR(m_txnRowCount);
    if (m_rowTarget >= 0 && bufferRowCount >= m_rowTarget) {
        extendBufferChain(0);
    }

    m_txnRowCount = 0;
}

// If partial transaction is going to span multiple buffer, first time move it to
// the next buffer, the next time move it to a 45 megabytes buffer, then after throw
// an exception and rollback.
bool DRTupleStream::checkOpenTransaction(DrStreamBlock* sb, size_t minLength,
        size_t& blockSize, size_t& uso) {
    if (sb && sb->hasDRBeginTxn()   /* this block contains a DR begin txn */
           && m_opened) {
        size_t partialTxnLength = sb->offset() - sb->lastDRBeginTxnOffset();
        size_t spaceNeeded = m_headerSpace + partialTxnLength + minLength;
        if (spaceNeeded > m_secondaryCapacity) {
            // txn larger than the max buffer size, set blockSize to 0 so that caller will abort
            blockSize = 0;

            char msg[256];
            snprintf(msg, sizeof msg, "Transaction requiring %jd bytes exceeds max DR Buffer size of %jd bytes",
                     spaceNeeded, m_secondaryCapacity);
            msg[sizeof msg - 1] = 0;
            throw TupleStreamException(SQLException::volt_output_buffer_overflow, msg);
        } else if (spaceNeeded > m_defaultCapacity) {
            blockSize = m_secondaryCapacity;
        }
        if (blockSize != 0) {
            uso -= partialTxnLength;
        }
        return true;
    }
    vassert(!m_opened);
    return false;
}

void DRTupleStream::extendBufferChain(size_t minLength) {
    DrStreamBlock *oldBlock = m_currBlock;
    size_t uso = m_uso;
    size_t blockSize = (minLength <= m_defaultCapacity) ? m_defaultCapacity : m_maxCapacity;
    bool openTransaction = checkOpenTransaction(oldBlock, minLength, blockSize, uso);

    TupleStreamBase::commonExtendBufferChain(blockSize, uso);

    if (openTransaction) {
        handleOpenTransaction(oldBlock);
    }

    pushPendingBlocks();
}

void DRTupleStream::generateDREvent(DREventType type, int64_t spHandle,
        int64_t uniqueId, ByteArray payloads)
{
    if (type != SWAP_TABLE) { // openTxn does this for SWAP_TABLE
        vassert(!m_opened);
        ++m_openSequenceNumber;
    }

    if (!m_enabled) {
        updateLastUniqueId(uniqueId, nullptr);

        openTransactionCommon(spHandle, uniqueId);
        commitTransactionCommon();
        return;
    }

    switch (type) {
    case CATALOG_UPDATE:
    case DR_STREAM_END:
    case DR_STREAM_START:
    case DR_ELASTIC_REBALANCE: {
        writeEventData(type, payloads, spHandle);
        m_currBlock->recordCompletedSequenceNumForDR(m_openSequenceNumber);
        updateLastUniqueId(uniqueId, m_currBlock);

        m_committedUso = m_uso;
        openTransactionCommon(spHandle, uniqueId);
        commitTransactionCommon();

        extendBufferChain(0);
        break;
    }
    case DR_ELASTIC_CHANGE: {
        // REMOVE this hack and uncomment generateElasticChangeEvents code
        // after the Replicated DR Stream is removed
        ReferenceSerializeInputBE input(payloads.data(), 8);
        int oldPartitionCnt = input.readInt();
        if (m_partitionId >= oldPartitionCnt && m_partitionId != 16383) {
            // Hack change the event to a DR_STREAM_START with isNewStreamForElasticAdd set to true
            ByteArray flagBuf = ByteArray(1);
            flagBuf[0] = 1;
            writeEventData(DR_STREAM_START, flagBuf, spHandle);
        }
        else {
            writeEventData(type, payloads, spHandle);
        }
        m_currBlock->recordCompletedSequenceNumForDR(m_openSequenceNumber);
        updateLastUniqueId(uniqueId, m_currBlock);

        m_committedUso = m_uso;
        openTransactionCommon(spHandle, uniqueId);
        commitTransactionCommon();

        extendBufferChain(0);
        break;
    }
    case SWAP_TABLE : {
        // For DR events that get generated between a BEGIN_TXN and END_TXN,
        // always call endTransaction() and then extendBufferChain() on dr
        // streams in the release() of the corresponding UndoAction.
        // Currently, only SWAP_TABLE does this, if there are others in the
        // future, the above logic can be refactored into a function.
        writeEventData(type, payloads, spHandle);
        break;
    }
    default:
        vassert(false);
    }
}

void DRTupleStream::writeEventData(DREventType type, ByteArray payloads, int64_t spHandle) {
    // Make sure current block is empty
    extendBufferChain(0);
    ExportSerializeOutput io(m_currBlock->mutableDataPtr(), m_currBlock->remaining());
    io.writeBinaryString(payloads.data(), payloads.length());
    m_currBlock->consumed(io.position());
    m_uso += io.position();

    m_currBlock->startDRSequenceNumber(m_openSequenceNumber);
    m_currBlock->markAsEventBuffer(type);
    m_currBlock->recordLastCommittedSpHandle(spHandle);
}

int32_t DRTupleStream::getTestDRBuffer(uint8_t drProtocolVersion,
                                       int32_t partitionId,
                                       std::vector<int32_t> partitionKeyValueList,
                                       std::vector<int32_t> flagList,
                                       long startSequenceNumber,
                                       char *outBytes) {
    int tupleStreamPartitionId = partitionId;
    DRTupleStream stream(tupleStreamPartitionId,
                         2 * 1024 * 1024 + MAGIC_HEADER_SPACE_FOR_JAVA + MAGIC_DR_TRANSACTION_PADDING, // 2MB
                         drProtocolVersion);

    char tableHandle[] = { 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f',
                           'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f', 'f' };

    // set up the schema used to fill the new buffer
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnLengths;
    std::vector<bool> columnAllowNull;
    for (int i = 0; i < 2; i++) {
        columnTypes.push_back(ValueType::tINTEGER);
        columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));
        columnAllowNull.push_back(false);
    }
    TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes,
                                                                columnLengths,
                                                                columnAllowNull);
    char tupleMemory[(2 + 1) * 8];
    TableTuple tuple(tupleMemory, schema);

    // Override start sequence number
    stream.m_openSequenceNumber = startSequenceNumber - 1;
    for (int ii = 0; ii < flagList.size(); ii++) {
        bool isMp = (flagList[ii] == TXN_PAR_HASH_MULTI && partitionKeyValueList[ii] != -1) ||
                    flagList[ii] == TXN_PAR_HASH_SPECIAL ||
                    (flagList[ii] == TXN_PAR_HASH_SINGLE && partitionKeyValueList[ii] == -1);
        int64_t uid = UniqueId::makeIdFromComponents(ii * 5, 0, isMp ? 16383 : partitionId);
        tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValueList[ii]));

        if (flagList[ii] == TXN_PAR_HASH_SPECIAL) {
            stream.truncateTable(tableHandle, "foobar", partitionId == 16383 ? -1 : 0, uid, uid);
        }

        for (int zz = 0; zz < 5; zz++) {
            stream.appendTuple(tableHandle, partitionId == 16383 ? -1 : 0, uid, uid, tuple, DR_RECORD_INSERT);
        }

        if (flagList[ii] == TXN_PAR_HASH_MULTI) {
            tuple.setNValue(0, ValueFactory::getIntegerValue(partitionKeyValueList[ii] + 1));
            for (int zz = 0; zz < 5; zz++) {
                stream.appendTuple(tableHandle,  partitionId == 16383 ? -1 : 0, uid, uid, tuple, DR_RECORD_INSERT);
            }
        }
        else if (flagList[ii] == TXN_PAR_HASH_SPECIAL) {
            stream.truncateTable(tableHandle, "foobar", partitionId == 16383 ? -1 : 0, uid, uid);
            stream.truncateTable(tableHandle, "foobar", partitionId == 16383 ? -1 : 0, uid, uid);
        }

        stream.endTransaction(uid);
    }

    TupleSchema::freeTupleSchema(schema);

    size_t headerSize = MAGIC_HEADER_SPACE_FOR_JAVA + MAGIC_DR_TRANSACTION_PADDING;
    const int32_t adjustedLength = static_cast<int32_t>(stream.m_currBlock->rawLength() - headerSize);
    ::memcpy(outBytes, stream.m_currBlock->rawPtr() + headerSize, adjustedLength);
    return adjustedLength;
}

int DRTupleStream::getDRLogHeaderSize() {
    return DR_LOG_HEADER_SIZE + ExecutorContext::getEngine()->getDRHiddenColumnSize();
}
