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

package org.voltdb.jni;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.UserDefinedAggregateFunctionRunner;
import org.voltdb.UserDefinedScalarFunctionRunner;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.common.Constants;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.export.ExportManager;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.sysprocs.saverestore.HiddenColumnFilter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.base.Charsets;

/* Serializes data over a connection that presumably is being read
 * by a voltdb execution engine. The serialization is currently a
 * a packed binary big endian buffer. Each buffer has a header
 * that provides the total length (as an int) and the command being
 * serialized (also as an int). Commands are described by the Commands
 * enum.
 *
 * These serializations are implemented as required since a lot of
 * the native interface isn't needed in many cases.
 *
 * The serialization could be more robust. Probably the right way to
 * get that robustness would be to create FastSerializable classes
 * for the more complex calls and read them through the fast serializable
 * C++ interface.
 *
 * (The fastserializer still requires hand-authoring the ser/deser
 * logic in java and C and doesn't result in any more favorable
 * packing; in fact the resulting wire content might be the same in
 * most cases... And you can't fast serialize an array of primitive
 * types without writing a wrapper object.)
 *
 * Responses to the IPC interface start with a 1 byte result code. If the
 * result code is failure then no message follows. If the result code
 * is success and a response is warranted it will follow and is length prefixed if necessary.
 * Unlike requests to the EE, the length prefix in responses does not include the 1 byte
 * result code nor the 4 byte length field. When we correct the 1 and 4 byte network reads and writes
 * we can this. A length will not prefix the response
 * if the length of the response can be deduced from the original request.
 *
 * The return message format for DMLPlanFragments is all big endian:
 * 1 byte result code
 * 8 byte results codes. Same number of results as numPlanFragments.
 *
 * The return message format for QueryPlanFragments is all big endian:
 * 1 byte result code
 * 4 byte result length. Does not include the length of the result code or result length field.
 * X bytes of serialized VoltTables. Same number of tables as numPlanFragments
 *
 * The return message format for PlanFragment is all big endian:
 * 1 byte result code
 * 4 byte result length. Does not include the length of the result code or result length field.
 * 4 byte result indicating number of dependencies returned
 * The dependency tables
 *
 * The return message format for ReceiveDependency consists of a single byte result code.
 *
 * The return message format for Load table consists of a single byte result code.
 */

public class ExecutionEngineIPC extends ExecutionEngine {

    /** Commands are serialized over the connection */
    private enum Commands {
        Initialize(0)
        , LoadCatalog(2)
        , ToggleProfiler(3)
        , Tick(4)
        , GetStats(5)
        , QueryPlanFragments(6)
        , PlanFragment(7)
        , LoadTable(9)
        , releaseUndoToken(10)
        , undoUndoToken(11)
        , CustomPlanFragment(12)
        , SetLogLevels(13)
        , Quiesce(16)
        , ActivateTableStream(17)
        , TableStreamSerializeMore(18)
        , UpdateCatalog(19)
        , ExportAction(20)
        , RecoveryMessage(21)
        , TableHashCode(22)
        , Hashinate(23)
        , GetPoolAllocations(24)
        , GetUSOs(25)
        , UpdateHashinator(27)
        , ExecuteTask(28)
        , ApplyBinaryLog(29)
        , ShutDown(30)
        , SetViewsEnabled(31)
        , DeleteMigratedRows(32)
        , DisableExternalStreams(33)
        , ExternalStreamsEnabled(34)
        , StoreTopicsGroup(35)
        , DeleteTopicsGroup(36)
        , FetchTopicsGroups(37)
        , CommitTopicsGroupOffsets(38)
        , FetchTopicsGroupOffsets(39)
        , DeleteExpiredTopicsOffsets(40)
        , SetReplicableTables(41)
        , ClearAllReplicableTables(42)
        , ClearReplicableTables(43);

        Commands(final int id) {
            m_id = id;
        }

        int m_id;
    }

    /**
     * One connection per ExecutionEngineIPC. This connection also interfaces
     * with Valgrind to report any problems that Valgrind may find including
     * reachable heap blocks on exit. Unfortunately it is not enough to inspect
     * the Valgrind return code as it considers reachable blocks to not be an
     * error.
     **/
    private class Connection {
        private Socket m_socket = null;
        private SocketChannel m_socketChannel = null;
        Connection(BackendTarget target, int port) {
            boolean connected = false;
            int retries = 0;
            while (!connected) {
                try {
                    System.out.println("Connecting to localhost:" + port);
                    m_socketChannel = SocketChannel.open(new InetSocketAddress(
                            "localhost", port));
                    m_socketChannel.configureBlocking(true);
                    m_socket = m_socketChannel.socket();
                    m_socket.setTcpNoDelay(true);
                    connected = true;
                } catch (final Exception e) {
                    System.out.println(e.getMessage());
                    if (retries++ <= 10) {
                        if (retries > 1) {
                            System.out.printf("Failed to connect to IPC EE on port %d. Retry #%d of 10\n", port, retries-1);
                            try {
                                Thread.sleep(10000);
                            }
                            catch (InterruptedException e1) {}
                        }
                    }
                    else {
                        System.out.printf("Failed to initialize IPC EE connection on port %d. Quitting.\n", port);
                        System.exit(-1);
                    }
                }
                if (!connected && retries == 1 && target == BackendTarget.NATIVE_EE_IPC) {
                    System.out.println("Ready to connect to voltdbipc process on port " + port);
                    System.out.println("Press Enter after you have started the EE process to initiate the connection to the EE");
                    try {
                        System.in.read();
                    } catch (final IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
            System.out.println("Created IPC connection for site.");
        }

        /* Close the socket indicating to the EE it should terminate */
        public void close() throws InterruptedException {
            if (m_socketChannel != null) {
                try {
                    m_socketChannel.close();
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
                m_socketChannel = null;
                m_socket = null;
            }
        }

        /** blocking write of all m_data to outputstream */
        void write() throws IOException {
            // write 4 byte length (which includes its own 4 bytes) in big-endian
            // order. this hurts .. but I'm not sure of a better way.
            m_dataNetwork.clear();
            final int amt = m_data.remaining();
            m_dataNetwork.putInt(4 + amt);
            if (m_dataNetwork.capacity() < (4 + amt)) {
                throw new IOException("Catalog data size (" + (4 + amt) +
                                      ") exceeds ExecutionEngineIPC's hard-coded data buffer capacity (" +
                                      m_dataNetwork.capacity() + ")");
            }
            m_dataNetwork.limit(4 + amt);
            m_dataNetwork.rewind();
            while (m_dataNetwork.hasRemaining()) {
                m_socketChannel.write(m_dataNetwork);
            }
        }

        /**
         * An error code specific to the IPC backend that indicates
         * that as part of fulfilling a previous request the IPC
         * backend requires a dependency table. This is not really
         * an error code.
         */
        static final int kErrorCode_RetrieveDependency = 100;

        /**
         * An error code to be sent in a response to an RetrieveDependency request.
         * Indicates that a dependency table was found and that it follows
         */
        static final int kErrorCode_DependencyFound = 101;

        /**
         * An error code to be sent in a response to an RetrieveDependency request.
         * Indicates that no dependency tables could be found and that no data follows.
         */
        static final int kErrorCode_DependencyNotFound = 102 ;

        /**
         * An error code that can be sent at any time indicating that
         * an export buffer follows
         */
        static final int kErrorCode_pushExportBuffer = 103;

        /**
         * Invoke crash VoltDB
         */
        static final int kErrorCode_CrashVoltDB = 104;

        /**
         * Retrieve value from Java for stats (DEPRECATED)
         */
        //static final int kErrorCode_getQueuedExportBytes = 105;

        /**
         * An error code that can be sent at any time indicating that
         * a per-fragment statistics buffer follows
         */
        static final int kErrorCode_pushPerFragmentStatsBuffer = 106;

        /**
         * Instruct the Java side to invoke a user-defined function and
         * return the result.
         */
        static final int kErrorCode_callJavaUserDefinedFunction = 107;

        /**
         * Instruct the Java side to start a user-defined aggregate function.
         */
        static final int kErrorCode_callJavaUserDefinedAggregateStart = 114;

        /**
         * Instruct the Java side to assemble a user-defined aggregate function.
         */
        static final int kErrorCode_callJavaUserDefinedAggregateAssemble = 115;

        /**
         * Instruct the Java side to combine a result from another user-defined aggregate with this one.
         */
        static final int kErrorCode_callJavaUserDefinedAggregateCombine = 116;

        /**
         * Instruct the Java side to complete the worker portion of a user-defined aggregate function.
         */
        static final int kErrorCode_callJavaUserDefinedAggregateWorkerEnd = 117;

        /**
         * Instruct the Java side to calculate the result of a user-defined aggregate function.
         */
        static final int kErrorCode_callJavaUserDefinedAggregateCoordinatorEnd = 118;

        ByteBuffer getBytes(int size) throws IOException {
            ByteBuffer header = ByteBuffer.allocate(size);
            while (header.hasRemaining()) {
                final int read = m_socket.getChannel().read(header);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            header.flip();
            return header;
        }

        private ByteBuffer readMessage() throws IOException {
            int bufferSize = m_connection.readInt();
            final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            while (buffer.hasRemaining()) {
                int read = m_socketChannel.read(buffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            buffer.flip();
            return buffer;
        }

        // Read per-fragment stats from the wire.
        void extractPerFragmentStatsInternal() {
            try {
                final ByteBuffer perFragmentStatsBuffer = readMessage();

                // Skip the perFragmentTimingEnabled flag.
                perFragmentStatsBuffer.get();
                m_succeededFragmentsCount = perFragmentStatsBuffer.getInt();
                if (m_perFragmentTimingEnabled) {
                    for (int i = 0; i < m_succeededFragmentsCount; i++) {
                        m_executionTimes[i] = perFragmentStatsBuffer.getLong();
                    }
                    // This is the time for the failed fragment.
                    if (m_succeededFragmentsCount < m_executionTimes.length) {
                        m_executionTimes[m_succeededFragmentsCount] = perFragmentStatsBuffer.getLong();
                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Internal function to receive and execute the UDF invocation request.
        void callJavaUserDefinedFunctionInternal() {
            try {
                // Read the request content from the wire.
                ByteBuffer udfBuffer = readMessage();

                int functionId = udfBuffer.getInt();
                UserDefinedScalarFunctionRunner udfRunner = m_functionManager.getFunctionRunnerById(functionId);
                assert(udfRunner != null);
                Throwable throwable = null;
                Object returnValue = null;
                try {
                    // Call the user-defined function.
                    returnValue = udfRunner.call(udfBuffer);
                    m_data.clear();
                    // Put the status code for success (zero) into the buffer.
                    m_data.putInt(0);
                    // Write the result to the buffer.
                    UserDefinedScalarFunctionRunner.writeValueToBuffer(m_data, udfRunner.getReturnType(), returnValue);
                    m_data.flip();
                    m_connection.write();
                    return;
                }
                catch (InvocationTargetException ex1) {
                    // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
                    // We need to get its cause and throw that to the user.
                    throwable = ex1.getCause();
                }
                catch (Throwable ex2) {
                    throwable = ex2;
                }
                // Getting here means the execution was not successful.
                m_data.clear();
                if (throwable != null) {
                    // Exception thrown, put return code = -1.
                    m_data.putInt(-1);
                    byte[] errorMsg = throwable.toString().getBytes(Constants.UTF8ENCODING);
                    SerializationHelper.writeVarbinary(errorMsg, m_data);
                }
                m_data.flip();
                m_connection.write();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void callJavaUserDefinedAggregateFunction(int operationId) {
            try {
                // Read the request content from the wire.
                ByteBuffer udafBuffer = readMessage();

                int functionId = udafBuffer.getInt();
                UserDefinedAggregateFunctionRunner udafRunner = m_functionManager
                        .getAggregateFunctionRunnerById(functionId);
                assert (udafRunner != null);

                Throwable throwable = null;
                try {
                    m_data.clear();
                    // Put the status code for success (zero) into the buffer.
                    m_data.putInt(0);

                    if (operationId == kErrorCode_callJavaUserDefinedAggregateStart) {
                        udafRunner.start();
                    } else {
                        int udafIndex = udafBuffer.getInt();

                        switch (operationId) {
                        case kErrorCode_callJavaUserDefinedAggregateAssemble:
                            udafRunner.assemble(udafBuffer, udafIndex);
                            break;
                        case kErrorCode_callJavaUserDefinedAggregateCombine:
                            udafRunner.combine(UserDefinedAggregateFunctionRunner.readObject(udafBuffer), udafIndex);
                            break;
                        case kErrorCode_callJavaUserDefinedAggregateWorkerEnd:
                            Object workerInstance = udafRunner.getFunctionInstance(udafIndex);

                            udafRunner.clearFunctionInstance(udafIndex);

                            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                    ObjectOutput out = new ObjectOutputStream(bos)) {
                                out.writeObject(workerInstance);
                                out.flush();

                                UserDefinedScalarFunctionRunner.writeValueToBuffer(m_data, VoltType.VARBINARY,
                                        bos.toByteArray());
                            }
                            break;
                        case kErrorCode_callJavaUserDefinedAggregateCoordinatorEnd:
                            Object returnValue = udafRunner.end(udafIndex);
                            UserDefinedScalarFunctionRunner.writeValueToBuffer(m_data, udafRunner.getReturnType(),
                                    returnValue);
                            break;
                        default:
                            throw new IllegalArgumentException("Unknown operation: " + operationId);
                        }
                    }

                    // Write the result to the buffer.

                    m_data.flip();
                    m_connection.write();
                    return;
                } catch (InvocationTargetException ex1) {
                    // Exceptions thrown during Java reflection will be wrapped into this InvocationTargetException.
                    // We need to get its cause and throw that to the user.
                    throwable = ex1.getCause();
                } catch (Throwable ex2) {
                    throwable = ex2;
                }

                // Getting here means the execution was not successful.
                m_data.clear();

                // Exception thrown, put return code = -1.
                m_data.putInt(-1);
                byte[] errorMsg = throwable.toString().getBytes(Constants.UTF8ENCODING);
                SerializationHelper.writeVarbinary(errorMsg, m_data);
                m_data.flip();
                m_connection.write();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Read a single byte indicating a return code. This method has evolved
         * to include providing dependency tables necessary for the completion of previous
         * request. The method loops ready status bytes instead of recursing to avoid
         * excessive recursion in the case where a large number of dependency tables
         * must be fetched before the request can be satisfied.
         *
         * Facing further evolutionary pressure, readStatusByte has  grown
         * EL buffer reading fins. EL buffers arrive here mid-command execution.
         * @return
         * @throws IOException
         */
        int readStatusByte() throws IOException {
            int status = kErrorCode_RetrieveDependency;

            while (true) {
                status = m_socket.getInputStream().read();
                if (status == kErrorCode_RetrieveDependency) {
                    final ByteBuffer dependencyIdBuffer = ByteBuffer.allocate(4);
                    while (dependencyIdBuffer.hasRemaining()) {
                        final int read = m_socketChannel.read(dependencyIdBuffer);
                        if (read == -1) {
                            throw new IOException("Unable to read enough bytes for dependencyId in order to " +
                            " satisfy IPC backend request for a dependency table");
                        }
                    }
                    dependencyIdBuffer.rewind();
                    sendDependencyTable(dependencyIdBuffer.getInt());
                }
                else if (status == ExecutionEngine.ERRORCODE_PROGRESS_UPDATE) {
                    m_history.append("GOT PROGRESS_UPDATE... ");
                    int batchIndex = m_connection.readInt();
                    int planNodeTypeAsInt = m_connection.readInt();
                    long tuplesFound = m_connection.readLong();
                    long currMemoryInBytes = m_connection.readLong();
                    long peakMemoryInBytes = m_connection.readLong();
                    long nextStep = fragmentProgressUpdate(batchIndex, planNodeTypeAsInt, tuplesFound,
                            currMemoryInBytes, peakMemoryInBytes);
                    m_history.append("...RESPONDING TO PROGRESS_UPDATE...nextStep=" + nextStep);
                    m_data.clear();
                    m_data.putLong(nextStep);
                    m_data.flip();
                    m_connection.write();
                    m_history.append(" WROTE RESPONSE TO PROGRESS_UPDATE\n");
                }
                else if (status == kErrorCode_pushExportBuffer) {
                    // Message structure:
                    // pushExportBuffer error code - 1 byte
                    // partition id - 4 bytes
                    // signature length (in bytes) - 4 bytes
                    // signature - signature length bytes
                    // start sequence number - 8 bytes
                    // tupleCount - 8 bytes
                    // uniqueId - 8 bytes
                    // last committed SpHandle - 8 bytes
                    // export buffer length - 4 bytes
                    // export buffer - export buffer length bytes
                    int partitionId = getBytes(4).getInt();
                    int signatureLength = getBytes(4).getInt();
                    byte signatureBytes[] = new byte[signatureLength];
                    getBytes(signatureLength).get(signatureBytes);
                    String signature = new String(signatureBytes, "UTF-8");
                    long startSequenceNumber = getBytes(8).getLong();
                    long committedSequenceNumber = getBytes(8).getLong();
                    long tupleCount = getBytes(8).getLong();
                    long uniqueId = getBytes(8).getLong();
                    long lastCommittedSpHandle = getBytes(8).getLong();
                    int length = getBytes(4).getInt();
                    ByteBuffer buffer = length == 0 ? null : getBytes(length);
                    ExportManager.pushExportBuffer(
                            partitionId,
                            signature,
                            startSequenceNumber,
                            committedSequenceNumber,
                            tupleCount,
                            uniqueId,
                            lastCommittedSpHandle,
                            0L,
                            buffer == null ? null : DBBPool.wrapBB(buffer));
                }
                else if (status == ExecutionEngine.ERRORCODE_DECODE_BASE64_AND_DECOMPRESS) {
                    int dataLength = m_connection.readInt();
                    String data = m_connection.readString(dataLength);
                    byte[] decodedDecompressedData = CompressionService.decodeBase64AndDecompressToBytes(data);
                    m_data.clear();
                    m_data.put(decodedDecompressedData);
                    m_data.flip();
                    m_connection.write();
                }
                else if (status == kErrorCode_CrashVoltDB) {
                    ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                    while (lengthBuffer.hasRemaining()) {
                        final int read = m_socket.getChannel().read(lengthBuffer);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    lengthBuffer.flip();
                    ByteBuffer messageBuffer = ByteBuffer.allocate(lengthBuffer.getInt());
                    while (messageBuffer.hasRemaining()) {
                        final int read = m_socket.getChannel().read(messageBuffer);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    messageBuffer.flip();
                    final int reasonLength = messageBuffer.getInt();
                    final byte reasonBytes[] = new byte[reasonLength];
                    messageBuffer.get(reasonBytes);
                    final String message = new String(reasonBytes, "UTF-8");

                    final int filenameLength = messageBuffer.getInt();
                    final byte filenameBytes[] = new byte[filenameLength];
                    messageBuffer.get(filenameBytes);
                    final String filename = new String(filenameBytes, "UTF-8");

                    final int lineno = messageBuffer.getInt();


                    final int numTraces = messageBuffer.getInt();
                    final String traces[] = new String[numTraces];

                    for (int ii = 0; ii < numTraces; ii++) {
                        final int traceLength = messageBuffer.getInt();
                        final byte traceBytes[] = new byte[traceLength];
                        messageBuffer.get(traceBytes);
                        traces[ii] = new String(traceBytes, "UTF-8");
                    }

                    ExecutionEngine.crashVoltDB(message, traces, filename, lineno);
                }
                else if (status == kErrorCode_pushPerFragmentStatsBuffer) {
                    // The per-fragment stats are in the very beginning.
                    extractPerFragmentStatsInternal();
                }
                else if (status == kErrorCode_callJavaUserDefinedFunction) {
                    callJavaUserDefinedFunctionInternal();
                } else if (status >= kErrorCode_callJavaUserDefinedAggregateStart
                        && status <= kErrorCode_callJavaUserDefinedAggregateCoordinatorEnd) {
                    callJavaUserDefinedAggregateFunction(status);
                }
                else {
                    break;
                }
            }

            try {
                checkErrorCode(status);
                return status;
            }
            catch (SerializableException e) {
                throw e;
            }
            catch (RuntimeException e) {
                throw (IOException)e.getCause();
            }
        }


        /**
         * Read and deserialize some number of tables from the wire. Assumes that the message is length prefixed.
         * @param tables Output array as well as indicator of exactly how many tables to read off of the wire
         * @throws IOException
         */
        public void readResultTables(final VoltTable tables[]) throws IOException {
            final ByteBuffer resultTablesLengthBytes = ByteBuffer.allocate(4);

            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (resultTablesLengthBytes.hasRemaining()) {
                int read = m_socketChannel.read(resultTablesLengthBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            resultTablesLengthBytes.flip();

            final int resultTablesLength = resultTablesLengthBytes.getInt();

            // check the dirty-ness of the batch
            final ByteBuffer dirtyBytes = ByteBuffer.allocate(1);
            while (dirtyBytes.hasRemaining()) {
                int read = m_socketChannel.read(dirtyBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            dirtyBytes.flip();
            // check if anything was changed
            final boolean dirty  = dirtyBytes.get() > 0;
            if (dirty) {
                m_dirty = true;
            }

            if (resultTablesLength <= 0) {
                return;
            }

            final ByteBuffer resultTablesBuffer = ByteBuffer
                    .allocate(resultTablesLength);
            //resultTablesBuffer.order(ByteOrder.LITTLE_ENDIAN);
            while (resultTablesBuffer.hasRemaining()) {
                int read = m_socketChannel.read(resultTablesBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            resultTablesBuffer.flip();

            for (int ii = 0; ii < tables.length; ii++) {
                final int dependencyCount = resultTablesBuffer.getInt(); // ignore the table count
                assert(dependencyCount == 1); //Expect one dependency generated per plan fragment
                resultTablesBuffer.getInt(); // ignore the dependency ID
                tables[ii] = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(resultTablesBuffer);
            }
        }

        public ByteBuffer readResultsBuffer() throws IOException {
            // check the dirty-ness of the batch
            final ByteBuffer dirtyBytes = ByteBuffer.allocate(1);
            while (dirtyBytes.hasRemaining()) {
                int read = m_socketChannel.read(dirtyBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            dirtyBytes.flip();
            // check if anything was changed
            m_dirty |= dirtyBytes.get() > 0;

            final ByteBuffer drBufferSizeBytes = ByteBuffer.allocate(4);
            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (drBufferSizeBytes.hasRemaining()) {
                int read = m_socketChannel.read(drBufferSizeBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            drBufferSizeBytes.flip();
            final int drBufferSize = drBufferSizeBytes.getInt();

            final ByteBuffer resultTablesLengthBytes = ByteBuffer.allocate(4);
            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (resultTablesLengthBytes.hasRemaining()) {
                int read = m_socketChannel.read(resultTablesLengthBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            resultTablesLengthBytes.flip();
            final int resultTablesLength = resultTablesLengthBytes.getInt();

            if (resultTablesLength <= 0) {
                return resultTablesLengthBytes;
            }

            final ByteBuffer resultTablesBuffer = ByteBuffer
                    .allocate(resultTablesLength+8);
            //resultTablesBuffer.order(ByteOrder.LITTLE_ENDIAN);
            resultTablesBuffer.putInt(drBufferSize);
            resultTablesBuffer.putInt(resultTablesLength);
            while (resultTablesBuffer.hasRemaining()) {
                int read = m_socketChannel.read(resultTablesBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            resultTablesBuffer.flip();
            return resultTablesBuffer;
        }

        /**
         * Read and deserialize a long from the wire.
         */
        public long readLong() throws IOException {
            final ByteBuffer longBytes = ByteBuffer.allocate(8);

            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (longBytes.hasRemaining()) {
                int read = m_socketChannel.read(longBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            longBytes.flip();

            final long retval = longBytes.getLong();
            return retval;
        }

        /**
         * Read and deserialize an int from the wire.
         */
        public int readInt() throws IOException {
            final ByteBuffer intBytes = ByteBuffer.allocate(4);

            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (intBytes.hasRemaining()) {
                int read = m_socketChannel.read(intBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            intBytes.flip();

            final int retval = intBytes.getInt();
            return retval;
        }

        /**
         * Read and deserialize a short from the wire.
         */
        public short readShort() throws IOException {
            final ByteBuffer shortBytes = ByteBuffer.allocate(2);

            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (shortBytes.hasRemaining()) {
                int read = m_socketChannel.read(shortBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            shortBytes.flip();

            final short retval = shortBytes.getShort();
            return retval;
        }

        /**
         * Read and deserialize a byte from the wire.
         */
        public byte readByte() throws IOException {
            final ByteBuffer bytes = ByteBuffer.allocate(1);

            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (bytes.hasRemaining()) {
                int read = m_socketChannel.read(bytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            bytes.flip();

            final byte retval = bytes.get();
            return retval;
        }

        /**
         * Read and deserialize a string from the wire.
         */
        public String readString(int size) throws IOException {
            final ByteBuffer stringBytes = ByteBuffer.allocate(size);

            //resultTablesLengthBytes.order(ByteOrder.LITTLE_ENDIAN);
            while (stringBytes.hasRemaining()) {
                int read = m_socketChannel.read(stringBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            stringBytes.flip();

            final String retval = new String(stringBytes.array(), Constants.UTF8ENCODING);
            return retval;
        }

        public void throwException(final int errorCode) throws IOException {
            final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while (lengthBuffer.hasRemaining()) {
                int read = m_socketChannel.read(lengthBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            lengthBuffer.flip();
            final int exceptionLength = lengthBuffer.getInt();//Length is only between EE and Java.
            if (exceptionLength == 0) {
                throw new EEException(errorCode);
            } else {
                final ByteBuffer exceptionBuffer = ByteBuffer.allocate(exceptionLength + 4);
                exceptionBuffer.putInt(exceptionLength);
                while(exceptionBuffer.hasRemaining()) {
                    int read = m_socketChannel.read(exceptionBuffer);
                    if (read == -1) {
                        throw new EOFException();
                    }
                }
                assert(!exceptionBuffer.hasRemaining());
                exceptionBuffer.rewind();
                throw SerializableException.deserializeFromBuffer(exceptionBuffer);
            }
        }
    }

    /** Local m_data */
    private final int m_clusterIndex;
    private final long m_siteId;
    private final int m_partitionId;
    private final int m_hostId;
    private final String m_hostname;
    // private final FastSerializer m_fser;
    private final Connection m_connection;
    private BBContainer m_dataNetworkOrigin;
    private ByteBuffer m_dataNetwork;
    private ByteBuffer m_data;

    // private int m_counter;

    private void verifyDataCapacity(int size) {
        if (size+4 > m_dataNetwork.capacity()) {
            m_dataNetworkOrigin.discard();
            m_dataNetworkOrigin = org.voltcore.utils.DBBPool.allocateDirect(size+4);
            m_dataNetwork = m_dataNetworkOrigin.b();
            m_dataNetwork.position(4);
            m_data = m_dataNetwork.slice();
        }
    }

    public ExecutionEngineIPC(
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int sitesPerHost,
            final int hostId,
            final String hostname,
            final int drClusterId,
            final int defaultDrBufferSize,
            final boolean drIgnoreConflicts,
            final int drCrcErrorIgnoreMax,
            final boolean drCrcErrorIgnoreFatal,
            final int tempTableMemory,
            final BackendTarget target,
            final int port,
            final HashinatorConfig hashinatorConfig,
            final boolean isLowestSiteId) {
        super(siteId, partitionId);

        // m_counter = 0;
        m_clusterIndex = clusterIndex;
        m_siteId = siteId;
        m_partitionId = partitionId;
        m_hostId = hostId;
        m_hostname = hostname;
        // m_fser = new FastSerializer(false, false);
        m_connection = new Connection(target, port);

        // voltdbipc assumes host byte order everywhere
        // Arbitrarily set to 20MB when 10MB crashed for an arbitrarily scaled unit test.
        m_dataNetworkOrigin = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 20);
        m_dataNetwork = m_dataNetworkOrigin.b();
        m_dataNetwork.position(4);
        m_data = m_dataNetwork.slice();

        initialize(
                m_clusterIndex,
                m_siteId,
                m_partitionId,
                sitesPerHost,
                m_hostId,
                m_hostname,
                drClusterId,
                defaultDrBufferSize,
                drIgnoreConflicts,
                drCrcErrorIgnoreMax,
                drCrcErrorIgnoreFatal,
                1024 * 1024 * tempTableMemory,
                hashinatorConfig,
                isLowestSiteId);
    }

    /** Utility method to generate an EEXception that can be overriden by derived classes**/
    @Override
    protected void throwExceptionForError(final int errorCode) {
        try {
            m_connection.throwException(errorCode);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final StringBuffer m_history = new StringBuffer();
    @Override
    public void release() throws EEException, InterruptedException {
        System.out.println("Shutdown IPC connection in progress.");
        System.out.println("But first, a little history:\n" + m_history );
        shutDown();
        m_connection.close();
        System.out.println("Shutdown IPC connection done.");
        m_dataNetworkOrigin.discard();
    }

    @Override
    public void decommission(boolean remove, boolean promote, int newSitePerHost) throws EEException, InterruptedException {
        System.out.println("Decommissioning IPC connection in progress.");
        System.out.println("But first, a little history:\n" + m_history );


        System.out.println("Decommissioned IPC connection done.");
    }

    private void shutDown() {
        int result = ExecutionEngine.ERRORCODE_ERROR;
        m_data.clear();
        m_data.putInt(Commands.ShutDown.m_id);
        try {
            m_data.flip();
            m_connection.write();
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Excpeption: " + e.getMessage());
            throw new RuntimeException();
        }
        checkErrorCode(result);
    }

    private static final Object printLockObject = new Object();
    /**
     * the abstract api assumes construction initializes but here initialization
     * is just another command.
     */
    public void initialize(
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int sitesPerHost,
            final int hostId,
            final String hostname,
            final int drClusterId,
            final int defaultDrBufferSize,
            final boolean drIgnoreConflicts,
            final int drCrcErrorIgnoreMax,
            final boolean drCrcErrorIgnoreFatal,
            final long tempTableMemory,
            final HashinatorConfig hashinatorConfig,
            final boolean createDrReplicatedStream)
    {
        synchronized(printLockObject) {
            System.out.println("Initializing an IPC EE " + this + " for hostId " + hostId + " siteId " + siteId + " from thread " + Thread.currentThread().getId());
        }
        int result = ExecutionEngine.ERRORCODE_ERROR;
        m_data.clear();
        m_data.putInt(Commands.Initialize.m_id);
        m_data.putInt(clusterIndex);
        m_data.putLong(siteId);
        m_data.putInt(partitionId);
        m_data.putInt(sitesPerHost);
        m_data.putInt(hostId);
        m_data.putInt(drClusterId);
        m_data.putInt(defaultDrBufferSize);
        m_data.putInt(drIgnoreConflicts ? 1 : 0);
        m_data.putInt(drCrcErrorIgnoreMax);
        m_data.putInt(drCrcErrorIgnoreFatal ? 1 : 0);
        m_data.putLong(EELoggers.getLogLevels());
        m_data.putLong(tempTableMemory);
        m_data.putInt(createDrReplicatedStream ? 1 : 0);
        m_data.putInt((short)hostname.length());
        m_data.put(hostname.getBytes(Charsets.UTF_8));
        try {
            m_data.flip();
            m_connection.write();
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
        checkErrorCode(result);
        updateHashinator(hashinatorConfig);
    }

    /** write the catalog as a UTF-8 byte string via connection */
    @Override
    protected void coreLoadCatalog(final long timestamp, final byte[] catalogBytes) throws EEException {
        int result = ExecutionEngine.ERRORCODE_ERROR;
        verifyDataCapacity(catalogBytes.length + 100);
        m_data.clear();
        m_data.putInt(Commands.LoadCatalog.m_id);
        m_data.putLong(timestamp);
        m_data.put(catalogBytes);
        m_data.put((byte)'\0');

        try {
            m_data.flip();
            m_connection.write();
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
        checkErrorCode(result);
    }

    /** write the diffs as a UTF-8 byte string via connection */
    @Override
    public void coreUpdateCatalog(final long timestamp, final boolean isStreamUpdate, final String catalogDiffs) throws EEException {
        int result = ExecutionEngine.ERRORCODE_ERROR;

        try {
            final byte catalogBytes[] = catalogDiffs.getBytes("UTF-8");
            verifyDataCapacity(catalogBytes.length + 100);
            m_data.clear();
            m_data.putInt(Commands.UpdateCatalog.m_id);
            m_data.putLong(timestamp);
            m_data.putInt(isStreamUpdate ? 1 : 0);
            m_data.put(catalogBytes);
            m_data.put((byte)'\0');
        } catch (final UnsupportedEncodingException ex) {
            Logger.getLogger(ExecutionEngineIPC.class.getName()).log(
                    Level.SEVERE, null, ex);
        }

        try {
            m_data.flip();
            m_connection.write();
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
        checkErrorCode(result);
    }

    @Override
    public void tick(final long time, final long lastCommittedSpHandle) {
        int result = ExecutionEngine.ERRORCODE_ERROR;
        m_data.clear();
        m_data.putInt(Commands.Tick.m_id);
        m_data.putLong(time);
        m_data.putLong(lastCommittedSpHandle);
        try {
            m_data.flip();
            m_connection.write();
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
        checkErrorCode(result);
        // no return code for tick.
    }

    @Override
    public void quiesce(long lastCommittedSpHandle) {
        int result = ExecutionEngine.ERRORCODE_ERROR;
        m_data.clear();
        m_data.putInt(Commands.Quiesce.m_id);
        m_data.putLong(lastCommittedSpHandle);
        try {
            m_data.flip();
            m_connection.write();
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Excpeption: " + e.getMessage());
            throw new RuntimeException();
        }
        checkErrorCode(result);
    }

    private void sendPlanFragmentsInvocation(final Commands cmd,
            final int numFragmentIds,
            final long[] planFragmentIds,
            long[] inputDepIdsIn,
            final Object[] parameterSets,
            DeterminismHash determinismHash,
            boolean[] isWriteFrags,
            int[] sqlCRCs,
            final long txnId,
            final long spHandle,
            final long lastCommittedSpHandle,
            final long uniqueId,
            final long undoToken)
    {
        // big endian, not direct
        final FastSerializer fser = new FastSerializer();
        try {
            for (int i = 0; i < numFragmentIds; ++i) {
                Object params = parameterSets[i];
                // pset can be ByteBuffer or ParameterSet instance
                int paramStart = fser.getPosition();
                if (params instanceof ByteBuffer) {
                    ByteBuffer buf = (ByteBuffer) params;
                    fser.write(buf);
                }
                else {
                    ParameterSet pset = (ParameterSet) params;
                    fser.writeParameterSet(pset);
                }
                if (determinismHash != null && isWriteFrags[i]) {
                    determinismHash.offerStatement(sqlCRCs[i], paramStart, fser.getContainerNoFlip().b());
                }
            }
        } catch (final Exception exception) { // ParameterSet serialization can throw RuntimeExceptions
            fser.discard();
            throw new RuntimeException(exception);
        }

        // if inputDepIds is null, make a bunch of dummies
        long[] inputDepIds = inputDepIdsIn;
        if (inputDepIds == null) {
            inputDepIds = new long[numFragmentIds];
            for (int i = 0; i < inputDepIds.length; i++) {
                inputDepIds[0] = -1;
            }
        }

        m_data.clear();
        do {
            m_data.putInt(cmd.m_id);
            m_data.putLong(txnId);
            m_data.putLong(spHandle);
            m_data.putLong(lastCommittedSpHandle);
            m_data.putLong(uniqueId);
            m_data.putLong(undoToken);
            m_data.put((m_perFragmentTimingEnabled ? (byte)1 : (byte)0));
            m_data.putInt(numFragmentIds);
            for (int i = 0; i < numFragmentIds; ++i) {
                m_data.putLong(planFragmentIds[i]);
            }
            for (int i = 0; i < numFragmentIds; ++i) {
                m_data.putLong(inputDepIds[i]);
            }
            verifyDataCapacity(m_data.position()+fser.size());
        } while (m_data.position() == 0);
        m_data.put(fser.getBuffer());
        fser.discard();

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public FastDeserializer coreExecutePlanFragments(
            final int bufferHint,
            final int numFragmentIds,
            final long[] planFragmentIds,
            final long[] inputDepIds,
            final Object[] parameterSets,
            DeterminismHash determinismHash,
            boolean[] isWriteFrags,
            int[] sqlCRCs,
            final long txnId,
            final long spHandle,
            final long lastCommittedSpHandle,
            final long uniqueId,
            final long undoToken, boolean traceOn) throws EEException {
        sendPlanFragmentsInvocation(Commands.QueryPlanFragments,
                numFragmentIds, planFragmentIds, inputDepIds, parameterSets, determinismHash, isWriteFrags, sqlCRCs,
                txnId, spHandle, lastCommittedSpHandle, uniqueId, undoToken);
        int result = ExecutionEngine.ERRORCODE_ERROR;
        if (m_perFragmentTimingEnabled) {
            m_executionTimes = new long[numFragmentIds];
        }

        while (true) {
            try {
                result = m_connection.readStatusByte();
                ByteBuffer resultTables = null;

                if (result == ExecutionEngine.ERRORCODE_NEED_PLAN) {
                    long fragmentId = m_connection.readLong();
                    byte[] plan = planForFragmentId(fragmentId);
                    m_data.clear();
                    m_data.put(plan);
                    m_data.flip();
                    m_connection.write();
                }
                else if (result == ExecutionEngine.ERRORCODE_SUCCESS) {
                    try {
                        resultTables = m_connection.readResultsBuffer();
                    } catch (final IOException e) {
                        throw new EEException(
                                ExecutionEngine.ERRORCODE_WRONG_SERIALIZED_BYTES);
                    }
                    return new FastDeserializer(resultTables);
                }
                else {
                    // failure
                    return null;
                }
            }
            catch (final IOException e) {
                m_history.append("GOT IOException: " + e.toString());
                System.out.println("Exception: " + e.getMessage());
                throw new RuntimeException(e);
            }
            catch (final Throwable thrown) {
                thrown.printStackTrace();
                m_history.append("GOT Throwable: " + thrown.toString());
                throw thrown;
            }
        }
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Unsupported implementation of toggleProfiler
     */
    @Override
    public void toggleProfiler(final int toggle) {
    }


    @Override
    public byte[] loadTable(final int tableId, final VoltTable table, final long txnId,
            final long spHandle, final long lastCommittedSpHandle, final long uniqueId,
            long undoToken, LoadTableCaller caller)
    throws EEException
    {
        if (caller == LoadTableCaller.DR || caller == LoadTableCaller.SNAPSHOT_REPORT_UNIQ_VIOLATIONS
                || caller == LoadTableCaller.BALANCE_PARTITIONS) {
            throw new UnsupportedOperationException("Haven't added IPC support for returning unique violations");
        }
        final ByteBuffer tableBytes = PrivateVoltTableFactory.getTableDataReference(table);
        m_data.clear();
        do {
            m_data.putInt(Commands.LoadTable.m_id);
            m_data.putInt(tableId);
            m_data.putLong(txnId);
            m_data.putLong(spHandle);
            m_data.putLong(lastCommittedSpHandle);
            m_data.putLong(uniqueId);
            m_data.putLong(undoToken);
            m_data.put(caller.getId());
            verifyDataCapacity(m_data.position() + tableBytes.remaining());
        } while (m_data.position() == 0);

        m_data.put(tableBytes);

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        int result = ExecutionEngine.ERRORCODE_ERROR;
        try {
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        if (result != ExecutionEngine.ERRORCODE_SUCCESS) {
            throw new EEException(result);
        }
        /*//
        // This code will hang expecting input that never arrives
        // until voltdbipc is extended to respond with information
        // negative or positive about "unique violations".
        try {
            ByteBuffer responseBuffer = readMessage();
            if (responseBuffer != null) {
                return responseBuffer.array();
            }
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
        //*/
        return null;
    }

    @Override
    public VoltTable[] getStats(
            final StatsSelector selector,
            final int[] locators,
            final boolean interval,
            final Long now) {
        m_data.clear();
        m_data.putInt(Commands.GetStats.m_id);
        m_data.putInt(selector.ordinal());
        if (interval) {
            m_data.put((byte)1);
        } else {
            m_data.put((byte)0);
        }
        m_data.putLong(now);
        m_data.putInt(locators.length);
        for (final int locator : locators) {
            m_data.putInt(locator);
        }

        m_data.flip();
        try {
            m_connection.write();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        int result = ExecutionEngine.ERRORCODE_ERROR;
        try {
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("IPC exception reading statistics status: " + e.getMessage());
        }

        try {
            if (result == ExecutionEngine.ERRORCODE_SUCCESS) {

                final ByteBuffer messageBuffer = readMessage();
                if (messageBuffer == null) {
                    return null;
                }

                final VoltTable results[] = new VoltTable[1];
                results[0] = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(messageBuffer);
                return results;
            }
        } catch (final IOException e) {
            System.out.println("IPC exception reading statistics table: " + e.getMessage());

        }
        return null;
    }

    private ByteBuffer readMessage() throws IOException {
        final ByteBuffer messageLengthBuffer = ByteBuffer.allocate(4);
        while (messageLengthBuffer.hasRemaining()) {
            int read = m_connection.m_socketChannel.read(messageLengthBuffer);
            if (read == -1) {
                throw new EOFException("End of file reading statistics(1)");
            }
        }
        messageLengthBuffer.rewind();
        int length = messageLengthBuffer.getInt();
        if (length == 0) {
            return null;
        }
        final ByteBuffer messageBuffer = ByteBuffer.allocate(length);
        while (messageBuffer.hasRemaining()) {
            int read = m_connection.m_socketChannel.read(messageBuffer);
            if (read == -1) {
                throw new EOFException("End of file reading statistics(2)");
            }
        }
        messageBuffer.rewind();
        return messageBuffer;
    }

    @Override
    public boolean releaseUndoToken(final long undoToken, boolean isEmptyDRTxn) {
        m_data.clear();
        m_data.putInt(Commands.releaseUndoToken.m_id);
        m_data.putLong(undoToken);
        m_data.put((byte) (isEmptyDRTxn ? 1 : 0));

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        int result = ExecutionEngine.ERRORCODE_ERROR;
        try {
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        if (result != ExecutionEngine.ERRORCODE_SUCCESS) {
            return false;
        }
        return true;
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        m_data.clear();
        m_data.putInt(Commands.undoUndoToken.m_id);
        m_data.putLong(undoToken);

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        int result = ExecutionEngine.ERRORCODE_ERROR;
        try {
            result = m_connection.readStatusByte();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        if (result != ExecutionEngine.ERRORCODE_SUCCESS) {
            return false;
        }
        return true;
    }

    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        m_data.clear();
        m_data.putInt(Commands.SetLogLevels.m_id);
        m_data.putLong(logLevels);

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        int result = ExecutionEngine.ERRORCODE_ERROR;
        try {
            result = m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        if (result != ExecutionEngine.ERRORCODE_SUCCESS) {
            return false;
        }
        return true;
    }

    /**
     * Retrieve a dependency table and send it via the connection. If
     * no table is available send a response code indicating such.
     * The message is prepended with two lengths. One length is for
     * the network layer and is the size of the whole message not including
     * the length prefix.
     * @param dependencyId ID of the dependency table to send to the client
     */
    private void sendDependencyTable(final int dependencyId) throws IOException{
        final byte[] dependencyBytes = nextDependencyAsBytes(dependencyId);
        if (dependencyBytes == null) {
            m_connection.m_socket.getOutputStream().write(Connection.kErrorCode_DependencyNotFound);
            return;
        }
        // 1 for response code + 4 for dependency length prefix + dependencyBytes.length
        final ByteBuffer message = ByteBuffer.allocate(1 + 4 + dependencyBytes.length);

        // write the response code
        message.put((byte)Connection.kErrorCode_DependencyFound);

        // write the dependency's length prefix
        message.putInt(dependencyBytes.length);

        // finally, write dependency table itself
        message.put(dependencyBytes);
        message.rewind();
        if (m_connection.m_socketChannel.write(message) != message.capacity()) {
            throw new IOException("Unable to send dependency table to client. Attempted blocking write of " +
                    message.capacity() + " but not all of it was written");
        }
    }

    @Override
    public Pair<byte[], Integer> getSnapshotSchema(int tableId, HiddenColumnFilter hiddenColumnFilter,
            boolean forceLive) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean activateTableStream(
            int tableId,
            TableStreamType streamType,
            HiddenColumnFilter hiddenColumnFilter,
            long undoQuantumToken,
            byte[] predicates) {
        m_data.clear();
        m_data.putInt(Commands.ActivateTableStream.m_id);
        m_data.putInt(tableId);
        m_data.putInt(streamType.ordinal());
        m_data.put(hiddenColumnFilter.getId());
        m_data.putLong(undoQuantumToken);
        m_data.put(predicates); // predicates

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        int result = ExecutionEngine.ERRORCODE_ERROR;
        try {
            result = m_connection.readStatusByte();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        if (result != ExecutionEngine.ERRORCODE_SUCCESS) {
            return false;
        }
        return true;
    }

    @Override
    public Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType streamType,
                                                      List<BBContainer> outputBuffers) {
        try {
            m_data.clear();
            m_data.putInt(Commands.TableStreamSerializeMore.m_id);
            m_data.putInt(tableId);
            m_data.putInt(streamType.ordinal());
            m_data.put(SnapshotUtil.OutputBuffersToBytes(outputBuffers));

            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();

            // Get the count.
            ByteBuffer countBuffer = ByteBuffer.allocate(4);
            while (countBuffer.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(countBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            countBuffer.flip();
            final int count = countBuffer.getInt();
            assert count == outputBuffers.size();

            // Get the remaining tuple count.
            ByteBuffer remainingBuffer = ByteBuffer.allocate(8);
            while (remainingBuffer.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(remainingBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            remainingBuffer.flip();
            final long remaining = remainingBuffer.getLong();

            final int[] serialized;

            if (count > 0) {
                serialized = new int[count];
            } else {
                serialized = new int[]{0};
            }
            for (int i = 0; i < count; i++) {
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                while (lengthBuffer.hasRemaining()) {
                    int read = m_connection.m_socketChannel.read(lengthBuffer);
                    if (read == -1) {
                        throw new EOFException();
                    }
                }
                lengthBuffer.flip();
                serialized[i] = lengthBuffer.getInt();
                ByteBuffer view = outputBuffers.get(i).b().duplicate();
                view.limit(view.position() + serialized[i]);
                while (view.hasRemaining()) {
                    m_connection.m_socketChannel.read(view);
                }
            }
            return Pair.of(remaining, serialized);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setExportStreamPositions(ExportSnapshotTuple sequences, int partitionId, String mStreamName) {
        try {
            m_data.clear();
            m_data.putInt(Commands.ExportAction.m_id);
            m_data.putLong(sequences.getAckOffset());
            m_data.putLong(sequences.getSequenceNumber());
            m_data.putLong(sequences.getGenerationId());
            if (mStreamName == null) {
                m_data.putInt(-1);
            } else {
                m_data.putInt(mStreamName.getBytes("UTF-8").length);
                m_data.put(mStreamName.getBytes("UTF-8"));
            }
            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean deleteMigratedRows(long txnid, long spHandle, long uniqueId,
            String tableName, long deletableTxnId, long undoToken) {
        try {
            m_data.clear();
            m_data.putInt(Commands.DeleteMigratedRows.m_id);
            m_data.putLong(txnid);
            m_data.putLong(spHandle);
            m_data.putLong(uniqueId);
            m_data.putLong(deletableTxnId);
            m_data.putLong(undoToken);
            m_data.putInt(tableName.getBytes("UTF-8").length);
            m_data.put(tableName.getBytes("UTF-8"));
            m_data.flip();
            m_connection.write();

            ByteBuffer results = ByteBuffer.allocate(1);
            while (results.remaining() > 0) {
                m_connection.m_socketChannel.read(results);
            }
            results.flip();

            return (results.getInt() == 1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long[] getUSOForExportTable(String streamName) {
        long[] retval = null;
        try {
            m_data.clear();
            m_data.putInt(Commands.GetUSOs.m_id);
            if (streamName == null) {
                m_data.putInt(-1);
            } else {
                m_data.putInt(streamName.getBytes("UTF-8").length);
                m_data.put(streamName.getBytes("UTF-8"));
            }
            m_data.flip();
            m_connection.write();

            ByteBuffer results = ByteBuffer.allocate(16);
            while (results.remaining() > 0) {
                m_connection.m_socketChannel.read(results);
            }
            results.flip();

            retval = new long[2];
            retval[0] = results.getLong();
            retval[1] = results.getLong();

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        return retval;
    }

    @Override
    public long tableHashCode(int tableId) {
        try {
            m_data.clear();
            m_data.putInt(Commands.TableHashCode.m_id);
            m_data.putInt(tableId);

            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();
            ByteBuffer hashCode = ByteBuffer.allocate(8);
            while (hashCode.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(hashCode);
                if (read <= 0) {
                    throw new EOFException();
                }
            }
            hashCode.flip();
            return hashCode.getLong();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashinate(Object value, HashinatorConfig config)
    {
        ParameterSet parameterSet = ParameterSet.fromArrayNoCopy(value);
        parameterSet.getSerializedSize(); // in case this memoizes stuff

        m_data.clear();
        m_data.putInt(Commands.Hashinate.m_id);
        m_data.putInt(config.configBytes.length);
        m_data.put(config.configBytes);
        try {
            parameterSet.flattenToBuffer(m_data);

            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();
            ByteBuffer part = ByteBuffer.allocate(4);
            while (part.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(part);
                if (read <= 0) {
                    throw new EOFException();
                }
            }
            part.flip();
            return part.getInt();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateHashinator(HashinatorConfig config)
    {
        m_data.clear();
        m_data.putInt(Commands.UpdateHashinator.m_id);
        m_data.putInt(config.configBytes.length);
        m_data.put(config.configBytes);
        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public long applyBinaryLog(ByteBuffer logs, long txnId, long spHandle, long lastCommittedSpHandle,
            long uniqueId, int remoteClusterId, long undoToken) throws EEException {
        m_data.clear();
        m_data.putInt(Commands.ApplyBinaryLog.m_id);
        m_data.putLong(txnId);
        m_data.putLong(spHandle);
        m_data.putLong(lastCommittedSpHandle);
        m_data.putLong(uniqueId);
        m_data.putInt(remoteClusterId);
        m_data.putLong(undoToken);
        m_data.put(logs.array());

        try {
            m_data.flip();
            m_connection.write();
            ByteBuffer rowCount = ByteBuffer.allocate(8);
            while (rowCount.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(rowCount);
                if (read <= 0) {
                    throw new EOFException();
                }
            }
            rowCount.flip();
            return rowCount.getLong();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        m_data.clear();
        m_data.putInt(Commands.GetPoolAllocations.m_id);
        try {
            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();
            ByteBuffer allocations = ByteBuffer.allocate(8);
            while (allocations.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(allocations);
                if (read <= 0) {
                    throw new EOFException();
                }
            }
            allocations.flip();
            return allocations.getLong();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] executeTask(TaskType taskType, ByteBuffer task) {
        m_data.clear();
        m_data.putInt(Commands.ExecuteTask.m_id);
        m_data.putLong(taskType.taskId);
        m_data.put(task.array());
        try {
            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();
            ByteBuffer length = ByteBuffer.allocate(4);
            while (length.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(length);
                if (read <= 0) {
                    throw new EOFException();
                }
            }
            length.flip();

            ByteBuffer retval = ByteBuffer.allocate(length.getInt());
            while (retval.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(retval);
                if (read <= 0) {
                    throw new EOFException();
                }
            }
            return  retval.array();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer getParamBufferForExecuteTask(int requiredCapacity) {
        return ByteBuffer.allocate(requiredCapacity);
    }

    private boolean m_perFragmentTimingEnabled = false;

    @Override
    public void setPerFragmentTimingEnabled(boolean enabled) {
        m_perFragmentTimingEnabled = enabled;
    }

    private int m_succeededFragmentsCount = 0;
    private long[] m_executionTimes = null;

    @Override
    public int extractPerFragmentStats(int batchSize, long[] executionTimesOut) {
        if (executionTimesOut != null) {
            assert(executionTimesOut.length >= m_succeededFragmentsCount);
            for (int i = 0; i < m_succeededFragmentsCount; i++) {
                executionTimesOut[i] = m_executionTimes[i];
            }
            // This is the time for the failed fragment.
            if (m_succeededFragmentsCount < executionTimesOut.length) {
                executionTimesOut[m_succeededFragmentsCount] = m_executionTimes[m_succeededFragmentsCount];
            }
        }
        return m_succeededFragmentsCount;
    }

    @Override
    public void setViewsEnabled(String viewNames, boolean enabled) {
        if (viewNames.equals("")) {
            return;
        }
        if (enabled) {
            System.out.println("The maintenance of the following views is restarting: " + viewNames);
        }
        else {
            System.out.println("The maintenance of the following views will be paused to accelerate the restoration: " + viewNames);
        }
        m_data.clear();
        m_data.putInt(Commands.SetViewsEnabled.m_id);
        try {
            final byte viewNameBytes[] = viewNames.getBytes("UTF-8");
            m_data.put(enabled ? (byte)1 : (byte)0);
            m_data.put(viewNameBytes);
            m_data.put((byte)'\0');
            m_data.flip();
            m_connection.write();
        } catch (final IOException e) {
            System.out.println("Excpeption: " + e.getMessage());
            throw new RuntimeException();
        }
    }

    @Override
    public void disableExternalStreams() {
        System.out.println("Disabling all external streams in EE");
        m_data.clear();
        m_data.putInt(Commands.DisableExternalStreams.m_id);
        m_data.flip();
        try {
            m_connection.write();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException();
        }
    }

    @Override
    public boolean externalStreamsEnabled() {
        m_data.clear();
        m_data.putInt(Commands.ExternalStreamsEnabled.m_id);
        m_data.flip();
        try {
            m_connection.write();
            m_connection.readStatusByte();
            return m_connection.readByte() == 1 ? true : false;
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException();
        }
    }

    @Override
    public void storeTopicsGroup(long undoToken, byte[] serializedGroup) {
        verifyDataCapacity(Integer.BYTES * 2 + Long.BYTES + serializedGroup.length);

        m_data.clear();
        m_data.putInt(Commands.StoreTopicsGroup.m_id);
        m_data.putLong(undoToken);
        m_data.putInt(serializedGroup.length);
        m_data.put(serializedGroup);
        m_data.flip();

        try {
            m_connection.write();
            m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteTopicsGroup(long undoToken, String groupId) {
        byte[] groupIdBytes = groupId.getBytes(Constants.UTF8ENCODING);
        verifyDataCapacity(Integer.BYTES * 2 + Long.BYTES + groupIdBytes.length);

        m_data.clear();
        m_data.putInt(Commands.DeleteTopicsGroup.m_id);
        m_data.putLong(undoToken);
        m_data.putInt(groupIdBytes.length);
        m_data.put(groupIdBytes);
        m_data.flip();

        try {
            m_connection.write();
            m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Pair<Boolean, byte[]> fetchTopicsGroups(int maxResultSize, String startGroupId) {
        byte[] groupIdBytes = startGroupId == null ? new byte[0] : startGroupId.getBytes(Constants.UTF8ENCODING);
        verifyDataCapacity(Integer.BYTES * 3 + groupIdBytes.length);

        m_data.clear();
        m_data.putInt(Commands.FetchTopicsGroups.m_id);
        m_data.putInt(maxResultSize);
        m_data.putInt(groupIdBytes.length);
        m_data.put(groupIdBytes);
        m_data.flip();

        try {
            m_connection.write();
            m_connection.readStatusByte();
            byte moreGroups = m_connection.readByte();
            int length = m_connection.readInt();
            ByteBuffer data = m_connection.getBytes(length);
            return Pair.of(moreGroups != 0, data.array());
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] commitTopicsGroupOffsets(long spUniqueId, long undoToken, short requestVersion, String groupId,
            byte[] offsets) {
        byte[] groupIdBytes = groupId.getBytes(Constants.UTF8ENCODING);
        verifyDataCapacity(Integer.BYTES * 3 + Long.BYTES * 2 + Short.BYTES + groupIdBytes.length + offsets.length);

        m_data.clear();
        m_data.putInt(Commands.CommitTopicsGroupOffsets.m_id);
        m_data.putLong(spUniqueId);
        m_data.putLong(undoToken);
        m_data.putShort(requestVersion);
        m_data.putInt(groupIdBytes.length);
        m_data.putInt(offsets.length);
        m_data.put(groupIdBytes);
        m_data.put(offsets);

        try {
            m_connection.write();
            m_connection.readStatusByte();
            int length = m_connection.readInt();
            return m_connection.getBytes(length).array();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] fetchTopicsGroupOffsets(short requestVersion, String groupId, byte[] offsets) {
        byte[] groupIdBytes = groupId.getBytes(Constants.UTF8ENCODING);
        verifyDataCapacity(Integer.BYTES * 3 + Short.BYTES + groupIdBytes.length + offsets.length);

        m_data.clear();
        m_data.putInt(Commands.FetchTopicsGroupOffsets.m_id);
        m_data.putShort(requestVersion);
        m_data.putInt(groupIdBytes.length);
        m_data.putInt(offsets.length);
        m_data.put(groupIdBytes);
        m_data.put(offsets);

        try {
            m_connection.write();
            m_connection.readStatusByte();
            int length = m_connection.readInt();
            return m_connection.getBytes(length).array();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteExpiredTopicsOffsets(long undoToken, TimestampType deleteOlderThan) {
        verifyDataCapacity(Long.BYTES * 2);

        m_data.clear();
        m_data.putInt(Commands.DeleteExpiredTopicsOffsets.m_id);
        m_data.putLong(undoToken);
        m_data.putLong(deleteOlderThan.getTime());

        try {
            m_connection.write();
            m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setReplicableTables(int clusterId, String[] tables) {
        byte[][] tableNames = null;
        int tablesSize = 0;
        if (tables != null) {
            tableNames = new byte[tables.length][];
            for (int i = 0; i < tables.length; ++i) {
                tableNames[i] = tables[i].getBytes(Constants.UTF8ENCODING);
                tablesSize += Integer.BYTES + tableNames.length;
            }
        }

        verifyDataCapacity(Byte.BYTES + Integer.BYTES * 2 + tablesSize);

        m_data.clear();
        m_data.putInt(Commands.SetReplicableTables.m_id);
        m_data.putInt(clusterId);
        if (tableNames == null) {
            m_data.putInt(-1);
        } else {
            m_data.putInt(tableNames.length);
            for (byte[] table : tableNames) {
                m_data.putInt(table.length);
                m_data.put(table);
            }
        }

        try {
            m_connection.write();
            m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearAllReplicableTables() {
        verifyDataCapacity(Byte.BYTES);

        m_data.clear();
        m_data.putInt(Commands.ClearAllReplicableTables.m_id);

        try {
            m_connection.write();
            m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clearReplicableTables(int clusterId) {
        verifyDataCapacity(Byte.BYTES);

        m_data.clear();
        m_data.putInt(Commands.ClearReplicableTables.m_id);
        m_data.putInt(clusterId);

        try {
            m_connection.write();
            m_connection.readStatusByte();
        } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
