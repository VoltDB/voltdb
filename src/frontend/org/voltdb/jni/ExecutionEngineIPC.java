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

package org.voltdb.jni;

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.StatsSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.VoltTable;
import org.voltdb.common.Constants;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.export.ExportManager;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.ByteBufferUtil;
import org.voltdb.utils.Encoder;

/* Serializes data over a connection that presumably is being read
 * by a voltdb execution engine. The serialization is currently a
 * a packed binary big endian buffer. Each buffer has a header
 * that provides the total length (as an int) and the command being
 * serialized (also as an int). Commands are described by the Commands
 * enum.
 *
 * These serializations are implemented as required to support easier memory
 * and performance profiling or debugging for specific native interface calls.
 * They need not be implemented for any native calls for which such practices
 * are of no value.
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
 * Responses to the IPC interface start with a 1 byte result code.
 * If the result code is failure then a serialized exception may follow.
 * If the result code is success and further result detail is required,
 * that will follow. Beyond that, the return message formats vary.
 *
 * Variable length message formats TEND to follow the result byte with a
 * 4-byte length but a length is not required for result messages of fixed
 * size or of a variable size that is determinable by the original outgoing
 * request message.
 *
 * By convention, length prefixes in return messages count (only) the bytes
 * that follow (not themselves).
 * There isn't much shared code for deserializing messages or message
 * components above a certain granularity.
 * Helpers for processing length-prefixed error message buffers and
 * length-prefixed UTF-8 strings are the most complex legos in the box.
 * This leaves the specialized message parsers free to be creative about
 * their formats once they've determined that the incoming result message
 * is "one of theirs".
 * That typically gets settled either by the result message's
 * call context or in Connection.pollForSuccess.
 *
 * There have been various proposals for keeping message formats in synch
 * between this code and voltdbipc.cpp, whether by code generators or by
 * self-checking protocols, but for now it's left to synchronized programming.
 */

public class ExecutionEngineIPC extends ExecutionEngine {

    /** Commands are serialized over the connection.
     * So, KEEP the C++ equivalent of this list in
     * voltdbipc.cpp IN SYNCH with it.
     */
    private enum Commands {
        Initialize(0),
        // currently unused(1),
        LoadCatalog(2),
        ToggleProfiler(3),
        Tick(4),
        GetStats(5),
        QueryPlanFragments(6),
        PlanFragment(7),
        // currently unused(8),
        LoadTable(9),
        releaseUndoToken(10),
        undoUndoToken(11),
        CustomPlanFragment(12),
        SetLogLevels(13),
        // currently unused(14),
        // currently unused(15),
        Quiesce(16),
        ActivateTableStream(17),
        TableStreamSerializeMore(18),
        UpdateCatalog(19),
        ExportAction(20),
        RecoveryMessage(21),
        TableHashCode(22),
        Hashinate(23),
        GetPoolAllocations(24),
        GetUSOs(25),
        // currently unused(26),
        updateHashinator(27),
        executeTask(28),
        applyBinaryLog(29);
        /** Commands are serialized over the connection.
         * So, KEEP the C++ equivalent of this list in
         * voltdbipc.cpp IN SYNCH with it.
         */

        Commands(final int id) {
            m_id = id;
        }

        int m_id;
    }

    // These "error codes" are actually command codes for upstream commands to
    // the java process, but they need to be disjoint from other error codes so
    // that the java code can determine when it is getting an upstream command
    // instead of a direct (error) response to its last command.
    // The voltdbipc processing for the responses to these upstream commands
    // does not need response codes distinct from other codes because the
    // upstream messages are responded to synchronously and are never
    // interrupted by other downstream traffic.

    // These definitions MUST MATCH the definitions in voltdbipc.cpp
    private static final int
        ERRORCODE_NEED_PLAN = 90,                         // Request for uncached plan bytes
        ERRORCODE_NEED_PROGRESS_UPDATE = 91,              // Request for "long run" reporting or timeout
        ERRORCODE_NEED_DECODE_BASE64_AND_DECOMPRESS = 92, // Request for decoding work
        ERRORCODE_NEED_DEPENDENCY = 93,                   // Request for dependent data
        ERRORCODE_NEED_BUFFER_EXPORT = 94,                // Request for buffer export
        ERRORCODE_NEED_QUEUED_EXPORT_BYTES_STAT = 95,     // Retrieve value for stats
        ERRORCODE_CRASH_VOLTDB = 99;                      // Crash with context string
    // These definitions MUST MATCH the definitions in voltdbipc.cpp

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
            // Keep trying until a connection succeeds and this function returns
            // or patience runs out and the process exits.
            int patience = 10;
            while (patience-- > 0) {
                try {
                    System.out.println("Connecting to localhost:" + port);
                    m_socketChannel = SocketChannel.open(
                            new InetSocketAddress("localhost", port));
                    m_socketChannel.configureBlocking(true);
                    m_socket = m_socketChannel.socket();
                    m_socket.setTcpNoDelay(true);
                    System.out.println("Established IPC connection for site.");
                    return;
                } catch (final Exception e) {
                    System.out.println(e.getMessage());
                }
                if (patience == 9 && target == BackendTarget.NATIVE_EE_IPC) {
                    // After an initial attempt to connect fails in
                    // non-valgrind mode, assume that this is a test
                    // launched in "manually launched voltdbipc" mode
                    // where the tester needs to be told the port number.
                    System.out.println(
                            "Ready to connect to voltdbipc process on port " +
                            port +
                            "\nPress Enter after you have started the voltdbipc process " +
                            "to allow the connection to the EE");
                    try {
                        System.in.read();
                        continue;
                    } catch (IOException e1) {
                        break; // Assume tester canceled.
                    }
                }
                System.out.println("Failed to connect to IPC EE on port " +
                        port + " with " + patience + " retries remaining.");
                try {
                    Thread.sleep(10000);
                }
                catch (InterruptedException e1) {}
            }
            System.out.println("Quitting. Failed to initialize IPC EE connection on port " + port);
            System.exit(-1);
        }

        /* Close the socket indicating to the EE it should terminate */
        void close() throws InterruptedException {
            if (m_socketChannel != null) {
                try {
                    m_socketChannel.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                m_socketChannel = null;
                m_socket = null;
            }
        }

        /** blocking write of all m_data to outputstream */
        void sendCommand() throws IOException {
            m_data.flip();
            // write 4 byte length (which includes its own 4 bytes) in big-endian
            // order. this hurts .. but I'm not sure of a better way.

            // From the javadoc for Buffer.clear():
            // "This method does not actually erase the data in the buffer,
            // but it is named as if it did because it will most often be used
            // in situations in which that might as well be the case."
            // This use case assumes that it DOES NOT erase data:
            // m_dataWithLengthPrefix contains the content serialized into its slice,
            // m_data.
            m_dataWithLengthPrefix.clear();
            // m_data.remaining() bytes are all the command bytes that
            // were serialized before the flip and are now ready to be sent.
            final int amt = m_data.remaining();
            m_dataWithLengthPrefix.putInt(4 + amt);
            if (m_dataWithLengthPrefix.capacity() < (4 + amt)) {
                throw new IOException("Catalog data size (" + (4 + amt) +
                                      ") exceeds ExecutionEngineIPC's hard-coded data buffer capacity (" +
                                      m_dataWithLengthPrefix.capacity() + ")");
            }
            m_dataWithLengthPrefix.limit(4 + amt);
            m_dataWithLengthPrefix.rewind();
            while (m_dataWithLengthPrefix.hasRemaining()) {
                m_socketChannel.write(m_dataWithLengthPrefix);
            }
        }

        ByteBuffer readBytes(int size) throws IOException {
            ByteBuffer header = ByteBuffer.allocate(size);
            while (header.hasRemaining()) {
                final int read = m_socketChannel.read(header);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            header.flip();
            return header;
        }

        /**
         * Read a result / response after sending a command to voltdbipc.
         * It starts with a single byte indicating a return code.
         * When the value indicates success,
         * it is up to the caller to optionally follow up, processing additional
         * result detail.
         * When the value indicates an error, throw a suitable EEException.
         * When the value indicates that some specific action is requested,
         * satisfy that request, and, except for the case of a requested crash,
         * read another return code byte and continue.
         * @throws IOException
         */
        void pollForSuccess() throws IOException {
            while (true) {
                int status = m_socket.getInputStream().read();
                switch (status) {
                case ERRORCODE_SUCCESS: {
                    return;
                }

                case ERRORCODE_NEED_DEPENDENCY: {
                    satisfyDependency();
                    break;
                }
                // A plan for a fragment may be unavailable in the EE's cache.
                case ERRORCODE_NEED_PLAN: {
                    satisfyPlan();
                    break;
                }
                // A plan for a fragment may have run for a "long time" and
                // want confirmation before continuing.
                case ERRORCODE_NEED_PROGRESS_UPDATE: {
                    satisfyProgressUpdate();
                    break;
                }
                case ERRORCODE_NEED_BUFFER_EXPORT: {
                    satisfyBufferExport();
                    break;
                }
                case ERRORCODE_NEED_QUEUED_EXPORT_BYTES_STAT: {
                    satisfyQueuedExportBytesStat();
                    break;
                }
                case ERRORCODE_NEED_DECODE_BASE64_AND_DECOMPRESS: {
                    satisfyDecodeBase64AndDecompress();
                    break;
                }

                case ERRORCODE_CRASH_VOLTDB: {
                    satisfyCrashRequest();
                    break;
                }

                default: {
                    throwSerializedException(status);
                    return;
                }
                }
            }
        }

        /**
         * @throws IOException
         */
        private void satisfyPlan() throws IOException {
            long fragmentId = readLong();
            byte[] plan = planForFragmentId(fragmentId);
            m_data.clear();
            m_data.put(plan);
            sendCommand();
        }

        /**
         * @throws IOException
         */
        private void satisfyProgressUpdate() throws IOException {
            int batchIndex = readInt();
            String planNodeName = readShortString();
            String lastAccessedTable = readShortString();
            long lastAccessedTableSize = readLong();
            long tuplesFound = readLong();
            long currMemoryInBytes = readLong();
            long peakMemoryInBytes = readLong();
            long nextStep = fragmentProgressUpdate(batchIndex, planNodeName,
                    lastAccessedTable, lastAccessedTableSize, tuplesFound,
                    currMemoryInBytes, peakMemoryInBytes);
            m_data.clear();
            m_data.putLong(nextStep);
            sendCommand();
        }

        /**
         * @throws IOException
         */
        private void satisfyDecodeBase64AndDecompress() throws IOException {
            String data = readIntSizedString();
            byte[] decodedDecompressedData = Encoder.decodeBase64AndDecompressToBytes(data);
            m_data.clear();
            m_data.put(decodedDecompressedData);
            sendCommand();
        }

        /**
         * @throws IOException
         * @throws EOFException
         * @throws UnsupportedEncodingException
         */
        private void satisfyQueuedExportBytesStat() throws IOException,
                EOFException, UnsupportedEncodingException {
            int partitionId = readInt();
            String signature = readIntSizedString();

            long retval = ExportManager.getQueuedExportBytes(partitionId, signature);
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(retval).flip();

            while (buf.hasRemaining()) {
                m_socketChannel.write(buf);
            }
        }

        /**
         * @throws IOException
         * @throws UnsupportedEncodingException
         */
        private void satisfyBufferExport() throws IOException,
                UnsupportedEncodingException {
            // Message structure:
            // error code - 1 byte already read
            // export generation - 8 bytes
            // partition id - 4 bytes
            // signature length (in bytes) - 4 bytes
            // signature - signature length bytes
            // uso - 8 bytes
            // sync - 1 byte
            // end of generation flag - 1 byte
            // export buffer length - 4 bytes
            // export buffer - export buffer length bytes
            long exportGeneration = readLong();
            int partitionId = readInt();
            String signature = readIntSizedString();
            long uso = readLong();
            boolean sync = readBoolean();
            boolean isEndOfGeneration = readBoolean();
            int length = readInt();
            ByteBuffer exportBytes = (length == 0) ? null : readBytes(length);
            ExportManager.pushExportBuffer(
                    exportGeneration,
                    partitionId,
                    signature,
                    uso,
                    0,
                    exportBytes,
                    sync,
                    isEndOfGeneration);
        }


        private void satisfyCrashRequest() {
            String message = "message lost";
            String filename = "file name lost";
            int lineno = 0;
            String[] traces = new String[0];
            try {
                ByteBuffer messageBuffer = readMessage();
                final int reasonLength = messageBuffer.getInt();
                final byte reasonBytes[] = new byte[reasonLength];
                messageBuffer.get(reasonBytes);
                message = new String(reasonBytes, Constants.UTF8ENCODING);

                filename = ByteBufferUtil.readNonNullSymbolString(messageBuffer);

                lineno = messageBuffer.getInt();

                final int numTraces = messageBuffer.getInt();
                traces = new String[numTraces];

                for (int ii = 0; ii < numTraces; ii++) {
                    traces[ii] = ByteBufferUtil.readNonNullSymbolString(messageBuffer);
                }
            } catch (IOException notImportant) { }
            crashVoltDB(message, traces, filename, lineno);
        }

        /**
         * Retrieve a dependency table and send it via the connection. If
         * no table is available send a response code indicating such.
         * The message is prepended with two lengths. One length is for
         * the network layer and is the size of the whole message not including
         * the length prefix.
         */
        private void satisfyDependency() throws IOException {
            int dependencyId = readInt();
            final byte[] dependencyBytes = nextDependencyAsBytes(dependencyId);
            if (dependencyBytes == null) {
                m_socket.getOutputStream().write(ERRORCODE_ERROR);
                return;
            }
            // 1 for response code + 4 for dependency length prefix + dependencyBytes.length
            final ByteBuffer message = ByteBuffer.allocate(1 + 4 + dependencyBytes.length);

            // write the response code
            message.put((byte)ERRORCODE_SUCCESS);

            // write the dependency's length prefix
            message.putInt(dependencyBytes.length);

            // finally, write dependency table itself
            message.put(dependencyBytes);
            message.flip();
            if (m_socketChannel.write(message) != message.capacity()) {
                throw new IOException("Unable to send dependency table to client. Attempted blocking write of " +
                        message.capacity() + " but not all of it was written");
            }
        }

        /**
         * Read some number of tables from the wire. Assumes that the message is length prefixed.
         * @param tables Output array as well as indicator of exactly how many tables to read off of the wire
         * @throws IOException
         */
        private void readResultTables(final VoltTable tables[]) throws IOException {
            final ByteBuffer resultTablesBuffer = readMessage();
            // check if anything was changed
            final boolean dirty = resultTablesBuffer.get() > 0;
            if (dirty) {
                m_dirty = true;
            }
            for (int ii = 0; ii < tables.length; ii++) {
                final int dependencyCount = resultTablesBuffer.getInt(); // ignore the table count
                assert(dependencyCount == 1); //Expect one dependency generated per plan fragment
                resultTablesBuffer.getInt(); // ignore the dependency ID
                tables[ii] = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(resultTablesBuffer);
            }
        }

        /**
         * Read a long from the wire.
         */
        long readLong() throws IOException
        { return readBytes(8).getLong(); }

        /**
         * Read an int from the wire.
         */
        int readInt() throws IOException
        { return readBytes(4).getInt(); }

        /**
         * Read a short from the wire.
         */
        short readShort() throws IOException
        { return readBytes(2).getShort(); }

        /**
         * Read a boolean from a 0 or 1 byte on the wire.
         */
        boolean readBoolean() throws IOException
        { return readBytes(1).get() == 1 ? true : false; }

        /**
         * Read a short-length-prefixed string from the wire.
         */
        String readShortString() throws IOException
        { return new String(readByteArray(readShort()), Constants.UTF8ENCODING); }

        /**
         * Read an int-length-prefixed string from the wire.
         */
        String readIntSizedString() throws IOException
        { return new String(readIntSizedByteArray(), Constants.UTF8ENCODING); }

        /**
         * Read an int-length-prefixed ByteBuffer from the wire.
         */
        ByteBuffer readMessage() throws IOException {
            int length = readInt();
            return readBytes(length);
        }

        /**
         * Read an int-length-prefixed byte array from the wire.
         */
        byte[] readIntSizedByteArray() throws IOException
        { return readByteArray(readInt()); }

        /**
         * Read a fixed-length byte array from the wire.
         */
        byte[] readByteArray(int size) throws IOException
        {
            ByteBuffer buffer = readBytes(size);
            byte[] resultBytes;
            if (buffer.hasArray()) {
                resultBytes = buffer.array();
            }
            else {
                resultBytes = new byte[size];
                buffer.get(resultBytes);
            }
            return resultBytes;
        }

        void throwSerializedException(final int errorCode) throws IOException {
            final int exceptionLength = readInt();//Length is only between EE and Java.
            if (exceptionLength == 0) {
                throw new EEException(errorCode);
            }
            final ByteBuffer exceptionBuffer = ByteBuffer.allocate(exceptionLength + 4);
            exceptionBuffer.putInt(exceptionLength);
            while (exceptionBuffer.hasRemaining()) {
                int read = m_socketChannel.read(exceptionBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            exceptionBuffer.flip();
            throw SerializableException.deserializeFromBuffer(exceptionBuffer);
        }
    }

    /** Local data */
    private final int m_clusterIndex;
    private final long m_siteId;
    private final int m_partitionId;
    private final int m_hostId;
    private final String m_hostname;
    private final Connection m_connection;
    private final BBContainer m_dataGuard;
    private final ByteBuffer m_dataWithLengthPrefix;
    private ByteBuffer m_data;

    // private int m_counter;

    public ExecutionEngineIPC(
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int hostId,
            final String hostname,
            final int tempTableMemory,
            final BackendTarget target,
            final int port,
            final HashinatorConfig hashinatorConfig,
            final boolean createDrReplicatedStream) {
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
        m_dataGuard = org.voltcore.utils.DBBPool.allocateDirect(1024 * 1024 * 20);
        m_dataWithLengthPrefix = m_dataGuard.b();
        // Reserve the first 4 buffer bytes to hold a length prefix.
        // The prefix will count itself and the final serialized/flipped
        // content of m_data.
        // m_data can be serialized into freely -- up to its limit --
        // without regard to this prefix count.
        // The count is recalculated and serialized just before the buffer is sent.
        m_dataWithLengthPrefix.position(4);
        m_data = m_dataWithLengthPrefix.slice();

        initialize(
                m_clusterIndex,
                m_siteId,
                m_partitionId,
                m_hostId,
                m_hostname,
                1024 * 1024 * tempTableMemory,
                hashinatorConfig,
                createDrReplicatedStream);
    }

    @Override
    public void release() throws EEException, InterruptedException {
        System.out.println("Shutdown IPC connection in progress.");
        m_connection.close();
        System.out.println("Shutdown IPC connection in done.");
        m_dataGuard.discard();
    }

    private static final Object printLockObject = new Object();

    /**
     * @throws RuntimeException
     */
    private void sendCommandForResponse() throws RuntimeException
    {
        try {
            m_connection.sendCommand();
            m_connection.pollForSuccess();
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void putString(ByteBuffer data, String string) {
        assert(string != null);
        byte[] bytes = string.getBytes(Constants.UTF8ENCODING);
        data.putInt(bytes.length);
        data.put(bytes);
    }

    /**
     * the abstract api assumes construction initializes but here initialization
     * is just another command.
     */
    public void initialize(
            final int clusterIndex,
            final long siteId,
            final int partitionId,
            final int hostId,
            final String hostname,
            final long tempTableMemory,
            final HashinatorConfig hashinatorConfig,
            final boolean createDrReplicatedStream)
    {
        synchronized(printLockObject) {
            System.out.println("Initializing an IPC EE " + this + " for hostId " + hostId + " siteId " + siteId + " from thread " + Thread.currentThread().getId());
        }
        m_data.clear();
        m_data.putInt(Commands.Initialize.m_id);
        m_data.putInt(clusterIndex);
        m_data.putLong(siteId);
        m_data.putInt(partitionId);
        m_data.putInt(hostId);
        m_data.putLong(EELoggers.getLogLevels());
        m_data.putLong(tempTableMemory);
        m_data.putInt(createDrReplicatedStream ? 1 : 0);
        putString(m_data, hostname);
        sendCommandForResponse();
        updateHashinator(hashinatorConfig);
    }

    /** write the catalog as a UTF-8 byte string via connection */
    @Override
    protected void loadCatalog(final long timestamp, final byte[] catalogBytes) throws EEException {
        m_data.clear();

        if (m_data.capacity() < catalogBytes.length + 100) {
            m_data = ByteBuffer.allocate(catalogBytes.length + 100);
        }
        m_data.putInt(Commands.LoadCatalog.m_id);
        m_data.putLong(timestamp);
        m_data.put(catalogBytes);
        m_data.put((byte)'\0');
        sendCommandForResponse();
    }

    /** write the diffs as a UTF-8 byte string via connection */
    @Override
    public void updateCatalog(final long timestamp, final String catalogDiffs) throws EEException {
        m_data.clear();
        byte[] catalogBytes = catalogDiffs.getBytes(Constants.UTF8ENCODING);
        if (m_data.capacity() < catalogBytes.length + 100) {
            m_data = ByteBuffer.allocate(catalogBytes.length + 100);
        }
        m_data.putInt(Commands.UpdateCatalog.m_id);
        m_data.putLong(timestamp);
        m_data.put(catalogBytes);
        m_data.put((byte)'\0');
        sendCommandForResponse();
    }

    @Override
    public void tick(final long time, final long lastCommittedSpHandle) {
        m_data.clear();
        m_data.putInt(Commands.Tick.m_id);
        m_data.putLong(time);
        m_data.putLong(lastCommittedSpHandle);
        sendCommandForResponse();
    }

    @Override
    public void quiesce(long lastCommittedSpHandle) {
        m_data.clear();
        m_data.putInt(Commands.Quiesce.m_id);
        m_data.putLong(lastCommittedSpHandle);
        sendCommandForResponse();
    }

    @Override
    protected VoltTable[] coreExecutePlanFragments(
            final int numFragmentIds,
            final long[] planFragmentIds,
            final long[] inputDepIdsIn,
            final Object[] parameterSets,
            final long txnId,
            final long spHandle,
            final long lastCommittedSpHandle,
            final long uniqueId,
            final long undoToken) throws EEException
    {
        // big endian, not direct
        final FastSerializer fser = new FastSerializer();
        try {
            for (int i = 0; i < numFragmentIds; ++i) {
                // pset can be ByteBuffer or ParameterSet instance
                if (parameterSets[i] instanceof ByteBuffer) {
                    fser.write((ByteBuffer) parameterSets[i]);
                }
                else {
                    ParameterSet pset = (ParameterSet) parameterSets[i];
                    ByteBuffer buf = ByteBuffer.allocate(pset.getSerializedSize());
                    pset.flattenToBuffer(buf);
                    buf.flip();
                    fser.write(buf);
                }
            }
        } catch (IOException exception) {
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
        m_data.putInt(Commands.QueryPlanFragments.m_id);
        m_data.putLong(txnId);
        m_data.putLong(spHandle);
        m_data.putLong(lastCommittedSpHandle);
        m_data.putLong(uniqueId);
        m_data.putLong(undoToken);
        m_data.putInt(numFragmentIds);
        for (int i = 0; i < numFragmentIds; ++i) {
            m_data.putLong(planFragmentIds[i]);
        }
        for (int i = 0; i < numFragmentIds; ++i) {
            m_data.putLong(inputDepIds[i]);
        }
        m_data.put(fser.getBuffer());
        fser.discard();
        sendCommandForResponse();
        final VoltTable resultTables[] = new VoltTable[numFragmentIds];
        for (int ii = 0; ii < numFragmentIds; ii++) {
            resultTables[ii] = PrivateVoltTableFactory.createUninitializedVoltTable();
        }
        try {
            m_connection.readResultTables(resultTables);
            return resultTables;
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
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
    public void toggleProfiler(final int toggle) { return; }


    @Override
    public byte[] loadTable(final int tableId, final VoltTable table, final long txnId,
            final long spHandle, final long lastCommittedSpHandle, final long uniqueId,
            boolean returnUniqueViolations, boolean shouldDRStream, long undoToken)
    throws EEException
    {
        if (returnUniqueViolations) {
            throw new UnsupportedOperationException("Haven't added IPC support for returning unique violations");
        }
        m_data.clear();
        m_data.putInt(Commands.LoadTable.m_id);
        m_data.putInt(tableId);
        m_data.putLong(txnId);
        m_data.putLong(spHandle);
        m_data.putLong(lastCommittedSpHandle);
        m_data.putLong(uniqueId);
        m_data.putLong(undoToken);
        m_data.putInt(returnUniqueViolations ? 1 : 0);
        m_data.putInt(shouldDRStream ? 1 : 0);

        final ByteBuffer tableBytes = PrivateVoltTableFactory.getTableDataReference(table);
        if (m_data.remaining() < tableBytes.remaining()) {
            m_data.flip();
            final ByteBuffer newBuffer = ByteBuffer.allocate(m_data.remaining()
                    + tableBytes.remaining());
            newBuffer.put(m_data);
            m_data = newBuffer;
        }
        m_data.put(tableBytes);
        sendCommandForResponse();
        byte[] response = null;
        try {
            response = m_connection.readIntSizedByteArray();
            if (response.length == 0) {
                return null;
            }
            return response;
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public VoltTable[] getStats(final StatsSelector selector, final int[] locators,
            final boolean interval, final Long now)
    {
        m_data.clear();
        m_data.putInt(Commands.GetStats.m_id);
        m_data.putInt(selector.ordinal());
        m_data.put(interval ? (byte)1 : (byte)0);
        m_data.putLong(now);
        m_data.putInt(locators.length);
        for (final int locator : locators) {
            m_data.putInt(locator);
        }
        sendCommandForResponse();
        try {
            final ByteBuffer messageBuffer = m_connection.readMessage();
            if (messageBuffer.remaining() == 0) {
                return new VoltTable[]{};
            }
            final VoltTable results[] = new VoltTable[1];
            results[0] = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(messageBuffer);
            return results;
        } catch (IOException e) {
            System.out.println("IPC exception reading statistics table: " + e.getMessage());
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public void releaseUndoToken(final long undoToken)
    {
        m_data.clear();
        m_data.putInt(Commands.releaseUndoToken.m_id);
        m_data.putLong(undoToken);
        sendCommandForResponse();
    }

    @Override
    public void undoUndoToken(final long undoToken)
    {
        m_data.clear();
        m_data.putInt(Commands.undoUndoToken.m_id);
        m_data.putLong(undoToken);
        sendCommandForResponse();
    }

    @Override
    public void setLogLevels(final long logLevels) throws EEException
    {
        m_data.clear();
        m_data.putInt(Commands.SetLogLevels.m_id);
        m_data.putLong(logLevels);
        sendCommandForResponse();
    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType streamType,
            long undoQuantumToken, byte[] predicates)
    {
        m_data.clear();
        m_data.putInt(Commands.ActivateTableStream.m_id);
        m_data.putInt(tableId);
        m_data.putInt(streamType.ordinal());
        m_data.putLong(undoQuantumToken);
        m_data.put(predicates);
        sendCommandForResponse();
        try {
            return m_connection.readBoolean();
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public Pair<Long, int[]> tableStreamSerializeMore(int tableId,
            TableStreamType streamType, List<BBContainer> outputBuffers)
    {
        m_data.clear();
        m_data.putInt(Commands.TableStreamSerializeMore.m_id);
        m_data.putInt(tableId);
        m_data.putInt(streamType.ordinal());
        m_data.put(SnapshotUtil.OutputBuffersToBytes(outputBuffers));
        sendCommandForResponse();
        try {
            // Get the count.
            final int count = m_connection.readInt();
            assert count == outputBuffers.size();

            // Get the remaining tuple count.
            final long remaining = m_connection.readLong();

            final int[] serialized = new int[count];
            for (int i = 0; i < count; i++) {
                serialized[i] = m_connection.readInt();

                ByteBuffer view = outputBuffers.get(i).b().duplicate();
                view.limit(view.position() + serialized[i]);
                while (view.hasRemaining()) {
                    m_connection.m_socketChannel.read(view);
                }
            }
            return Pair.of(remaining, serialized);
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void exportAction(boolean syncAction,
            long ackOffset, long seqNo, int partitionId, String tableSignature)
    {
        m_data.clear();
        m_data.putInt(Commands.ExportAction.m_id);
        m_data.putInt(syncAction ? 1 : 0);
        m_data.putLong(ackOffset);
        m_data.putLong(seqNo);
        putString(m_data, tableSignature);
        sendCommandForResponse();
        try {
            long result_offset = m_connection.readLong();
            if (result_offset < 0) {
                System.out.println("exportAction failed!  syncAction: " + syncAction + ", ackTxnId: " +
                    ackOffset + ", seqNo: " + seqNo + ", partitionId: " + partitionId +
                    ", tableSignature: " + tableSignature);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long[] getUSOForExportTable(String tableSignature)
    {
        try {
            m_data.clear();
            m_data.putInt(Commands.GetUSOs.m_id);
            putString(m_data, tableSignature);
            sendCommandForResponse();
            long[] retval = new long[2];
            retval[0] = m_connection.readLong();
            retval[1] = m_connection.readLong();
            return retval;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void processRecoveryMessage(ByteBuffer buffer, long pointer)
    {
        m_data.clear();
        m_data.putInt(Commands.RecoveryMessage.m_id);
        m_data.putInt(buffer.remaining());
        m_data.put(buffer);
        sendCommandForResponse();
    }

    @Override
    public long tableHashCode(int tableId)
    {
        m_data.clear();
        m_data.putInt(Commands.TableHashCode.m_id);
        m_data.putInt(tableId);
        sendCommandForResponse();
        try {
            return m_connection.readLong();
        } catch (IOException e) {
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
        m_data.putInt(config.type.typeId());
        m_data.putInt(config.configBytes.length);
        m_data.put(config.configBytes);
        try {
            parameterSet.flattenToBuffer(m_data);
            sendCommandForResponse();
            return m_connection.readInt();
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateHashinator(HashinatorConfig config)
    {
        m_data.clear();
        m_data.putInt(Commands.updateHashinator.m_id);
        m_data.putInt(config.type.typeId());
        m_data.putInt(config.configBytes.length);
        m_data.put(config.configBytes);
        sendCommandForResponse();
    }

    @Override
    public void applyBinaryLog(ByteBuffer log, long txnId, long spHandle, long lastCommittedSpHandle, long uniqueId,
                               long undoToken) throws EEException
    {
        m_data.clear();
        m_data.putInt(Commands.applyBinaryLog.m_id);
        m_data.putLong(txnId);
        m_data.putLong(spHandle);
        m_data.putLong(lastCommittedSpHandle);
        m_data.putLong(uniqueId);
        m_data.putLong(undoToken);
        m_data.put(log.array());
        sendCommandForResponse();
    }

    @Override
    public long getThreadLocalPoolAllocations() {
        m_data.clear();
        m_data.putInt(Commands.GetPoolAllocations.m_id);
        sendCommandForResponse();
        try {
            return m_connection.readLong();
        } catch (IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] executeTask(TaskType taskType, ByteBuffer task) {
        m_data.clear();
        m_data.putInt(Commands.executeTask.m_id);
        m_data.putLong(taskType.taskId);
        m_data.put(task.array());
        sendCommandForResponse();
        try {
            return m_connection.readIntSizedByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer getParamBufferForExecuteTask(int requiredCapacity)
    {
        return ByteBuffer.allocate(requiredCapacity);
    }

}
