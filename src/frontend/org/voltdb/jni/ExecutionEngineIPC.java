/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.BackendTarget;
import org.voltdb.FragmentPlanSource;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltTable;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.export.ExportManager;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

import com.google.common.base.Charsets;

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
        Initialize(0),
        LoadCatalog(2),
        ToggleProfiler(3),
        Tick(4),
        GetStats(5),
        QueryPlanFragments(6),
        PlanFragment(7),
        LoadTable(9),
        releaseUndoToken(10),
        undoUndoToken(11),
        CustomPlanFragment(12),
        SetLogLevels(13),
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
        updateHashinator(27);
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
                    System.out.printf("Ready to connect to voltdbipc process on port %d\n", port);
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
         * Retrieve value from Java for stats
         */
        static final int kErrorCode_getQueuedExportBytes = 105;

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
                    continue;
                }
                if (status == kErrorCode_CrashVoltDB) {
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
                if (status == kErrorCode_pushExportBuffer) {
                    ByteBuffer header = ByteBuffer.allocate(30);
                    while (header.hasRemaining()) {
                        final int read = m_socket.getChannel().read(header);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    header.flip();

                    long exportGeneration = header.getLong();
                    int partitionId = header.getInt();
                    int signatureLength = header.getInt();
                    byte signatureBytes[] = new byte[signatureLength];
                    header.get(signatureBytes);
                    String signature = new String(signatureBytes, "UTF-8");
                    long uso = header.getLong();
                    boolean sync = header.get() == 1 ? true : false;
                    boolean isEndOfGeneration = header.get() == 1 ? true : false;
                    int length = header.getInt();
                    ByteBuffer exportBuffer = ByteBuffer.allocateDirect(length);
                    while (exportBuffer.hasRemaining()) {
                        final int read = m_socket.getChannel().read(exportBuffer);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    exportBuffer.flip();
                    ExportManager.pushExportBuffer(
                            exportGeneration,
                            partitionId,
                            signature,
                            uso,
                            0,
                            length == 0 ? null : exportBuffer,
                                    sync,
                                    isEndOfGeneration);
                    continue;
                }
                if (status == kErrorCode_getQueuedExportBytes) {
                    ByteBuffer header = ByteBuffer.allocate(12);
                    while (header.hasRemaining()) {
                        final int read = m_socket.getChannel().read(header);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    header.flip();

                    int partitionId = header.getInt();
                    int signatureLength = header.getInt();
                    byte signatureBytes[] = new byte[signatureLength];
                    header.get(signatureBytes);
                    String signature = new String(signatureBytes, "UTF-8");

                    long retval = ExportManager.getQueuedExportBytes(partitionId, signature);
                    ByteBuffer buf = ByteBuffer.allocate(8);
                    buf.putLong(retval).flip();

                    while (buf.hasRemaining()) {
                        m_socketChannel.write(buf);
                    }
                    continue;
                }

                try {
                    checkErrorCode(status);
                    break;
                }
                catch (final RuntimeException e) {
                    if (e instanceof SerializableException) {
                        throw e;
                    } else {
                        throw (IOException)e.getCause();
                    }
                }
            }

            return status;
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

            final FastDeserializer ds = new FastDeserializer(resultTablesBuffer);
            // check if anything was changed
            final boolean dirty = ds.readBoolean();
            if (dirty)
                m_dirty = true;

            for (int ii = 0; ii < tables.length; ii++) {
                final int dependencyCount = ds.readInt(); // ignore the table count
                assert(dependencyCount == 1); //Expect one dependency generated per plan fragment
                ds.readInt(); // ignore the dependency ID
                tables[ii] = (VoltTable) ds.readObject(tables[ii], null);
            }
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
    private final BBContainer m_dataNetworkOrigin;
    private final ByteBuffer m_dataNetwork;
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
            final HashinatorType type,
            final byte config[],
            FragmentPlanSource planSource) {
        super(siteId, partitionId, planSource);

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
        m_dataNetwork = m_dataNetworkOrigin.b;
        m_dataNetwork.position(4);
        m_data = m_dataNetwork.slice();

        initialize(
                m_clusterIndex,
                m_siteId,
                m_partitionId,
                m_hostId,
                m_hostname,
                1024 * 1024 * tempTableMemory,
                type,
                config);
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

    @Override
    public void release() throws EEException, InterruptedException {
        System.out.println("Shutdown IPC connection in progress.");
        m_connection.close();
        System.out.println("Shutdown IPC connection in done.");
        m_dataNetworkOrigin.discard();
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
            final int hostId,
            final String hostname,
            final long tempTableMemory,
            final HashinatorType hashinatorType,
            final byte hashinatorConfig[])
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
        m_data.putInt(hostId);
        m_data.putLong(EELoggers.getLogLevels());
        m_data.putLong(tempTableMemory);
        m_data.putInt(hashinatorType.typeId());
        m_data.putInt(hashinatorConfig.length);
        m_data.putInt((short)hostname.length());
        m_data.put(hashinatorConfig);
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
    }

    /** write the catalog as a UTF-8 byte string via connection */
    @Override
    public void loadCatalog(final long timestamp, final String serializedCatalog) throws EEException {
        int result = ExecutionEngine.ERRORCODE_ERROR;
        m_data.clear();

        try {
            final byte catalogBytes[] = serializedCatalog.getBytes("UTF-8");
            if (m_data.capacity() < catalogBytes.length + 100) {
                m_data = ByteBuffer.allocate(catalogBytes.length + 100);
            }
            m_data.putInt(Commands.LoadCatalog.m_id);
            m_data.putLong(timestamp);
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

    /** write the diffs as a UTF-8 byte string via connection */
    @Override
    public void updateCatalog(final long timestamp, final String catalogDiffs) throws EEException {
        int result = ExecutionEngine.ERRORCODE_ERROR;
        m_data.clear();

        try {
            final byte catalogBytes[] = catalogDiffs.getBytes("UTF-8");
            if (m_data.capacity() < catalogBytes.length + 100) {
                m_data = ByteBuffer.allocate(catalogBytes.length + 100);
            }
            m_data.putInt(Commands.UpdateCatalog.m_id);
            m_data.putLong(timestamp);
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
            final ParameterSet[] parameterSets,
            final long spHandle,
            final long lastCommittedSpHandle,
            final long uniqueId,
            final long undoToken)
    {
        // big endian, not direct
        final FastSerializer fser = new FastSerializer();
        try {
            for (int i = 0; i < numFragmentIds; ++i) {
                parameterSets[i].writeExternal(fser);
            }
        } catch (final IOException exception) {
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
        m_data.putInt(cmd.m_id);
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

        try {
            m_data.flip();
            m_connection.write();
        } catch (final Exception e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    protected VoltTable[] coreExecutePlanFragments(
            final int numFragmentIds,
            final long[] planFragmentIds,
            final long[] inputDepIds,
            final ParameterSet[] parameterSets,
            final long spHandle,
            final long lastCommittedSpHandle,
            final long uniqueId,
            final long undoToken) throws EEException {
        sendPlanFragmentsInvocation(Commands.QueryPlanFragments,
                numFragmentIds, planFragmentIds, inputDepIds, parameterSets,
                spHandle, lastCommittedSpHandle, uniqueId, undoToken);
        int result = ExecutionEngine.ERRORCODE_ERROR;

        while (true) {
            try {
                result = m_connection.readStatusByte();

                if (result == ExecutionEngine.ERRORCODE_NEED_PLAN) {
                    long fragmentId = m_connection.readLong();
                    byte[] plan = planForFragmentId(fragmentId);
                    m_data.clear();
                    m_data.put(plan);
                    m_data.flip();
                    m_connection.write();
                }
                else if (result == ExecutionEngine.ERRORCODE_SUCCESS) {
                    final VoltTable resultTables[] = new VoltTable[numFragmentIds];
                    for (int ii = 0; ii < numFragmentIds; ii++) {
                        resultTables[ii] = PrivateVoltTableFactory.createUninitializedVoltTable();
                    }
                    try {
                        m_connection.readResultTables(resultTables);
                    } catch (final IOException e) {
                        throw new EEException(
                                ExecutionEngine.ERRORCODE_WRONG_SERIALIZED_BYTES);
                    }
                    return resultTables;
                }
                else {
                    // failure
                    return null;
                }
            } catch (final IOException e) {
                System.out.println("Exception: " + e.getMessage());
                throw new RuntimeException(e);
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
        return;
    }


    @Override
    public void loadTable(final int tableId, final VoltTable table, final long spHandle,
            final long lastCommittedSpHandle)
    throws EEException
    {
        m_data.clear();
        m_data.putInt(Commands.LoadTable.m_id);
        m_data.putInt(tableId);
        m_data.putLong(spHandle);
        m_data.putLong(lastCommittedSpHandle);

        final ByteBuffer tableBytes = table.getTableDataReference();
        if (m_data.remaining() < tableBytes.remaining()) {
            m_data.flip();
            final ByteBuffer newBuffer = ByteBuffer.allocate(m_data.remaining()
                    + tableBytes.remaining());
            newBuffer.rewind();
            //newBuffer.order(ByteOrder.LITTLE_ENDIAN);
            newBuffer.put(m_data);
            m_data = newBuffer;
        }
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
    }

    @Override
    public VoltTable[] getStats(
            final SysProcSelector selector,
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
                final ByteBuffer messageLengthBuffer = ByteBuffer.allocate(4);
                while (messageLengthBuffer.hasRemaining()) {
                    int read = m_connection.m_socketChannel.read(messageLengthBuffer);
                    if (read == -1) {
                        throw new EOFException("End of file reading statistics(1)");
                    }
                }
                messageLengthBuffer.rewind();
                final ByteBuffer messageBuffer = ByteBuffer.allocate(messageLengthBuffer.getInt());
                while (messageBuffer.hasRemaining()) {
                    int read = m_connection.m_socketChannel.read(messageBuffer);
                    if (read == -1) {
                        throw new EOFException("End of file reading statistics(2)");
                    }
                }
                messageBuffer.rewind();

                final FastDeserializer fds = new FastDeserializer(messageBuffer);
                final VoltTable results[] = new VoltTable[1];
                final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
                results[0] = (VoltTable)fds.readObject(resultTable, this);
                return results;
            }
        } catch (final IOException e) {
            System.out.println("IPC exception reading statistics table: " + e.getMessage());

        }
        return null;
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        m_data.clear();
        m_data.putInt(Commands.releaseUndoToken.m_id);
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
    public boolean activateTableStream(int tableId, TableStreamType streamType) {
        m_data.clear();
        m_data.putInt(Commands.ActivateTableStream.m_id);
        m_data.putInt(tableId);
        m_data.putInt(streamType.ordinal());
        m_data.putInt(0);       // Predicate count

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
    public int tableStreamSerializeMore(BBContainer c, int tableId, TableStreamType streamType) {
        int bytesReturned = -1;
        ByteBuffer view = c.b.duplicate();
        try {
            m_data.clear();
            m_data.putInt(Commands.TableStreamSerializeMore.m_id);
            m_data.putInt(tableId);
            m_data.putInt(streamType.ordinal());
            m_data.putInt(1);                   // Number of buffers
            m_data.putInt(c.b.remaining());     // Byte limit

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

            /*
             * Error or no more tuple data for this table.
             */
            if (count == -1 || count == 0) {
                return count;
            }

            // Get the remaining tuple count.
            ByteBuffer remainingBuffer = ByteBuffer.allocate(8);
            while (remainingBuffer.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(remainingBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            remainingBuffer.flip();
            //TODO: Do something useful with the remaining count.
            /*final long remaining = */ remainingBuffer.getLong();

            // Get the first length.
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            while (lengthBuffer.hasRemaining()) {
                int read = m_connection.m_socketChannel.read(lengthBuffer);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            lengthBuffer.flip();
            final int length = lengthBuffer.getInt();

            bytesReturned = length;
            view.limit(view.position() + length);
            while (view.hasRemaining()) {
                m_connection.m_socketChannel.read(view);
            }
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }

        return bytesReturned;
    }

    @Override
    public ExportProtoMessage exportAction(boolean syncAction,
            long ackOffset, long seqNo, int partitionId, String mTableSignature) {
        try {
            m_data.clear();
            m_data.putInt(Commands.ExportAction.m_id);
            m_data.putInt(syncAction ? 1 : 0);
            m_data.putLong(ackOffset);
            m_data.putLong(seqNo);
            if (mTableSignature == null) {
                m_data.putInt(-1);
            } else {
                m_data.putInt(mTableSignature.getBytes("UTF-8").length);
                m_data.put(mTableSignature.getBytes("UTF-8"));
            }
            m_data.flip();
            m_connection.write();

            ByteBuffer results = ByteBuffer.allocate(8);
            while (results.remaining() > 0)
                m_connection.m_socketChannel.read(results);
            results.flip();
            long result_offset = results.getLong();
            if (result_offset < 0) {
                ExportProtoMessage reply = null;
                reply = new ExportProtoMessage( 0, partitionId, mTableSignature);
                reply.error();
                return reply;
            }
            else {
                return null;
            }

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public long[] getUSOForExportTable(String tableSignature) {
        long[] retval = null;
        try {
            m_data.clear();
            m_data.putInt(Commands.GetUSOs.m_id);
            if (tableSignature == null) {
                m_data.putInt(-1);
            } else {
                m_data.putInt(tableSignature.getBytes("UTF-8").length);
                m_data.put(tableSignature.getBytes("UTF-8"));
            }
            m_data.flip();
            m_connection.write();

            ByteBuffer results = ByteBuffer.allocate(16);
            while (results.remaining() > 0)
                m_connection.m_socketChannel.read(results);
            results.flip();

            retval = new long[2];
            retval[0] = results.getLong();
            retval[1] = results.getLong();

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    @Override
    public void processRecoveryMessage( ByteBuffer buffer, long pointer) {
        try {
            m_data.clear();
            m_data.putInt(Commands.RecoveryMessage.m_id);
            m_data.putInt(buffer.remaining());
            m_data.put(buffer);

            m_data.flip();
            m_connection.write();

            m_connection.readStatusByte();
        } catch (final IOException e) {
            System.out.println("Exception: " + e.getMessage());
            throw new RuntimeException(e);
        }
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
    public int hashinate(Object value, HashinatorType type, byte config[])
    {
        ParameterSet parameterSet = new ParameterSet();
        parameterSet.setParameters(value);

        final FastSerializer fser = new FastSerializer();
        try {
            parameterSet.writeExternal(fser);
        } catch (final IOException exception) {
            throw new RuntimeException(exception);
        }

        m_data.clear();
        m_data.putInt(Commands.Hashinate.m_id);
        m_data.putInt(type.typeId());
        m_data.putInt(config.length);
        m_data.put(config);
        m_data.put(fser.getBuffer());
        try {
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
    public void updateHashinator(HashinatorType type, byte[] config)
    {
        m_data.clear();
        m_data.putInt(Commands.updateHashinator.m_id);
        m_data.putInt(type.typeId());
        m_data.putInt(config.length);
        m_data.put(config);
        try {
            m_data.flip();
            m_connection.write();
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
}
