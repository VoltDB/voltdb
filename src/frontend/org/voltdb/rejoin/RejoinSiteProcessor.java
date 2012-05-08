package org.voltdb.rejoin;

import java.nio.ByteBuffer;
import java.util.List;

import org.voltcore.utils.Pair;

public interface RejoinSiteProcessor {

    /**
     * Initialize the snapshot sink, bind to a socket and wait for incoming
     * connection.
     *
     * @return A list of local addresses and port that remote node can connect
     *         to.
     */
    public abstract Pair<List<byte[]>, Integer> initialize();

    /**
     * Whether or not all snapshot blocks are polled
     *
     * @return true if no more blocks to come, false otherwise
     */
    public abstract boolean isEOF();

    /**
     * Closes all connections
     */
    public abstract void close();

    /**
     * Poll the next block to be sent to EE.
     *
     * @return The next block along with its table ID, or null if there's none.
     */
    public abstract Pair<Integer, ByteBuffer> poll();

}