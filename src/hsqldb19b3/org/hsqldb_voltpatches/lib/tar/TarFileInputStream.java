/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib.tar;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * Note that this class <b>is not</b> a java.io.FileInputStream,
 * because our goal is to greatly restrict the public methods of
 * FileInputStream, yet we must use public methods of the underlying
 * FileInputStream internally.  Can't accomplish these goals in Java if we
 * subclass.
 * <P>
 * This class is ignorant about Tar header fields, attributes and such.
 * It is concerned with reading and writing blocks of data in conformance with
 * Tar formatting, in a way convenient to those who want to get the header and
 * data blocks.
 * <P>
 * Asymmetric to the Tar file writing side, the bufferBlocks setting here is
 * used only for to adjust read buffer size (for file data reads), so the user
 * can compromise between available memory and performance.  Small buffer sizes
 * will always work, but will incur more reads; on the other hand, buffer sizes
 * larger than the largest component file is just a waste of memory.
 * <P/>
 * We assume the responsibility to manage the setting because the decision
 * should be based on available RAM more than anything else (therefore, we can't
 * set a good value automatically).
 * <P/>
 * As alluded to above, headers are read in separate reads, regardless of the
 * readBufferBlocks setting.  readBufferBlocks is used for reading
 * <I>file data</I>.
 * <P/>
 * I have purposefully not implemented skip(), because, though I haven't tested
 * it, I believe our readBlock() and readBlocks() methods are at least as fast,
 * since we use the larges read buffer within limits the user has set.
 */
public class TarFileInputStream {

    /* Would love to use a RandomAccessFile, but RandomAccessFiles do not play
     * nicely with InputStreams or filters, and it just would not work with
     * compressed input. */
    protected long bytesRead = 0;

    // Pronounced as past tense of "to read", not the other forms of "read".
    // I.e., the homonym of "red".
    private InputStream readStream;

    /* This is not a "Reader", but the byte "Stream" that we read() from. */
    protected byte[] readBuffer;
    protected int    readBufferBlocks;
    protected int    compressionType;

    /**
     * Convenience wrapper to use default readBufferBlocks and compressionType.
     *
     * @see #TarFileInputStream(File, int, int)
     */
    public TarFileInputStream(File sourceFile) throws IOException {
        this(sourceFile, TarFileOutputStream.Compression.DEFAULT_COMPRESSION);
    }

    /**
     * Convenience wrapper to use default readBufferBlocks.
     *
     * @see #TarFileInputStream(File, int, int)
     */
    public TarFileInputStream(File sourceFile,
                              int compressionType) throws IOException {
        this(sourceFile, compressionType,
             TarFileOutputStream.Compression.DEFAULT_BLOCKS_PER_RECORD);
    }

    public int getReadBufferBlocks() {
        return readBufferBlocks;
    }

    /**
     * This class does no validation or enforcement of file naming conventions.
     * If desired, the caller should enforce extensions like "tar" and
     * "tar.gz" (and that they match the specified compression type).
     * <P/>
     * This object will automatically release its I/O resources when you get
     * false back from a readNextHeaderBlock() call.
     * If you abort before then, you must call the close() method like for a
     * normal InputStream.
     *
     * @see #close()
     * @see #readNextHeaderBlock()
     */
    public TarFileInputStream(File sourceFile, int compressionType,
                              int readBufferBlocks) throws IOException {

        if (!sourceFile.isFile()) {
            throw new FileNotFoundException(sourceFile.getAbsolutePath());
        }

        if (!sourceFile.canRead()) {
            throw new IOException(RB.singleton.getString(RB.READ_DENIED,
                    sourceFile.getAbsolutePath()));
        }

        this.readBufferBlocks = readBufferBlocks;
        this.compressionType  = compressionType;
        readBuffer            = new byte[readBufferBlocks * 512];

        switch (compressionType) {

            case TarFileOutputStream.Compression.NO_COMPRESSION :
                readStream = new FileInputStream(sourceFile);
                break;

            case TarFileOutputStream.Compression.GZIP_COMPRESSION :
                readStream =
                    new GZIPInputStream(new FileInputStream(sourceFile),
                                        readBuffer.length);
                break;

            default :
                throw new IllegalArgumentException(
                    RB.singleton.getString(
                        RB.COMPRESSION_UNKNOWN, compressionType));
        }
    }

    /**
     * readBlocks(int) is the method that USERS of this class should use to
     * read file data from the tar file.
     * This method reads from the tar file and writes to the readBuffer array.
     * <P>
     * This class and subclasses should read from the underlying readStream
     * <b>ONLY WITH THIS METHOD</b>.
     * That way we can be confident that bytesRead will always be accurate.
     * <P>
     * This method is different from a typical Java byte array read command
     * in that when reading tar files <OL>
     *   <LI>we always know ahead-of-time how many bytes we should read, and
     *   <LI>we always want to read quantities of bytes in multiples of 512.
     * </OL>
     *
     * @param blocks  How many 512 blocks to read.
     * @throws IOException for an I/O error on the underlying InputStream
     * @throws TarMalformatException if no I/O error occurred, but we failed to
     *                               read the exact number of bytes requested.
     */
    public void readBlocks(int blocks)
    throws IOException, TarMalformatException {

        /* int for blocks should support sizes up to about 1T, according to
         * my off-the-cuff calculations */
        if (compressionType
                != TarFileOutputStream.Compression.NO_COMPRESSION) {
            readCompressedBlocks(blocks);

            return;
        }

        int i = readStream.read(readBuffer, 0, blocks * 512);

        bytesRead += i;

        if (i != blocks * 512) {
            throw new TarMalformatException(
                RB.singleton.getString(RB.INSUFFICIENT_READ, blocks * 512, i));
        }
    }

    /**
     * Work-around for the problem that compressed InputReaders don't fill
     * the read buffer before returning.
     *
     * Has visibility 'protected' so that subclasses may override with
     * different algorithms, or use different algorithms for different
     * compression stream.
     */
    protected void readCompressedBlocks(int blocks) throws IOException {

        int bytesSoFar    = 0;
        int requiredBytes = 512 * blocks;

        // This method works with individual bytes!
        int i;

        while (bytesSoFar < requiredBytes) {
            i = readStream.read(readBuffer, bytesSoFar,
                                requiredBytes - bytesSoFar);

            if (i < 0) {
                // A VoltDB extension to disable tagging eof as an error.
                return;
                /* disable 3 lines ...
                throw new EOFException(
                    RB.singleton.getString(
                        RB.DECOMPRESS_RANOUT, bytesSoFar, requiredBytes));
                ... disabled 3 lines */
                // End of VoltDB extension
            }

            bytesRead  += i;
            bytesSoFar += i;
        }
    }

    /**
     * readBlock() and readNextHeaderBlock are the methods that USERS of this
     * class should use to read header blocks from the tar file.
     * <P>
     * readBlock() should be used when you know that the current block should
     * contain what you want.
     * E.g. you know that the very first block of a tar file should contain
     * a Tar Entry header block.
     *
     * @see #readNextHeaderBlock
     */
    public void readBlock() throws IOException, TarMalformatException {
        readBlocks(1);
    }

    /**
     * readBlock() and readNextHeaderBlock are the methods that USERS of this
     * class should use to read header blocks from the tar file.
     * <P>
     * readNextHeaderBlock continues working through the Tar File from the
     * current point until it finds a block with a non-0 first byte.
     *
     * @return  True if a header block was read and place at beginning of the
     *          readBuffer array.  False if EOF was encountered without finding
     *          any blocks with first byte != 0.  If false is returned, we have
     *          automatically closed the this TarFileInputStream too.
     * @see #readBlock
     */
    public boolean readNextHeaderBlock()
    throws IOException, TarMalformatException {

        // We read a-byte-at-a-time because there should only be 2 empty blocks
        // between each Tar Entry.
        try {
            while (readStream.available() > 0) {
                readBlock();

                if (readBuffer[0] != 0) {
                    return true;
                }
            }
        } catch (EOFException ee) {
            /* This is a work-around.
             * Sun Java's inputStream.available() works like crap.
             * Reach this point when performing a read of a GZip stream when
             * .available == 1, which according to API Spec, should not happen.
             * We treat this condition exactly as if readStream.available is 0,
             * which it should be.
             */
        }

        close();

        return false;
    }

    /**
     * Implements java.io.Closeable.
     *
     * @see java.io.Closeable
     */
    public void close() throws IOException {
        readStream.close();
    }
}
