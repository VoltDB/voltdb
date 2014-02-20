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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Note that this class <b>is not</b> a java.io.FileOutputStream,
 * because our goal is to greatly restrict the public methods of
 * FileOutputStream, yet we must use public methods of the underlying
 * FileOutputStream internally.  Can't accomplish these goals in Java if we
 * subclass.
 * <P>
 * This class is ignorant about Tar header fields, attributes and such.
 * It is concerned with reading and writing blocks of data in conformance with
 * Tar formatting, in a way convenient to those who want to write the header
 * and data blocks.
 * <P>
 * Users write file data by means of populating the provided, public byte array,
 * then calling the single write(int) method to write a portion of that array.
 * This design purposefully goes with efficiency, simplicity, and performance
 * over Java convention, which would not use public fields.
 * <P>
 * At this time, we do not support appending.  That would greatly decrease the
 * generality and simplicity of the our design, since appending is trivial
 * without compression and very difficult with compression.
 * <P>
 * Users must finish tar file creation by using the finish() method.
 * Just like a normal OutputStream, if processing is aborted for any reason,
 * the close() method must be used to free up system resources.
 * <P>
 * <B>SECURITY NOTE</B>
 * Due to pitiful lack of support for file security in Java before version 1.6,
 * this class will only explicitly set permissions if it is compiled for Java
 * 1.6.  If it was not, and if your tar entries contain private data in files
 * with 0400 or similar, be aware that that file can be pretty much be
 * extracted by anybody with access to the tar file.
 *
 * @see #finish
 * @see #close
 */
public class TarFileOutputStream {

    public interface Compression {

        public static final int NO_COMPRESSION            = 0;
        public static final int GZIP_COMPRESSION          = 1;
        public static final int DEFAULT_COMPRESSION       = NO_COMPRESSION;
        public static final int DEFAULT_BLOCKS_PER_RECORD = 20;
    }

    public static boolean debug = Boolean.getBoolean("DEBUG");
    protected int         blocksPerRecord;
    protected long        bytesWritten = 0;
    private OutputStream  writeStream;
    private File          targetFile;
    private File          writeFile;

    /* This is not a "Writer", but the byte "Stream" that we write() to. */
    public byte[] writeBuffer;
    /* We purposefully provide no public getter or setter for writeBuffer.
     * No getter because the whole point of this class is that the byte
     * array is a direct, optimally efficient byte array.  No setter because
     * the inside implementation of this class is intimately dependent upon
     * the nature of the write buffer. */
    public static final byte[] ZERO_BLOCK = new byte[512];

    /**
     * Convenience wrapper to use default blocksPerRecord and compressionType.
     *
     * @see #TarFileOutputStream(File, int, int)
     */
    public TarFileOutputStream(File targetFile) throws IOException {
        this(targetFile, Compression.DEFAULT_COMPRESSION);
    }

    /**
     * Convenience wrapper to use default blocksPerRecord.
     *
     * @see #TarFileOutputStream(File, int, int)
     */
    public TarFileOutputStream(File targetFile,
                               int compressionType) throws IOException {
        this(targetFile, compressionType,
             TarFileOutputStream.Compression.DEFAULT_BLOCKS_PER_RECORD);
    }

    /**
     * This class does no validation or enforcement of file naming conventions.
     * If desired, the caller should enforce extensions like "tar" and
     * "tar.gz" (and that they match the specified compression type).
     * <P/>
     * It also overwrites files without warning (just like FileOutputStream).
     */
    public TarFileOutputStream(File targetFile, int compressionType,
                               int blocksPerRecord) throws IOException {

        this.blocksPerRecord = blocksPerRecord;
        this.targetFile      = targetFile;
        writeFile = new File(targetFile.getParentFile(),
                             targetFile.getName() + "-partial");

        if (this.writeFile.exists()) {
            throw new IOException(RB.singleton.getString(RB.MOVE_WORK_FILE,
                    writeFile.getAbsolutePath()));
        }

        if (targetFile.exists() && !targetFile.canWrite()) {
            throw new IOException(RB.singleton.getString(RB.CANT_OVERWRITE,
                    targetFile.getAbsolutePath()));
        }

        File parentDir = targetFile.getAbsoluteFile().getParentFile();

        if (parentDir.exists() && parentDir.isDirectory()) {
            if (!parentDir.canWrite()) {
                throw new IOException(RB.singleton.getString(RB.CANT_WRITE_DIR,
                        parentDir.getAbsolutePath()));
            }
        } else {
            throw new IOException(RB.singleton.getString(RB.NO_PARENT_DIR,
                    parentDir.getAbsolutePath()));
        }

        writeBuffer = new byte[blocksPerRecord * 512];

        switch (compressionType) {

            case TarFileOutputStream.Compression.NO_COMPRESSION :
                writeStream = new FileOutputStream(writeFile);
                break;

            case TarFileOutputStream.Compression.GZIP_COMPRESSION :
                writeStream =
                    new GZIPOutputStream(new FileOutputStream(writeFile),
                                         writeBuffer.length);
                break;

            default :
                throw new IllegalArgumentException(
                    RB.singleton.getString(
                        RB.COMPRESSION_UNKNOWN, compressionType));
        }

//#ifdef JAVA6
        writeFile.setExecutable(false, true);
        writeFile.setExecutable(false, false);
        writeFile.setReadable(false, false);
        writeFile.setReadable(true, true);
        writeFile.setWritable(false, false);
        writeFile.setWritable(true, true);

//#endif
        // We restrict permissions to the file owner before writing
        // anything, in case we will be writing anything private into this
        // file.
    }

    /**
     * This class and subclasses should write to the underlying writeStream
     * <b>ONLY WITH THIS METHOD</b>.
     * That way we can be confident that bytesWritten will always be accurate.
     */
    public void write(byte[] byteArray, int byteCount) throws IOException {

        writeStream.write(byteArray, 0, byteCount);

        bytesWritten += byteCount;
    }

    /**
     * The normal way to write file data (as opposed to header data or padding)
     * using this class.
     */
    public void write(int byteCount) throws IOException {
        write(writeBuffer, byteCount);
    }

    /**
     * Write a user-specified 512-byte block.
     *
     * For efficiency, write(int) should be used when writing file body content.
     *
     * @see #write(int)
     */
    public void writeBlock(byte[] block) throws IOException {

        if (block.length != 512) {
            throw new IllegalArgumentException(
                RB.singleton.getString(RB.BAD_BLOCK_WRITE_LEN, block.length));
        }

        write(block, block.length);
    }

    /**
     * Writes the specified quantity of zero'd blocks.
     */
    public void writePadBlocks(int blockCount) throws IOException {

        for (int i = 0; i < blockCount; i++) {
            write(ZERO_BLOCK, ZERO_BLOCK.length);
        }
    }

    /**
     * Writes a single zero'd block.
     */
    public void writePadBlock() throws IOException {
        writePadBlocks(1);
    }

    public int bytesLeftInBlock() {

        int modulus = (int) (bytesWritten % 512L);

        if (modulus == 0) {
            return 0;
        }

        return 512 - modulus;
    }

    /**
     * @throws IllegalStateException if end of file not on a block boundary.
     */
    public void assertAtBlockBoundary() {

        if (bytesLeftInBlock() != 0) {
            throw new IllegalArgumentException(
                RB.singleton.getString(
                    RB.ILLEGAL_BLOCK_BOUNDARY, Long.toString(bytesWritten)));
        }
    }

    /**
     * Rounds out the current block to the next block bondary.
     * If we are currently at a block boundary, nothing is done.
     */
    public void padCurrentBlock() throws IOException {

        int padBytes = bytesLeftInBlock();

        if (padBytes == 0) {
            return;
        }

        write(ZERO_BLOCK, padBytes);

        // REMOVE THIS DEV-ASSERTION:
        assertAtBlockBoundary();
    }

    /**
     * Implements java.io.Flushable.
     *
     * @see java.io.Flushable
     */
    public void flush() throws IOException {
        writeStream.flush();
    }

    /**
     * Implements java.io.Closeable.
     * <P/>
     * <B>IMPORTANT:<B/>  This method <B>deletes</B> the work file after
     * closing it!
     *
     * @see java.io.Closeable
     */
    public void close() throws IOException {

        writeStream.close();

        if (!writeFile.delete()) {
            throw new IOException(
                RB.singleton.getString(
                    RB.WORKFILE_DELETE_FAIL, writeFile.getAbsolutePath()));
        }
    }

    public long getBytesWritten() {
        return bytesWritten;
    }

    /**
     * (Only) when this method returns successfully, the generated file will be
     * a valid tar file.
     *
     * This method always performs a close, so you never need to call the close
     * if your code makes it to this method.
     * (You do need to call close if processing is aborted before calling
     * finish()).
     *
     * @see #close
     */
    public void finish() throws IOException {
        // A VoltDB extension to enable archiving to a stream
        finishStream();
        writeFile.renameTo(targetFile);
    }

    public void finishStream() throws IOException {
        // End of VoltDB extension

        try {
            long finalBlock = bytesWritten / 512 + 2;

            if (finalBlock % blocksPerRecord != 0) {

                // Round up total archive size to a blocksPerRecord multiple
                finalBlock = (finalBlock / blocksPerRecord + 1)
                             * blocksPerRecord;
            }

            int finalPadBlocks = (int) (finalBlock - bytesWritten / 512L);

            if (TarFileOutputStream.debug) {
                System.out.println(RB.singleton.getString(RB.PAD_BLOCK_WRITE,
                        finalPadBlocks));
            }

            writePadBlocks(finalPadBlocks);
        } catch (IOException ioe) {
            try {
                close();
            } catch (IOException ne) {

                // Too difficult to report every single error.
                // More important that the user know about the original Exc.
            }

            throw ioe;
        }

        writeStream.close();
        // A VoltDB extension to enable archiving to a stream
        /* disable 1 line ...
        writeFile.renameTo(targetFile);
        ... disabled 1 line */
        // End of VoltDB extension
    }

    /************************* Volt DB Extensions *************************/

    public TarFileOutputStream(GZIPOutputStream outputStream) throws IOException {
        blocksPerRecord = TarFileOutputStream.Compression.DEFAULT_BLOCKS_PER_RECORD;

        writeBuffer = new byte[blocksPerRecord * 512];

        writeStream = outputStream;
    }
    /**********************************************************************/
}
