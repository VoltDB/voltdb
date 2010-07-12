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


package org.hsqldb_voltpatches.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Creates a direct, compressed or decompressed copy of a file.
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 * @version 1.9.0
 * @since 1.9.0
 */
public class FileArchiver {

    public static final int  COMPRESSION_NONE = 0;
    public static final int  COMPRESSION_ZIP  = 1;
    public static final int  COMPRESSION_GZIP = 2;
    private static final int COPY_BLOCK_SIZE  = 1 << 16;

    /*
    public static void compressFile(String infilename, String outfilename,
                                    FileAccess storage) throws IOException {
        FileArchiver.archive(infilename, outfilename, storage, COMPRESSION_ZIP);
    }

    public static void decompressFile(String infilename, String outfilename,
                                      FileAccess storage) throws IOException {
        FileArchiver.unarchive(
                infilename, outfilename, storage, COMPRESSION_ZIP);
    }

    public static void copyFile(String infilename, String outfilename,
                                    FileAccess storage) throws IOException {
        FileArchiver.archive(
                infilename, outfilename, storage, COMPRESSION_NONE);
    }

    public static void restoreFile(String infilename, String outfilename,
                                      FileAccess storage) throws IOException {
        FileArchiver.unarchive(
                infilename, outfilename, storage, COMPRESSION_NONE);
    }
    */
    public static void archive(String infilename, String outfilename,
                               FileAccess storage,
                               int compressionType) throws IOException {

        InputStream          in        = null;
        OutputStream         f         = null;
        DeflaterOutputStream deflater  = null;
        boolean              completed = false;

        // if there is no file
        if (!storage.isStreamElement(infilename)) {
            return;
        }

        try {
            byte[] b = new byte[COPY_BLOCK_SIZE];

            in = storage.openInputStreamElement(infilename);
            f  = storage.openOutputStreamElement(outfilename);

            switch (compressionType) {

                case COMPRESSION_ZIP :
                    f = deflater = new DeflaterOutputStream(f,
                            new Deflater(Deflater.BEST_SPEED), b.length);
                    break;

                case COMPRESSION_GZIP :
                    f = deflater = new GZIPOutputStream(f, b.length);
                    break;

                case COMPRESSION_NONE :
                    break;

                default :
                    throw new RuntimeException("FileArchiver"
                                               + compressionType);
            }

            while (true) {
                int l = in.read(b, 0, b.length);

                if (l == -1) {
                    break;
                }

                f.write(b, 0, l);
            }

            completed = true;
        } catch (Throwable e) {
            throw FileUtil.toIOException(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }

                if (f != null) {
                    if (deflater != null) {
                        deflater.finish();
                    }

                    f.close();
                }

                if (!completed && storage.isStreamElement(outfilename)) {
                    storage.removeElement(outfilename);
                }
            } catch (Throwable e) {
                throw FileUtil.toIOException(e);
            }
        }
    }

    public static void unarchive(String infilename, String outfilename,
                                 FileAccess storage,
                                 int compressionType) throws IOException {

        InputStream  f         = null;
        OutputStream outstream = null;
        boolean      completed = false;

        try {
            if (!storage.isStreamElement(infilename)) {
                return;
            }

            storage.removeElement(outfilename);

            byte[] b = new byte[COPY_BLOCK_SIZE];

            f = storage.openInputStreamElement(infilename);

            switch (compressionType) {

                case COMPRESSION_ZIP :
                    f = new InflaterInputStream(f, new Inflater());
                    break;

                case COMPRESSION_GZIP :
                    f = new GZIPInputStream(f, b.length);
                    break;

                case COMPRESSION_NONE :
                    break;

                default :
                    throw new RuntimeException("FileArchiver: "
                                               + compressionType);
            }

            outstream = storage.openOutputStreamElement(outfilename);

            while (true) {
                int l = f.read(b, 0, b.length);

                if (l == -1) {
                    break;
                }

                outstream.write(b, 0, l);
            }

            completed = true;
        } catch (Throwable e) {
            throw FileUtil.toIOException(e);
        } finally {
            try {
                if (f != null) {
                    f.close();
                }

                if (outstream != null) {
                    outstream.flush();
                    outstream.close();
                }

                if (!completed && storage.isStreamElement(outfilename)) {
                    storage.removeElement(outfilename);
                }
            } catch (Throwable e) {
                throw FileUtil.toIOException(e);
            }
        }
    }
}
