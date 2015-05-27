/* Copyright (c) 2001-2014, The HSQL Development Group
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.lib.InputStreamInterface;
import org.hsqldb_voltpatches.lib.InputStreamWrapper;
import org.hsqldb_voltpatches.lib.StringUtil;

/**
 * Generates a tar archive from specified Files and InputStreams.
 * Modified by fredt for hot backup
 * @version 2.2.9
 * @since 2.0.0
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
public class TarGenerator {

    protected TarFileOutputStream      archive;
    protected List<TarEntrySupplicant> entryQueue =
        new ArrayList<TarEntrySupplicant>();
    protected long paxThreshold = 0100000000000L;

    // in bytes.  Value here exactly = 8GB.

    /**
     * When data file is this size or greater, in bytes, a
     * Pix Interchange Format 'x' record will be created and used for the file
     * entry.
     * <P>
     * <B>Limitation</B>
     * At this time, PAX is only implemented for entries added as Files,
     * not entries added as Stream.
     * </P>
     */
    public void setPaxThreshold(long paxThreshold) {
        this.paxThreshold = paxThreshold;
    }

    /**
     * @see #setPaxThreshold(long)
     */
    public long getPaxThreshold() {
        return paxThreshold;
    }

    /**
     * Compression is determined directly by the suffix of the file name in
     * the specified path.
     *
     * @param inFile  Absolute or relative (from user.dir) File for
     *                     tar file to be created.  getName() Suffix must
     *                     indicate tar file and may indicate a compression
     *                     method.
     * @param overWrite    True to replace an existing file of same path.
     * @param blocksPerRecord  Null will use default tar value.
     */
    public TarGenerator(File inFile, boolean overWrite, Integer blocksPerRecord)
            throws IOException {
        File archiveFile = inFile.getAbsoluteFile();

        // Do this so we can be sure .getParent*() is non-null.  (Also allows
        // us to use .getPath() instead of very long .getAbsolutePath() for
        // error messages.
        int compression = TarFileOutputStream.Compression.NO_COMPRESSION;

        if (archiveFile.getName().endsWith(".tgz")
                || archiveFile.getName().endsWith(".tar.gz")) {
            compression = TarFileOutputStream.Compression.GZIP_COMPRESSION;
        } else if (archiveFile.getName().endsWith(".tar")) {

            // purposefully do nothing
        } else {
            throw new IllegalArgumentException(
                RB.unsupported_ext.getString(
                    getClass().getName(), archiveFile.getPath()));
        }

        if (archiveFile.exists()) {
            if (!overWrite) {
                throw new IOException(
                    RB.dest_exists.getString(archiveFile.getPath()));
            }
        } else {
            File parentDir = archiveFile.getParentFile();

            // parentDir will be absolute, since archiveFile is absolute.
            if (parentDir.exists()) {
                if (!parentDir.isDirectory()) {
                    throw new IOException(
                        RB.parent_not_dir.getString(parentDir.getPath()));
                }

                if (!parentDir.canWrite()) {
                    throw new IOException(
                        RB.cant_write_parent.getString(parentDir.getPath()));
                }
            } else {
                if (!parentDir.mkdirs()) {
                    throw new IOException(
                        RB.parent_create_fail.getString(parentDir.getPath()));
                }
            }
        }

        archive = (blocksPerRecord == null)
                  ? new TarFileOutputStream(archiveFile, compression)
                  : new TarFileOutputStream(archiveFile, compression,
                  blocksPerRecord.intValue());

        if ((blocksPerRecord != null) && TarFileOutputStream.debug) {
            System.out.println(
                RB.bpr_write.getString(blocksPerRecord.intValue()));
        }
    }

    public void queueEntry(File file)
            throws FileNotFoundException, TarMalformatException {
        queueEntry(null, file);
    }

    public void queueEntry(String entryPath, File file)
            throws FileNotFoundException, TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, file, archive,
                paxThreshold));
    }

    public void queueEntry(String entryPath, InputStreamInterface is)
            throws FileNotFoundException, TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, is, archive,
                paxThreshold));
    }

    /**
     * This method does not support Pax Interchange Format, nor data sizes
     * greater than 2G.
     * <P>
     * This limitation may or may not be eliminated in the future.
     * </P>
     */
    public void queueEntry(String entryPath, InputStream inStream, int maxBytes)
            throws IOException, TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, inStream, maxBytes,
                '0', archive));
    }

    /**
     * This method does release all of the streams, even if there is a failure.
     */
    public void write() throws IOException, TarMalformatException {
// A VoltDB extension to generalize the feedback channel
        write(System.err);
    }

    /// Short-term backward compatability with the one call from VoltDB/pro
    /// allows a branch with hsqldb232 upgraded to continue to
    /// build alongside VoltDB/pro master.
    /// VoltDB/voltdb code should use a different overload.
    /// TODO: Eliminate this method and bypass this call in VEMCore.java
    /// when the hsqldb232 upgrade hits master.
    public void write(boolean must_be_false, boolean always_false)
            throws IOException, TarMalformatException {
        assert(!must_be_false);
        assert(!always_false);
        write(null);
    }

    public void write(java.io.PrintStream feedback) throws IOException, TarMalformatException {
// End of VoltDB extension
        if (TarFileOutputStream.debug) {
            System.out.println(
                RB.write_queue_report.getString(entryQueue.size()));
        }

        TarEntrySupplicant entry;

        try {
            for (int i = 0; i < entryQueue.size(); i++) {
// A VoltDB extension to generalize the feedback channel
                if (feedback != null) feedback.print(Integer.toString(i + 1) + " / "
/* disable 1 line ...
                System.err.print(Integer.toString(i + 1) + " / "
... disabled 1 line */
// End of VoltDB extension
                                 + entryQueue.size() + ' ');
                entry = entryQueue.get(i);
// A VoltDB extension to generalize the feedback channel
                if (feedback != null) feedback.print(entry.getPath() + "... ");
/* disable 1 line ...
                System.err.print(entry.getPath() + "... ");
... disabled 1 line */
// End of VoltDB extension
                entry.write();
                archive.assertAtBlockBoundary();
// A VoltDB extension to generalize the feedback channel
                if (feedback != null) feedback.println();
/* disable 1 line ...
                System.err.println();
... disabled 1 line */
// End of VoltDB extension
            }

            archive.finish();
        } catch (IOException ioe) {
            System.err.println();    // Exception should cause a report

            try {

                // Just release resources from any Entry's input, which may be
                // left open.
                for (TarEntrySupplicant sup : entryQueue) {
                    sup.close();
                }

                archive.close();
            } catch (IOException ne) {

                // Too difficult to report every single error.
                // More important that the user know about the original Exc.
            }

            throw ioe;
        }
    }

    /**
     * Slots for supplicant files and input streams to be added to a Tar
     * archive.
     *
     * @author Blaine Simpson (blaine dot simpson at admc dot com)
     */
    static protected class TarEntrySupplicant {
        static final byte[] HEADER_TEMPLATE =
            TarFileOutputStream.ZERO_BLOCK.clone();
        static Character              swapOutDelim = null;
        final static byte[] ustarBytes   = {
            'u', 's', 't', 'a', 'r'
        };

        static {
            char c = System.getProperty("file.separator").charAt(0);

            if (c != '/') {
                swapOutDelim = Character.valueOf(c);
            }

            try {
                writeField(TarHeaderField.uid, 0L, HEADER_TEMPLATE);
                writeField(TarHeaderField.gid, 0L, HEADER_TEMPLATE);
            } catch (TarMalformatException tme) {

                // This would definitely get caught in Dev env.
                throw new RuntimeException(tme);
            }

            // Setting uid and gid to 0 = root.
            // Misleading, yes.  Anything better we can do?  No.
            int magicStart = TarHeaderField.magic.getStart();

            for (int i = 0; i < ustarBytes.length; i++) {

                // UStar magic field
                HEADER_TEMPLATE[magicStart + i] = ustarBytes[i];
            }

            HEADER_TEMPLATE[263] = '0';
            HEADER_TEMPLATE[264] = '0';

            // UStar version field, version = 00
            // This is the field that Gnu Tar desecrates.
        }

        static protected void writeField(TarHeaderField field, String newValue,
                                         byte[] target)
                throws TarMalformatException {
            int    start = field.getStart();
            int    stop  = field.getStop();
            byte[] ba;

            try {
                ba = newValue.getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            if (ba.length > stop - start) {
                throw new TarMalformatException(
                    RB.tar_field_toobig.getString(field.toString(), newValue));
            }

            for (int i = 0; i < ba.length; i++) {
                target[start + i] = ba[i];
            }
        }

        static protected void clearField(TarHeaderField field, byte[] target) {
            int start = field.getStart();
            int stop  = field.getStop();

            for (int i = start; i < stop; i++) {
                target[i] = 0;
            }
        }

        static protected void writeField(TarHeaderField field, long newValue,
                                         byte[] target)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(
                field,
                TarEntrySupplicant.prePaddedOctalString(
                    newValue, field.getStop() - field.getStart()), target);
        }

        public static String prePaddedOctalString(long val, int width) {
            return StringUtil.toPaddedString(Long.toOctalString(val), width,
                                             '0', false);
        }

        protected byte[] rawHeader = HEADER_TEMPLATE.clone();
        protected String fileMode  = DEFAULT_FILE_MODES;

        // Following fields are always initialized by constructors.
        protected InputStreamInterface inputStream;
        protected String               path;
        protected long                 modTime;
        protected TarFileOutputStream  tarStream;
        protected long                 dataSize;    // In bytes
        protected boolean              paxSized = false;
        protected final long           paxThreshold;

        // (paxSized = true) tells the target entry to NOT set the size header field,
        // to ensure that no tar client accidentally extracts only
        // a portion of the file data.
        // If the client can't read the correct size from the PIF data,
        // we want the client to report that so the user can get a better
        // tar client!
        // Size will only be written to entry's header if paxSized is false.
        public String getPath() {
            return path;
        }

        public long getDataSize() {
            return dataSize;
        }

        /*
         * Internal constructor that validates the entry's path.
         */
        protected TarEntrySupplicant(String path, char typeFlag,
                                     TarFileOutputStream tarStream,
                                     long paxThreshold)
                throws TarMalformatException {
            this.paxThreshold = paxThreshold;

            if (path == null) {
                throw new IllegalArgumentException(
                    RB.missing_supp_path.getString());
            }

            this.path      = (swapOutDelim == null)
                             ? path
                             : path.replace(swapOutDelim.charValue(), '/');
            this.tarStream = tarStream;
            writeField(TarHeaderField.typeflag, typeFlag);

            if ((typeFlag == '\0') || (typeFlag == ' ')) {
                writeField(TarHeaderField.uname,
                           System.getProperty("user.name"), HEADER_TEMPLATE);
                writeField(TarHeaderField.gname, "root", HEADER_TEMPLATE);

                // Setting UNAME and GNAME at the instance level instead of the
                // static template, because record types 'x' and 'g' do not set
                // these fields.
                // POSIX UStar compliance requires that we set "gname" field.
                // It's impossible for use to determine the correct value from
                // Java.  We punt with "root" because (a) it's the only group
                // name
                // we know should exist on every UNIX system, and (b) every tar
                // client gracefully handles it when extractor user does not
                // have privs for the specified group.
            }
        }

        /**
         * This creates a 'x' entry for a 0/\0 entry.
         */
        public TarEntrySupplicant makeXentry()
                throws IOException, TarMalformatException {
            PIFGenerator pif = new PIFGenerator(new File(path));

            pif.addRecord("size", dataSize);

            /*
             *  Really bad to make pseudo-stream just to get a byte array out
             * of it, but it would be a very poor use of development time to
             * re-design this class because the comparative time wasted at
             * runtime will be negligable compared to storing the data entries.
             */
            return new TarEntrySupplicant(
                pif.getName(), new ByteArrayInputStream(pif.toByteArray()),
                pif.size(), 'x', tarStream);
        }

        /**
         * After instantiating a TarEntrySupplicant, the user must either invoke
         * write() or close(), to release system resources on the input
         * File/Stream.
         */
        public TarEntrySupplicant(String path, File file,
                                  TarFileOutputStream tarStream,
                                  long paxThreshold)
                throws FileNotFoundException, TarMalformatException {

            // Must use an expression-embedded ternary here to satisfy compiler
            // that this() call be first statement in constructor.
            this(((path == null)
                  ? file.getPath()
                  : path), '0', tarStream, paxThreshold);

            // Difficult call for '0'.  binary 0 and character '0' both mean
            // regular file.  Binary 0 pre-UStar is probably more portable,
            // but we are writing a valid UStar header, and I doubt anybody's
            // tar implementation would choke on this since there is no
            // outcry of UStar archives failing to work with older tars.
            if (!file.isFile()) {
                throw new IllegalArgumentException(
                    RB.nonfile_entry.getString());
            }

            if (!file.canRead()) {
                throw new IllegalArgumentException(
                    RB.read_denied.getString(file.getAbsolutePath()));
            }

            modTime     = file.lastModified() / 1000L;
            fileMode    = TarEntrySupplicant.getLameMode(file);
            dataSize    = file.length();
            inputStream = new InputStreamWrapper(new FileInputStream(file));
        }

        public TarEntrySupplicant(String path, InputStreamInterface is,
                                  TarFileOutputStream tarStream,
                                  long paxThreshold)
                throws FileNotFoundException, TarMalformatException {

            // Must use an expression-embedded ternary here to satisfy compiler
            // that this() call be first statement in constructor.
            this(path, '0', tarStream, paxThreshold);

            // Difficult call for '0'.  binary 0 and character '0' both mean
            // regular file.  Binary 0 pre-UStar is probably more portable,
            // but we are writing a valid UStar header, and I doubt anybody's
            // tar implementation would choke on this since there is no
            // outcry of UStar archives failing to work with older tars.
            modTime     = System.currentTimeMillis() / 1000L;
            fileMode    = DEFAULT_FILE_MODES;
            inputStream = is;
        }

        /**
         * After instantiating a TarEntrySupplicant, the user must either invoke
         * write() or close(), to release system resources on the input
         * File/Stream.
         * <P>
         * <B>WARNING:</B>
         * Do not use this method unless the quantity of available RAM is
         * sufficient to accommodate the specified maxBytes all at one time.
         * This constructor loads all input from the specified InputStream into
         * RAM before anything is written to disk.
         * </P>
         *
         * @param maxBytes This method will fail if more than maxBytes bytes
         *                 are supplied on the specified InputStream.
         *                 As the type of this parameter enforces, the max
         *                 size you can request is 2GB.
         */
        public TarEntrySupplicant(String path, InputStream origStream,
                                  int maxBytes, char typeFlag,
                                  TarFileOutputStream tarStream)
                throws IOException, TarMalformatException {

            /*
             * If you modify this, make sure to not intermix reading/writing of
             * the PipedInputStream and the PipedOutputStream, or you could
             * cause dead-lock.  Everything is safe if you close the
             * PipedOutputStream before reading the PipedInputStream.
             */
            this(path, typeFlag, tarStream, 0100000000000L);

            if (maxBytes < 1) {
                throw new IllegalArgumentException(RB.read_lt_1.getString());
            }

            int               i;
            PipedOutputStream outPipe = new PipedOutputStream();

            /*
             *  This constructor not available until Java 1.6:
             * inputStream = new PipedInputStream(outPipe, maxBytes);
             */
            try {
                inputStream =
                    new InputStreamWrapper(new PipedInputStream(outPipe));

                while ((i =
                        origStream.read(tarStream.writeBuffer, 0,
                                        tarStream.writeBuffer.length)) > 0) {
                    outPipe.write(tarStream.writeBuffer, 0, i);
                }

                outPipe.flush();    // Do any good on a pipe?
                dataSize = inputStream.available();

                if (TarFileOutputStream.debug) {
                    System.out.println(
                        RB.stream_buffer_report.getString(
                            Long.toString(dataSize)));
                }
            } catch (IOException ioe) {
                close();

                throw ioe;
            } finally {
                try {
                    outPipe.close();
                } finally {
                    outPipe = null;    // Encourage buffer GC
                }
            }

            modTime = new java.util.Date().getTime() / 1000L;
        }

        public void close() throws IOException {
            if (inputStream == null) {
                return;
            }

            try {
                inputStream.close();
            } finally {
                inputStream = null;    // Encourage buffer GC
            }
        }

        protected long headerChecksum() {
            long sum = 0;

            for (int i = 0; i < rawHeader.length; i++) {
                boolean isInRange =
                    ((i >= TarHeaderField.checksum.getStart())
                     && (i < TarHeaderField.checksum.getStop()));

                sum += isInRange
                       ? 32
                       : (255 & rawHeader[i]);

                // We ignore current contents of the checksum field so that
                // this method will continue to work right, even if we later
                // recycle the header or RE-calculate a header.
            }

            return sum;
        }

        protected void clearField(TarHeaderField field) {
            TarEntrySupplicant.clearField(field, rawHeader);
        }

        protected void writeField(TarHeaderField field, String newValue)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(field, newValue, rawHeader);
        }

        protected void writeField(TarHeaderField field, long newValue)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(field, newValue, rawHeader);
        }

        protected void writeField(TarHeaderField field, char c)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(field, Character.toString(c),
                                          rawHeader);
        }

        /**
         * Writes entire entry to this object's tarStream.
         *
         * This method is guaranteed to close the supplicant's input stream.
         */
        public void write() throws IOException, TarMalformatException {
            int i;

            try {

                // normal file streams will return -1 as size limit
                // getSizeLimit() is called just before writing the entry
                long sizeLimit = inputStream.getSizeLimit();

                // special stream with explicit zero limit is not written
                if (sizeLimit == 0) {
                    return;
                }

                // special stream
                if (sizeLimit > 0) {
                    dataSize = sizeLimit;
                }

                if (dataSize >= paxThreshold) {
                    paxSized = true;
                    makeXentry().write();
                    System.err.print("x... ");
                }

                writeField(TarHeaderField.name, path);

                // TODO:  If path.length() > 99, then write a PIF entry with
                // the file path.
                // Don't waste time using the PREFIX header field.
                writeField(TarHeaderField.mode, fileMode);

                if (!paxSized) {
                    writeField(TarHeaderField.size, dataSize);
                }

                writeField(TarHeaderField.mtime, modTime);
                writeField(
                    TarHeaderField.checksum,
                    TarEntrySupplicant.prePaddedOctalString(
                        headerChecksum(), 6) + "\0 ");

                // Silly, but that's what the base header spec calls for.
                tarStream.writeBlock(rawHeader);

                long dataStart = tarStream.getBytesWritten();

                while ((i = inputStream.read(tarStream.writeBuffer)) > 0) {
                    tarStream.write(i);
                }

                if (dataStart + dataSize != tarStream.getBytesWritten()) {
                    throw new IOException(
                        RB.data_changed.getString(
                            Long.toString(dataSize),
                            Long.toString(
                                tarStream.getBytesWritten() - dataStart)));
                }

                tarStream.padCurrentBlock();
            } finally {
                close();
            }
        }

        /**
         * This method is so-named because it only sets the owner privileges,
         * not any "group" or "other" privileges.
         * <P>
         * This is because of Java limitation.
         * Incredibly, with Java 1.6, the API gives you the power to set
         * privileges for "other" (last nibble in file Mode), but no ability
         * to detect the same.
         * </P>
         */
        static protected String getLameMode(File file) {
            int umod = 0;

//#ifdef JAVA6
            if (file.canExecute()) {
                umod = 1;
            }

//#endif
            if (file.canWrite()) {
                umod += 2;
            }

            if (file.canRead()) {
                umod += 4;
            }

            return "0" + umod + "00";

            // Conservative since Java gives us no way to determine group or
            // other privileges on a file, and this file may contain passwords.
        }

        public static final String DEFAULT_FILE_MODES = "600";

        // Be conservative, because these files contain passwords
    }
    // A VoltDB extension to support explicit tar outputstream pre-configuration
    public TarGenerator(java.util.zip.GZIPOutputStream outputStream) throws IOException {
        archive = new TarFileOutputStream(outputStream);
    }
    // End of VoltDB extension
}
