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

import org.hsqldb_voltpatches.lib.StringUtil;

/**
 * Generates a tar archive from specified Files and InputStreams.
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
public class TarGenerator {

    /**
     * Creates specified tar file to contain specified files, or stdin,
     * using default blocks-per-record and replacing tar file if it already
     * exists.
     */
    static public void main(String[] sa)
    throws IOException, TarMalformatException {

        if (sa.length < 1) {
            System.out.println(RB.singleton.getString(RB.TARGENERATOR_SYNTAX,
                    DbBackup.class.getName()));
            System.exit(0);
        }

        TarGenerator generator = new TarGenerator(new File(sa[0]), true, null);

        if (sa.length == 1) {
            generator.queueEntry("stdin", System.in, 10240);
        } else {
            for (int i = 1; i < sa.length; i++) {
                generator.queueEntry(new File(sa[i]));
            }
        }

        generator.write();
    }

    protected TarFileOutputStream archive;
    protected List                entryQueue   = new ArrayList();
    protected long                paxThreshold = 0100000000000L;

    // in bytes.  Value here exactly = 8GB.

    /**
     * When data file is this size or greater, in bytes, a
     * Pix Interchange Format 'x' record will be created and used for the file
     * entry.
     * <P/>
     * <B>Limitation</B>
     * At this time, PAX is only implemented for entries added a Files,
     * not entries added as Stream.
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
    public TarGenerator(File inFile, boolean overWrite,
                        Integer blocksPerRecord) throws IOException {

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
                RB.singleton.getString(
                    RB.UNSUPPORTED_EXT, getClass().getName(),
                    archiveFile.getPath()));
        }

        if (archiveFile.exists()) {
            if (!overWrite) {
                throw new IOException(RB.singleton.getString(RB.DEST_EXISTS,
                        archiveFile.getPath()));
            }
        } else {
            File parentDir = archiveFile.getParentFile();

            // parentDir will be absolute, since archiveFile is absolute.
            if (parentDir.exists()) {
                if (!parentDir.isDirectory()) {
                    throw new IOException(
                        RB.singleton.getString(
                            RB.PARENT_NOT_DIR, parentDir.getPath()));
                }

                if (!parentDir.canWrite()) {
                    throw new IOException(
                        RB.singleton.getString(
                            RB.CANT_WRITE_PARENT, parentDir.getPath()));
                }
            } else {
                if (!parentDir.mkdirs()) {
                    throw new IOException(
                        RB.singleton.getString(
                            RB.PARENT_CREATE_FAIL, parentDir.getPath()));
                }
            }
        }

        archive = (blocksPerRecord == null)
                  ? new TarFileOutputStream(archiveFile, compression)
                  : new TarFileOutputStream(archiveFile, compression,
                                            blocksPerRecord.intValue());

        if (blocksPerRecord != null && TarFileOutputStream.debug) {
            System.out.println(RB.singleton.getString(RB.BPR_WRITE,
                    blocksPerRecord.intValue()));
        }
    }

    public void queueEntry(File file)
    throws FileNotFoundException, TarMalformatException {
        queueEntry(null, file);
    }

    public void queueEntry(String entryPath,
                           File file)
                           throws FileNotFoundException,
                                  TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, file, archive));
    }

    /**
     * This method does not support Pax Interchange Format, nor data sizes
     * greater than 2G.
     * <P/>
     * This limitation may or may not be eliminated in the future.
     */
    public void queueEntry(String entryPath, InputStream inStream,
                           int maxBytes)
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

    public void write(java.io.PrintStream feedback) throws IOException, TarMalformatException {
// End of VoltDB extension

        if (TarFileOutputStream.debug) {
            System.out.println(RB.singleton.getString(RB.WRITE_QUEUE_REPORT,
                    entryQueue.size()));
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

                entry = (TarEntrySupplicant) entryQueue.get(i);

// A VoltDB extension to generalize the feedback channel
                if (feedback != null) feedback.print(entry.getPath() + "... ");
/* disable 1 line ...
                System.err.print(entry.getPath() + "... ");
... disabled 1 line */
// End of VoltDB extension

                if (entry.getDataSize() >= paxThreshold) {
                    entry.makeXentry().write();
// A VoltDB extension to generalize the feedback channel
                if (feedback != null) feedback.print("x... ");
/* disable 1 line ...
                    System.err.print("x... ");
... disabled 1 line */
// End of VoltDB extension
                }

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
                for (int i = 0; i < entryQueue.size(); i++) {
                    ((TarEntrySupplicant) entryQueue.get(i)).close();
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

        static protected byte[] HEADER_TEMPLATE =
            TarFileOutputStream.ZERO_BLOCK.clone();
        static Character              swapOutDelim = null;
        final protected static byte[] ustarBytes   = {
            'u', 's', 't', 'a', 'r'
        };

        static {
            char c = System.getProperty("file.separator").charAt(0);

            if (c != '/') {
                swapOutDelim = new Character(c);
            }

            try {
                writeField(TarHeaderFields.UID, 0L, HEADER_TEMPLATE);
                writeField(TarHeaderFields.GID, 0L, HEADER_TEMPLATE);
            } catch (TarMalformatException tme) {

                // This would definitely get caught in Dev env.
                throw new RuntimeException(tme);
            }

            // Setting uid and gid to 0 = root.
            // Misleading, yes.  Anything better we can do?  No.
            int magicStart = TarHeaderFields.getStart(TarHeaderFields.MAGIC);

            for (int i = 0; i < ustarBytes.length; i++) {

                // UStar magic field
                HEADER_TEMPLATE[magicStart + i] = ustarBytes[i];
            }

            HEADER_TEMPLATE[263] = '0';
            HEADER_TEMPLATE[264] = '0';

            // UStar version field, version = 00
            // This is the field that Gnu Tar desecrates.
        }

        static protected void writeField(int fieldId, String newValue,
                                         byte[] target)
                                         throws TarMalformatException {

            int    start = TarHeaderFields.getStart(fieldId);
            int    stop  = TarHeaderFields.getStop(fieldId);
            byte[] ba;

            try {
                ba = newValue.getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            if (ba.length > stop - start) {
                throw new TarMalformatException(
                    RB.singleton.getString(
                        RB.TAR_FIELD_TOOBIG,
                        TarHeaderFields.toString(fieldId), newValue));
            }

            for (int i = 0; i < ba.length; i++) {
                target[start + i] = ba[i];
            }
        }

        static protected void clearField(int fieldId, byte[] target) {

            int start = TarHeaderFields.getStart(fieldId);
            int stop  = TarHeaderFields.getStop(fieldId);

            for (int i = start; i < stop; i++) {
                target[i] = 0;
            }
        }

        static protected void writeField(int fieldId, long newValue,
                                         byte[] target)
                                         throws TarMalformatException {

            TarEntrySupplicant.writeField(
                fieldId,
                TarEntrySupplicant.prePaddedOctalString(
                    newValue,
                    TarHeaderFields.getStop(fieldId)
                    - TarHeaderFields.getStart(fieldId)), target);
        }

        static public String prePaddedOctalString(long val, int width) {
            return StringUtil.toPaddedString(Long.toOctalString(val), width,
                                             '0', false);
        }

        protected byte[] rawHeader = HEADER_TEMPLATE.clone();
        protected String fileMode  = DEFAULT_FILE_MODES;

        // Following fields are always initialized by constructors.
        protected InputStream         inputStream;
        protected String              path;
        protected long                modTime;
        protected TarFileOutputStream tarStream;
        protected long                dataSize;    // In bytes
        protected boolean             paxSized = false;

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
                                     TarFileOutputStream tarStream)
                                     throws TarMalformatException {

            if (path == null) {
                throw new IllegalArgumentException(
                    RB.singleton.getString(RB.MISSING_SUPP_PATH));
            }

            this.path = (swapOutDelim == null) ? path
                                               : path.replace(
                                                   swapOutDelim.charValue(),
                                                   '/');
            this.tarStream = tarStream;

            writeField(TarHeaderFields.TYPEFLAG, typeFlag);

            if (typeFlag == '\0' || typeFlag == ' ') {
                writeField(TarHeaderFields.UNAME,
                           System.getProperty("user.name"), HEADER_TEMPLATE);
                writeField(TarHeaderFields.GNAME, "root", HEADER_TEMPLATE);

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

            paxSized = true;

            // This tells the target entry to NOT set the size header field,
            // to ensure that no tar client accidentally extracts only
            // a portion of the file data.
            // If the client can't read the correct size from the PIF data,
            // we want the client to report that so the user can get a better
            // tar client!
            /* Really bad to make pseudo-stream just to get a byte array out
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
                                  TarFileOutputStream tarStream)
                                  throws FileNotFoundException,
                                         TarMalformatException {

            // Must use an expression-embedded ternary here to satisfy compiler
            // that this() call be first statement in constructor.
            this(((path == null) ? file.getPath()
                                 : path), '0', tarStream);

            // Difficult call for '0'.  binary 0 and character '0' both mean
            // regular file.  Binary 0 pre-UStar is probably more portable,
            // but we are writing a valid UStar header, and I doubt anybody's
            // tar implementation would choke on this since there is no
            // outcry of UStar archives failing to work with older tars.
            if (!file.isFile()) {
                throw new IllegalArgumentException(
                    RB.singleton.getString(RB.NONFILE_ENTRY));
            }

            if (!file.canRead()) {
                throw new IllegalArgumentException(
                    RB.singleton.getString(
                        RB.READ_DENIED, file.getAbsolutePath()));
            }

            modTime     = file.lastModified() / 1000L;
            fileMode    = TarEntrySupplicant.getLameMode(file);
            dataSize    = file.length();
            inputStream = new FileInputStream(file);
        }

        /**
         * After instantiating a TarEntrySupplicant, the user must either invoke
         * write() or close(), to release system resources on the input
         * File/Stream.
         * <P/>
         * <B>WARNING:</B>
         * Do not use this method unless the quantity of available RAM is
         * sufficient to accommodate the specified maxBytes all at one time.
         * This constructor loads all input from the specified InputStream into
         * RAM before anything is written to disk.
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
            this(path, typeFlag, tarStream);

            if (maxBytes < 1) {
                throw new IllegalArgumentException(
                    RB.singleton.getString(RB.READ_LT_1));
            }

            int               i;
            PipedOutputStream outPipe = new PipedOutputStream();

            inputStream = new PipedInputStream(outPipe);

            /* This constructor not available until Java 1.6:
            inputStream = new PipedInputStream(outPipe, maxBytes);
            */
            try {
                while ((i =
                        origStream
                            .read(tarStream.writeBuffer, 0, tarStream
                                .writeBuffer.length)) > 0) {
                    outPipe.write(tarStream.writeBuffer, 0, i);
                }

                outPipe.flush();    // Do any good on a pipe?

                dataSize = inputStream.available();

                if (TarFileOutputStream.debug) {
                    System.out.println(
                        RB.singleton.getString(
                            RB.STREAM_BUFFER_REPORT, Long.toString(dataSize)));
                }
            } catch (IOException ioe) {
                inputStream.close();

                throw ioe;
            } finally {
                outPipe.close();
            }

            modTime = new java.util.Date().getTime() / 1000L;
        }

        public void close() throws IOException {
            inputStream.close();
        }

        protected long headerChecksum() {

            long sum = 0;

            for (int i = 0; i < rawHeader.length; i++) {
                boolean isInRange =
                    (i >= TarHeaderFields.getStart(TarHeaderFields.CHECKSUM)
                     && i < TarHeaderFields.getStop(TarHeaderFields.CHECKSUM));

                sum += isInRange ? 32
                                 : (255 & rawHeader[i]);

                // We ignore current contents of the checksum field so that
                // this method will continue to work right, even if we later
                // recycle the header or RE-calculate a header.
            }

            return sum;
        }

        protected void clearField(int fieldId) {
            TarEntrySupplicant.clearField(fieldId, rawHeader);
        }

        protected void writeField(int fieldId,
                                  String newValue)
                                  throws TarMalformatException {
            TarEntrySupplicant.writeField(fieldId, newValue, rawHeader);
        }

        protected void writeField(int fieldId,
                                  long newValue) throws TarMalformatException {
            TarEntrySupplicant.writeField(fieldId, newValue, rawHeader);
        }

        protected void writeField(int fieldId,
                                  char c) throws TarMalformatException {
            TarEntrySupplicant.writeField(fieldId, Character.toString(c),
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
                writeField(TarHeaderFields.NAME, path);

                // TODO:  If path.length() > 99, then write a PIF entry with
                // the file path.
                // Don't waste time using the PREFIX header field.
                writeField(TarHeaderFields.MODE, fileMode);

                if (!paxSized) {
                    writeField(TarHeaderFields.SIZE, dataSize);
                }

                writeField(TarHeaderFields.MTIME, modTime);
                writeField(
                    TarHeaderFields.CHECKSUM,
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
                        RB.singleton.getString(
                            RB.DATA_CHANGED, Long.toString(dataSize),
                            Long.toString(
                                (tarStream.getBytesWritten() - dataStart))));
                }

                tarStream.padCurrentBlock();
            } finally {
                close();
            }
        }

        /**
         * This method is so-named because it only sets the owner privileges,
         * not any "group" or "other" privileges.
         * <P/>
         * This is because of Java limitation.
         * Incredibly, with Java 1.6, the API gives you the power to set
         * privileges for "other" (last nibble in file Mode), but no ability
         * to detect the same.
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

        final static public String DEFAULT_FILE_MODES = "600";

        // Be conservative, because these files contain passwords
    }

    /************************* Volt DB Extensions *************************/

    public void write(boolean outputToStream, boolean verbose) throws IOException, TarMalformatException {

        if (TarFileOutputStream.debug) {
            System.out.println(RB.singleton.getString(RB.WRITE_QUEUE_REPORT,
                    entryQueue.size()));
        }

        TarEntrySupplicant entry;

        try {
            for (int i = 0; i < entryQueue.size(); i++) {
                if (verbose) {
                    System.out.print(Integer.toString(i + 1) + " / "
                                     + entryQueue.size() + ' ');
                }

                entry = (TarEntrySupplicant) entryQueue.get(i);

                if (verbose) {
                    System.out.print(entry.getPath() + "... ");
                }

                if (entry.getDataSize() >= paxThreshold) {
                    entry.makeXentry().write();
                    if (verbose) {
                        System.out.print("x... ");
                    }
                }

                entry.write();
                archive.assertAtBlockBoundary();

                if (verbose) {
                    System.out.println();
                }
            }

            if (outputToStream) {
                archive.finishStream();
                return;
            }
            archive.finish();
        } catch (IOException ioe) {
            System.err.println();    // Exception should cause a report

            try {

                // Just release resources from any Entry's input, which may be
                // left open.
                for (int i = 0; i < entryQueue.size(); i++) {
                    ((TarEntrySupplicant) entryQueue.get(i)).close();
                }

                archive.close();
            } catch (IOException ne) {

                // Too difficult to report every single error.
                // More important that the user know about the original Exc.
            }

            throw ioe;
        }
    }

    public TarGenerator(java.util.zip.GZIPOutputStream outputStream) throws IOException {
        archive = new TarFileOutputStream(outputStream);
    }
    /**********************************************************************/
}
