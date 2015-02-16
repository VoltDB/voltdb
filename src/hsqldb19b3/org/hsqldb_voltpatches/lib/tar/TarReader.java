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

import org.hsqldb_voltpatches.lib.StringUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

/**
 * Reads a Tar file for reporting or extraction.
 * N.b. this is not a <I>Reader</I> in the <CODE>java.io.Reader</CODE> sense,
 * but in the sense of differentiating <CODE>tar x</CODE> and
 * <CODE>tar t</CODE> from <CODE>tar c</CODE>.
 * <P>
 * <B>SECURITY NOTE</B>
 * Due to pitiful lack of support for file security in Java before version 1.6,
 * this class will only explicitly set permissions if it is compiled for Java
 * 1.6.  If it was not, and if your tar entries contain private data in files
 * with 0400 or similar, be aware that they will be extracted with privs such
 * that they can be ready by anybody.
 * </P>
 *
 * @author Blaine Simpson (blaine dot simpson at admc dot com)
 */
public class TarReader {

    public static final int LIST_MODE = 0;

    /**
     * EXTRACT_MODE refuses to overwrite existing files.
     */
    public static final int EXTRACT_MODE = 1;

    /**
     * OVERWRITE_MODE is just EXTRACT_MODE where we will silently overwrite
     * existing files upon extraction.
     */
    public static final int OVERWRITE_MODE = 2;

    protected TarFileInputStream archive;
    protected Pattern[]          patterns = null;
    protected int                mode;
    protected File               extractBaseDir;    // null means current directory

    // Not used for Absolute path entries
    // This path is always absolutized

    /**
     * Compression is determined directly by the suffix of the file name in
     * the specified path.
     *
     * @param inFile  Absolute or relative (from user.dir) path to
     *                tar file to be read.  Suffix may indicate
     *                a compression method.
     * @param mode    Whether to list, extract-without-overwrite, or
     *                extract-with-overwrite.
     * @param patternStrings
     *                  List of regular expressions to match against tar entry
     *                  names.  If null, all entries will be listed or
     *                  extracted.  If non-null, then only entries with names
     *                  which match will be extracted or listed.
     * @param readBufferBlocks  Null will use default tar value.
     * @param inDir   Directory that RELATIVE entries will be extracted
     *                relative to.  Defaults to current directory (user.dir).
     *                Only used for extract modes and relative file entries.
     * @throws IllegalArgumentException if any given pattern is an invalid
     *                  regular expression.  Don't have to worry about this if
     *                  you call with null 'patterns' param.
     * @see Pattern
     */
    public TarReader(File inFile, int mode, String[] patternStrings,
                     Integer readBufferBlocks, File inDir) throws IOException {

        this.mode = mode;

        File archiveFile = inFile.getAbsoluteFile();

        extractBaseDir = (inDir == null) ? null
                                         : inDir.getAbsoluteFile();

        int compression =
            TarFileOutputStream.Compression.NO_COMPRESSION;

        if (archiveFile.getName().endsWith(".tgz")
                || archiveFile.getName().endsWith(".gz")) {
            compression = TarFileOutputStream.Compression.GZIP_COMPRESSION;
        }

        if (patternStrings != null) {
            patterns = new Pattern[patternStrings.length];

            for (int i = 0; i < patternStrings.length; i++) {
                patterns[i] = Pattern.compile(patternStrings[i]);
            }
        }

        // Don't check for archive file existence here.  We can depend upon the
        // TarFileInputStream to check that.
        archive = (readBufferBlocks == null)
                  ? new TarFileInputStream(archiveFile, compression)
                  : new TarFileInputStream(archiveFile, compression,
                                           readBufferBlocks.intValue());
    }

    public void read() throws IOException, TarMalformatException {
        // A VoltDB extension to make extraction less verbose
        read(true);
    }

    public void read(boolean verbose) throws IOException, TarMalformatException {
        // End of VoltDB extension

        TarEntryHeader header;
        boolean        anyUnsupporteds = false;
        boolean        matched;
        Long           paxSize   = null;
        String         paxString = null;

        try {
            EACH_HEADER:
            while (archive.readNextHeaderBlock()) {
                header = new TarEntryHeader(archive.readBuffer);

                char entryType = header.getEntryType();

                if (entryType == 'x') {
                    /* Since we don't know the name of the target file yet,
                     * we must load the size from all pax headers.
                     * If the target file is not thereafter excluded via
                     * patterns, we will need this size for the listing or to
                     * extract the data.
                     */
                    paxSize   = getPifData(header).getSize();
                    paxString = header.toString();

                    continue;
                }

                if (paxSize != null) {

                    // Ignore "size" field in the entry header because PIF
                    // setting overrides.
                    header.setDataSize(paxSize.longValue());

                    paxSize = null;
                }

                if (patterns != null) {
                    matched = false;

                    for (int i = 0; i < patterns.length; i++) {
                        if (patterns[i].matcher(header.getPath()).matches()) {
                            matched = true;

                            break;
                        }
                    }

                    if (!matched) {
                        paxString = null;

                        skipFileData(header);

                        continue EACH_HEADER;
                    }
                }

                if (entryType != '\0' && entryType != '0'
                        && entryType != 'x') {
                    anyUnsupporteds = true;
                }

                switch (mode) {

                    case LIST_MODE :
                        if (paxString != null) {
                            System.out.println(paxString);
                        }

                        System.out.println(header.toString());
                        skipFileData(header);
                        break;

                    case EXTRACT_MODE :
                    case OVERWRITE_MODE :
                        if (paxString != null) {
                            System.out.println(paxString);
                        }

                        /* Display entry summary before successful extraction.
                         * Both "tar" and "rsync" display the name of the
                         * currently extracting file, and we do the same.
                         * Thefore the currently "shown" name is still being
                         * extracted.
                         */
                        // A VoltDB extension to make extraction less verbose
                        if (verbose) System.out.println(header.toString());
                        /* disable 1 line ...
                        System.out.println(header.toString());
                        ... disabled 1 line */
                        // End of VoltDB extension

                        // Instance variable mode will be used to differentiate
                        // behavior inside of extractFile().
                        if (entryType == '\0' || entryType == '0'
                                || entryType == 'x') {
                            extractFile(header);
                        } else {
                            skipFileData(header);
                        }
                        break;

                    default :
                        throw new IllegalArgumentException(
                                RB.unsupported_mode.getString(mode));
                }

                paxString = null;
            }

            if (anyUnsupporteds) {
                System.out.println(RB.unsupported_entry_present.getString());
            }
        } catch (IOException ioe) {
            archive.close();

            throw ioe;
        }
    }

    protected PIFData getPifData(TarEntryHeader header)
    throws IOException, TarMalformatException {

        /*
         * If you modify this, make sure to not intermix reading/writing of
         * the PipedInputStream and the PipedOutputStream, or you could
         * cause dead-lock.  Everything is safe if you close the
         * PipedOutputStream before reading the PipedInputStream.
         */
        long dataSize = header.getDataSize();

        if (dataSize < 1) {
            throw new TarMalformatException(
                    RB.pif_unknown_datasize.getString());
        }

        if (dataSize > Integer.MAX_VALUE) {
            throw new TarMalformatException(RB.pif_data_toobig.getString(
                    Long.toString(dataSize), Integer.MAX_VALUE));
        }

        int readNow;
        int readBlocks = (int) (dataSize / 512L);
        int modulus    = (int) (dataSize % 512L);

        // Couldn't care less about the entry "name" field.
        PipedInputStream  inPipe  = null;
        PipedOutputStream outPipe = new PipedOutputStream();

        /* This constructor not available until Java 1.6:
                new PipedInputStream(outPipe, (int) dataSize);
        */
        try {
            inPipe = new PipedInputStream(outPipe);
            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                          ? archive.getReadBufferBlocks()
                          : readBlocks;

                archive.readBlocks(readNow);

                readBlocks -= readNow;

                outPipe.write(archive.readBuffer, 0, readNow * 512);
            }

            if (modulus != 0) { archive.readBlock();
                outPipe.write(archive.readBuffer, 0, modulus);
            }

            outPipe.flush();    // Do any good on a pipe?
        } catch (IOException ioe) {
            if (inPipe != null) {
                inPipe.close();
            }

            throw ioe;
        } finally {
            try {
                outPipe.close();
            } finally {
                outPipe = null;  // Encourage buffer GC
            }
        }

        return new PIFData(inPipe);
    }

    protected void extractFile(TarEntryHeader header)
    throws IOException, TarMalformatException {

        if (header.getDataSize() < 1) {
            throw new TarMalformatException(RB.data_size_unknown.getString());
        }

        int  readNow;
        int  readBlocks = (int) (header.getDataSize() / 512L);
        int  modulus    = (int) (header.getDataSize() % 512L);
        File newFile    = header.generateFile();

        if (!newFile.isAbsolute()) {
            newFile = (extractBaseDir == null) ? newFile.getAbsoluteFile()
                                               : new File(extractBaseDir,
                                               newFile.getPath());
        }

        // newFile is definitively Absolutized at this point
        File parentDir = newFile.getParentFile();

        if (newFile.exists()) {
            if (mode != TarReader.OVERWRITE_MODE) {
                throw new IOException(
                    RB.extraction_exists.getString(newFile.getAbsolutePath()));
            }

            if (!newFile.isFile()) {
                throw new IOException(
                        RB.extraction_exists_notfile.getString(
                        newFile.getAbsolutePath()));
            }

            // Better to let FileOutputStream creation zero it than to
            // to newFile.delete().
        }

        if (parentDir.exists()) {
            if (!parentDir.isDirectory()) {
                throw new IOException(
                        RB.extraction_parent_not_dir.getString(
                        parentDir.getAbsolutePath()));
            }

            if (!parentDir.canWrite()) {
                throw new IOException(
                        RB.extraction_parent_not_writable.getString(
                        parentDir.getAbsolutePath()));
            }
        } else {
            if (!parentDir.mkdirs()) {
                throw new IOException(
                        RB.extraction_parent_mkfail.getString(
                        parentDir.getAbsolutePath()));
            }
        }

        int              fileMode  = header.getFileMode();
        FileOutputStream outStream = new FileOutputStream(newFile);

        try {

//#ifdef JAVA6
            // Don't know exactly why I am still able to write to the file
            // after removing read and write privs from myself, but it does
            // work.
            newFile.setExecutable(false, false);
            newFile.setReadable(false, false);
            newFile.setWritable(false, false);
            newFile.setExecutable(((fileMode & 0100) != 0), true);
            newFile.setReadable((fileMode & 0400) != 0, true);
            newFile.setWritable((fileMode & 0200) != 0, true);

//#endif
            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                          ? archive.getReadBufferBlocks()
                          : readBlocks;

                archive.readBlocks(readNow);

                readBlocks -= readNow;

                outStream.write(archive.readBuffer, 0, readNow * 512);
            }

            if (modulus != 0) {
                archive.readBlock();
                outStream.write(archive.readBuffer, 0, modulus);
            }

            outStream.flush();
        } finally {
            try {
                outStream.close();
            } finally {
                outStream = null;  // Encourage buffer GC
            }
        }

        newFile.setLastModified(header.getModTime() * 1000);

        if (newFile.length() != header.getDataSize()) {
            throw new IOException(RB.write_count_mismatch.getString(
                    Long.toString(header.getDataSize()),
                    newFile.getAbsolutePath(),
                    Long.toString(newFile.length())));
        }
    }

    protected void skipFileData(TarEntryHeader header)
    throws IOException, TarMalformatException {

        /*
         * Some entry types which we don't support have 0 data size.
         * If we just return here, the entry will just be skipped./
        */
        if (header.getDataSize() == 0) {
            return;
        }

        if (header.getDataSize() < 0) {
            throw new TarMalformatException(RB.data_size_unknown.getString());
        }

        int skipNow;
        int oddBlocks  = (header.getDataSize() % 512L == 0L) ? 0
                                                             : 1;
        int skipBlocks = (int) (header.getDataSize() / 512L) + oddBlocks;

        while (skipBlocks > 0) {
            skipNow = (skipBlocks > archive.getReadBufferBlocks())
                      ? archive.getReadBufferBlocks()
                      : skipBlocks;

            archive.readBlocks(skipNow);

            skipBlocks -= skipNow;
        }
    }

    /**
     * A Tar entry header constituted from a header block in a tar file.
     *
     * @author Blaine Simpson (blaine dot simpson at admc dot com)
     */
    @SuppressWarnings("serial")
    static protected class TarEntryHeader {

        static protected class MissingField extends Exception {

            private TarHeaderField field;

            public MissingField(TarHeaderField field) {
                this.field = field;
            }

            public String getMessage() {
                return RB.header_field_missing.getString(field.toString());
            }
        }

        protected SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        /**
         * @param rawHeader  May be longer than 512 bytes, but the first 512
         *                   bytes MUST COMPRISE a raw tar entry header.
         */
        public TarEntryHeader(byte[] rawHeader) throws TarMalformatException {

            this.rawHeader = rawHeader;

            Long expectedCheckSum = readInteger(TarHeaderField.checksum);

            try {
                if (expectedCheckSum == null) {
                    throw new MissingField(TarHeaderField.checksum);
                }

                long calculatedCheckSum = headerChecksum();

                if (expectedCheckSum.longValue() != calculatedCheckSum) {
                    throw new TarMalformatException(
                            RB.checksum_mismatch.getString(
                            expectedCheckSum.toString(),
                            Long.toString(calculatedCheckSum)));
                }

                path = readString(TarHeaderField.name);

                if (path == null) {
                    throw new MissingField(TarHeaderField.name);
                }

                Long longObject = readInteger(TarHeaderField.mode);

                if (longObject == null) {
                    throw new MissingField(TarHeaderField.mode);
                }

                fileMode   = (int) longObject.longValue();
                longObject = readInteger(TarHeaderField.size);

                if (longObject != null) {
                    dataSize = longObject.longValue();
                }

                longObject = readInteger(TarHeaderField.mtime);

                if (longObject == null) {
                    throw new MissingField(TarHeaderField.mtime);
                }

                modTime = longObject.longValue();
            } catch (MissingField mf) {
                throw new TarMalformatException(mf.getMessage());
            }

            entryType = readChar(TarHeaderField.typeflag);
            ownerName = readString(TarHeaderField.uname);

            String pathPrefix = readString(TarHeaderField.prefix);

            if (pathPrefix != null) {
                path = pathPrefix + '/' + path;
            }

            // We're not loading the "gname" field, since there is nothing at
            // all that Java can do with it.
            ustar = isUstar();
        }

        protected byte[] rawHeader;
        /* CRITICALLY IMPORTANT:  TO NOT USE rawHeader.length OR DEPEND ON
         * THE LENGTH OF the rawHeader ARRAY!  Use only the first 512 bytes!
         */
        protected String  path;
        protected int     fileMode;
        protected long    dataSize = -1;    // In bytes
        protected long    modTime;
        protected char    entryType;
        protected String  ownerName;
        protected boolean ustar;

        /**
         * @return a new Absolutized File object generated from this
         * TarEntryHeader.
         */
        public File generateFile() {

            if (entryType != '\0' && entryType != '0') {
                throw new IllegalStateException(
                        RB.create_only_normal.getString());
            }

            // Unfortunately, it does no good to set modification times or
            // privileges here, since those settings have no effect on our
            // new file until after is created by the FileOutputStream
            // constructor.
            return new File(path);
        }

        public char getEntryType() {
            return entryType;
        }

        public String getPath() {
            return path;
        }

        /**
         * Setter is needed in order to override header size setting for Pax.
         */
        public void setDataSize(long dataSize) {
            this.dataSize = dataSize;
        }

        public long getDataSize() {
            return dataSize;
        }

        public long getModTime() {
            return modTime;
        }

        public int getFileMode() {
            return fileMode;
        }

        /**
         * Choosing not to report fields that we don't write (e.g. "gname"),
         * but which would certainly be useful for a general Java tar client
         * implementation.
         * This design decision is subject to change.
         */
        public String toString() {

            StringBuffer sb = new StringBuffer(
                sdf.format(Long.valueOf(modTime * 1000L)) + ' ');

            sb.append((entryType == '\0') ? ' '
                                          : entryType);
            sb.append(ustar ? '*'
                            : ' ');
            sb.append(
                " "
                + StringUtil.toPaddedString(
                    Integer.toOctalString(fileMode), 4, ' ', false) + ' '
                        + StringUtil.toPaddedString(
                            Long.toString(dataSize), 11, ' ', false) + "  ");
            sb.append(StringUtil.toPaddedString(((ownerName == null) ? "-"
                                                                     : ownerName), 8,
                                                                     ' ',
                                                                     true));
            sb.append("  " + path);

            return sb.toString();
        }

        /**
         * Is this any UStar variant
         */
        public boolean isUstar() throws TarMalformatException {

            String magicString = readString(TarHeaderField.magic);

            return magicString != null && magicString.startsWith("ustar");
        }

        /**
         * @return index based at 0 == from
         */
        public static int indexOf(byte[] ba, byte val, int from, int to) {

            for (int i = from; i < to; i++) {
                if (ba[i] == val) {
                    return i - from;
                }
            }

            return -1;
        }

        protected char readChar(TarHeaderField field) throws TarMalformatException {

            /* Depends on readString(int) contract that it will never return
             * a 0-length String */
            String s = readString(field);

            return (s == null) ? '\0'
                               : s.charAt(0);
        }

        /**
         * @return null or String with length() > 0.
         */
        protected String readString(TarHeaderField field) throws TarMalformatException {

            int start = field.getStart();
            int stop  = field.getStop();
            int termIndex = TarEntryHeader.indexOf(rawHeader, (byte) 0, start,
                                                   stop);

            switch (termIndex) {

                case 0 :
                    return null;

                case -1 :
                    termIndex = stop - start;
                    break;

                default:
                    break;
            }

            try {
                return new String(rawHeader, start, termIndex);
            } catch (Throwable t) {

                // Java API does not specify behavior if decoding fails.
                throw new TarMalformatException(
                        RB.bad_header_value.getString(field.toString()));
            }
        }

        /**
         * Integer as in positive whole number, which does not imply Java
         * types of <CODE>int</CODE> or <CODE>Integer</CODE>.
         */
        protected Long readInteger(TarHeaderField field)
                throws TarMalformatException {

            String s = readString(field);

            if (s == null) {
                return null;
            }

            try {
                return Long.valueOf(s, 8);
            } catch (NumberFormatException nfe) {
                throw new TarMalformatException(
                        RB.bad_numeric_header_value.getString(
                        field.toString(), nfe.toString()));
            }
        }

        protected long headerChecksum() {

            long sum = 0;

            for (int i = 0; i < 512; i++) {
                boolean isInRange =
                    (i >= TarHeaderField.checksum.getStart()
                     && i < TarHeaderField.checksum.getStop());

                // We ignore current contents of the checksum field so that
                // this method will continue to work right, even if we later
                // recycle the header or RE-calculate a header.
                sum += isInRange ? 32
                                 : (255 & rawHeader[i]);
            }

            return sum;
        }
    }
}
